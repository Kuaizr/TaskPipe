import abc
import asyncio
import pickle
import logging # Added for logging consistency
from typing import Any, Optional, Dict, List, Tuple, Union, Type, Coroutine

# Assuming runnables.py is in the same directory or accessible via PYTHONPATH
from .runnables import (
    Runnable, 
    ExecutionContext,
    NO_INPUT,
    _default_cache_key_generator,
    Pipeline as SyncPipeline, # For operator fallback if needed
    BranchAndFanIn as SyncBranchAndFanIn,
    Conditional as SyncConditional,
    While as SyncWhile,
    _PendingConditional as _SyncPendingConditional
)

logger = logging.getLogger(__name__)

class AsyncRunnable(Runnable):
    """Base class for asynchronous Runnables."""
    
    # __init__ is inherited from Runnable

    async def invoke_async(self, input_data: Any = NO_INPUT,
                           context: Optional[ExecutionContext] = None) -> Any:
        """Asynchronous invoke method. Handles input declaration, caching (respecting use_cache), retries, and error handling."""
        effective_context = context if context is not None else ExecutionContext(
            initial_input=input_data if input_data is not NO_INPUT else None)

        actual_input_for_invoke = input_data
        if self.input_declaration and input_data is NO_INPUT and effective_context:
            if isinstance(self.input_declaration, str):
                actual_input_for_invoke = effective_context.get_output(self.input_declaration)
            elif isinstance(self.input_declaration, dict):
                kwargs_from_context = {}
                for param_name, source_key in self.input_declaration.items():
                    if isinstance(source_key, str):
                        kwargs_from_context[param_name] = effective_context.get_output(source_key)
                    else:
                        kwargs_from_context[param_name] = source_key
                actual_input_for_invoke = kwargs_from_context
            elif callable(self.input_declaration):
                if asyncio.iscoroutinefunction(self.input_declaration):
                    actual_input_for_invoke = await self.input_declaration(effective_context)
                else:
                    actual_input_for_invoke = self.input_declaration(effective_context)

        cache_key = None # Initialize
        if self.use_cache:
            cache_key = self._get_cache_key(actual_input_for_invoke, effective_context)
            if cache_key in self._invoke_cache:
                effective_context.log_event(f"Node '{self.name}': Async invoke result from cache.")
                result = self._invoke_cache[cache_key]
                if self.name and effective_context: # Ensure effective_context is not None (it won't be here)
                    effective_context.add_output(self.name, result)
                return result
        
        if not self.use_cache: # Log if caching is disabled
            effective_context.log_event(f"Node '{self.name}': Async caching disabled (use_cache=False).")


        current_attempt = 0
        max_attempts = (self._retry_config or {}).get("max_attempts", 1)
        delay_seconds = (self._retry_config or {}).get("delay_seconds", 0)
        retry_on_exceptions = (self._retry_config or {}).get("retry_on_exceptions", (Exception,))
        
        last_exception: Optional[Exception] = None # Store the last exception for re-raising

        while current_attempt < max_attempts:
            current_attempt += 1
            effective_context.log_event(
                f"Node '{self.name}': Async invoking (Attempt {current_attempt}/{max_attempts}). "
                f"Input type: {type(actual_input_for_invoke).__name__}")

            try:
                result = await self._internal_invoke_async(actual_input_for_invoke, effective_context)
                if self.use_cache and cache_key is not None: 
                    self._invoke_cache[cache_key] = result
                effective_context.log_event(
                    f"Node '{self.name}': Async invoked successfully. "
                    f"Output type: {type(result).__name__}.")
                
                if self.name:
                    effective_context.add_output(self.name, result)
                return result

            except Exception as e:
                last_exception = e # Store the current exception
                effective_context.log_event(
                    f"Node '{self.name}': Error during async invoke "
                    f"(Attempt {current_attempt}/{max_attempts}): "
                    f"{type(e).__name__}: {e}")

                is_retryable = isinstance(e, retry_on_exceptions)
                is_last_attempt = (current_attempt == max_attempts)

                if not is_retryable or is_last_attempt:
                    if self._error_handler: # Assumes _error_handler is Runnable for invoke_async
                        effective_context.log_event(
                            f"Node '{self.name}': Invoking async error handler "
                            f"'{getattr(self._error_handler, 'name', 'UnnamedErrorHandler')}'.") # Use getattr for name
                        try:
                            if isinstance(self._error_handler, Runnable): # Check if it's a Runnable
                                error_handler_output = await self._error_handler.invoke_async(
                                    actual_input_for_invoke, effective_context)
                                effective_context.log_event(
                                    f"Node '{self.name}': Async error handler "
                                    f"'{getattr(self._error_handler, 'name', 'UnnamedErrorHandler')}' executed.")
                                if self.name:
                                    effective_context.add_output(self.name, error_handler_output)
                                if self.use_cache and cache_key is not None: # <<< MODIFIED: Check use_cache
                                    self._invoke_cache[cache_key] = error_handler_output
                                return error_handler_output
                            else:
                                effective_context.log_event(f"Node '{self.name}': Error handler is not a Runnable, cannot invoke_async. Raising original error.")
                                raise e # Re-raise original error if handler is not Runnable
                        except Exception as eh_e:
                            effective_context.log_event(
                                f"Node '{self.name}': Async error handler "
                                f"'{getattr(self._error_handler, 'name', 'UnnamedErrorHandler')}' also failed: "
                                f"{type(eh_e).__name__}: {eh_e}")
                            raise e from eh_e # Re-raise original error, with error handler's error as context
                    raise e # Re-raise original error if no error_handler or if it wasn't a Runnable
                
                effective_context.log_event(
                    f"Node '{self.name}': Async retrying in {delay_seconds} seconds.")
                if delay_seconds > 0: # Ensure no sleep if delay is zero
                    await asyncio.sleep(delay_seconds)
        
        # This part is reached if all retry attempts fail and the last exception was not handled or re-raised
        if last_exception is not None:
            raise last_exception
        else:
            raise RuntimeError(f"AsyncRunnable '{self.name}': Exited retry loop unexpectedly without result or exception.")


    async def check_async(self, data_from_invoke: Any,
                          context: Optional[ExecutionContext] = None) -> bool:
        """Asynchronous version of check method, respecting use_cache."""
        if not self.use_cache: 
            if context: context.log_event(f"Node '{self.name}': Async check caching disabled (use_cache=False).")
            if self._custom_async_check_fn:
                return await self._custom_async_check_fn(data_from_invoke)
            # If no custom_async_check_fn, use default async check logic
            return await self._default_check_async(data_from_invoke)

        cache_key_data = data_from_invoke
        try:
            cache_key = pickle.dumps(cache_key_data)
        except Exception:
            cache_key = object()

        if cache_key in self._check_cache:
            if context:
                context.log_event(f"Node '{self.name}': Async check result from cache.")
            return self._check_cache[cache_key]

        result: bool
        if self._custom_async_check_fn:
            result = await self._custom_async_check_fn(data_from_invoke)
        else: # No custom_async_check_fn, use default async
            result = await self._default_check_async(data_from_invoke)

        # self._check_cache is written to only if self.use_cache was True (due to initial check)
        self._check_cache[cache_key] = result
        if context:
            context.log_event(f"Node '{self.name}': Async check result: {result}.")
        return result

    async def _default_check_async(self, data_from_invoke: Any) -> bool:
        """Default asynchronous check logic. Simply checks truthiness."""
        return bool(data_from_invoke)

    @abc.abstractmethod
    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Any:
        """Subclasses must implement this for their core asynchronous logic."""
        pass

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        logger.info(f"AsyncRunnable '{self.name}': _internal_invoke (sync) called, running _internal_invoke_async via asyncio.run().")
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                # This is a problematic scenario: calling asyncio.run() from an already running loop.
                logger.error(
                    f"FATAL: AsyncRunnable '{self.name}'._internal_invoke was called synchronously "
                    f"from within an active asyncio event loop. This will lead to errors. "
                    f"Use 'await {self.name}.invoke_async()' instead."
                )
                # Raising an error is better than letting asyncio.run() fail with a less clear message.
                raise RuntimeError(
                    f"AsyncRunnable '{self.name}'._internal_invoke cannot be called synchronously "
                    f"from an active event loop. Use 'await {self.name}.invoke_async()'."
                )
        except RuntimeError:  # No running event loop, which is the expected case for this sync wrapper
            pass
        return asyncio.run(self._internal_invoke_async(input_data, context))


    # --- Operator Overloading for Async Composers ---
    def __or__(self, other: Union[Runnable, Dict[str, Runnable]]) -> 'AsyncPipeline':
        # If self is AsyncRunnable, the resulting pipeline should always be AsyncPipeline
        # to correctly handle the async nature of self.
        if isinstance(other, Runnable) and not isinstance(other, dict):
            return AsyncPipeline(self, other)
        elif isinstance(other, dict) and all(isinstance(r, Runnable) for r in other.values()):
            branch_component: Runnable
            if any(isinstance(r, AsyncRunnable) for r in other.values()):
                branch_component = AsyncBranchAndFanIn(other)
            else:  # All runnables in dict are SyncRunnable
                branch_component = SyncBranchAndFanIn(other)
            return AsyncPipeline(self, branch_component)
        return NotImplemented

    def __ror__(self, other: Runnable) -> 'AsyncPipeline':
        if isinstance(other, Runnable) and not isinstance(other, dict):
            return AsyncPipeline(other, self)
        return NotImplemented

    def __mod__(self, true_branch: Runnable) -> '_AsyncPendingConditional': # Always return _AsyncPendingConditional
        if not isinstance(true_branch, Runnable):
            return NotImplemented
        # If self is AsyncRunnable, the conditional structure should be async-aware.
        return _AsyncPendingConditional(self, true_branch)


class _AsyncPendingConditional:
    def __init__(self, condition_r: Runnable, true_r: Runnable):
        self.condition_r = condition_r
        self.true_r = true_r
        # Name generation can be done in AsyncConditional
        # self.name_hint = f"({condition_r.name} % {true_r.name})" 

    def __rshift__(self, false_r: Runnable) -> 'AsyncConditional':
        if not isinstance(false_r, Runnable):
            return NotImplemented
        # name = f"({self.name_hint} >> {false_r.name})"
        return AsyncConditional(self.condition_r, self.true_r, false_r)

# --- Asynchronous Composer Classes ---

class AsyncPipeline(AsyncRunnable):
    def __init__(self, first: Runnable, second: Runnable, name: Optional[str] = None, **kwargs):
        super().__init__(name or f"AsyncPipeline[{first.name}_then_{second.name}]", **kwargs)
        if not isinstance(first, Runnable) or not isinstance(second, Runnable):
            raise TypeError("Both 'first' and 'second' must be Runnable instances.")
        self.first = first
        self.second = second
        if self.input_declaration is None and first.input_declaration is not None:
            self.input_declaration = first.input_declaration

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Any:
        context.log_event(f"Node '{self.name}': First task '{self.first.name}' starting.")
        output_first = await self.first.invoke_async(input_data, context)
        context.log_event(f"Node '{self.name}': First task '{self.first.name}' completed. Second task '{self.second.name}' starting.")
        output_second = await self.second.invoke_async(output_first, context)
        context.log_event(f"Node '{self.name}': Second task '{self.second.name}' completed.")
        return output_second

    async def _default_check_async(self, data_from_invoke: Any) -> bool:
        # Delegate check to the second runnable in the pipeline
        return await self.second.check_async(data_from_invoke)


class AsyncConditional(AsyncRunnable):
    def __init__(self, condition_r: Runnable, true_r: Runnable, false_r: Runnable, name: Optional[str] = None, **kwargs):
        super().__init__(name or f"AsyncCond[{condition_r.name}?{true_r.name}:{false_r.name}]", **kwargs)
        self.condition_r = condition_r
        self.true_r = true_r
        self.false_r = false_r

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Any:
        context.log_event(f"Node '{self.name}': Evaluating condition '{self.condition_r.name}'.")
        condition_output = await self.condition_r.invoke_async(input_data, context)
        
        context.log_event(f"Node '{self.name}': Checking condition output.")
        if await self.condition_r.check_async(condition_output, context):
            context.log_event(f"Node '{self.name}': Condition TRUE, executing true_branch '{self.true_r.name}'.")
            return await self.true_r.invoke_async(condition_output, context)
        else:
            context.log_event(f"Node '{self.name}': Condition FALSE, executing false_branch '{self.false_r.name}'.")
            return await self.false_r.invoke_async(condition_output, context)


class AsyncWhile(AsyncRunnable):
    def __init__(self, condition_check_runnable: Runnable, body_runnable: Runnable, max_loops: int = 100, name: Optional[str] = None, **kwargs):
        super().__init__(name or f"AsyncWhile[{condition_check_runnable.name}_do_{body_runnable.name}]", **kwargs)
        self.condition_check_runnable = condition_check_runnable
        self.body_runnable = body_runnable
        self.max_loops = max_loops

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> List[Any]:
        loop_count = 0
        current_input_for_iteration = input_data
        all_body_outputs = []

        context.log_event(f"Node '{self.name}': Starting async loop (max_loops={self.max_loops}).")
        while loop_count < self.max_loops:
            context.log_event(f"Node '{self.name}': Loop {loop_count + 1}. Evaluating condition '{self.condition_check_runnable.name}'.")
            condition_output = await self.condition_check_runnable.invoke_async(current_input_for_iteration, context)
            
            if not await self.condition_check_runnable.check_async(condition_output, context):
                context.log_event(f"Node '{self.name}': Condition FALSE. Exiting loop after {loop_count} iterations.")
                break
            
            context.log_event(f"Node '{self.name}': Condition TRUE. Executing body '{self.body_runnable.name}'.")
            body_output = await self.body_runnable.invoke_async(current_input_for_iteration, context)
            all_body_outputs.append(body_output)
            
            current_input_for_iteration = body_output
            loop_count += 1
        else:
            context.log_event(f"Node '{self.name}': Loop exited due to max_loops ({self.max_loops}) reached.")
        
        context.log_event(f"Node '{self.name}': Loop finished. Returning {len(all_body_outputs)} outputs.")
        return all_body_outputs


class AsyncBranchAndFanIn(AsyncRunnable):
    """Asynchronous version of BranchAndFanIn. Accepts mixed Runnable types."""
    
    def __init__(self, tasks_dict: Dict[str, Runnable], # Changed to Runnable
                 name: Optional[str] = None, **kwargs):
        super().__init__(name or f"AsyncBranchFanIn_{'_'.join(tasks_dict.keys())}", **kwargs)
        if not isinstance(tasks_dict, dict) or not all(isinstance(r, Runnable) for r in tasks_dict.values()):
            raise TypeError("tasks_dict must be a dictionary of Runnables.")
        self.tasks_dict = tasks_dict
        # max_workers is implicit with asyncio.gather

    async def _internal_invoke_async(self, input_data: Any, 
                                   context: ExecutionContext) -> Dict[str, Any]:
        context.log_event(
            f"Node '{self.name}': Starting async parallel execution for "
            f"{len(self.tasks_dict)} tasks.")
        
        # Create coroutines for each task using invoke_async
        coroutines_map = {
            key: task.invoke_async(input_data, context)
            for key, task in self.tasks_dict.items()
        }
        
        # Gather results, allowing exceptions to propagate if not handled by individual tasks
        # return_exceptions=True allows us to inspect errors if needed, rather than failing fast.
        results_or_exceptions = await asyncio.gather(
            *coroutines_map.values(), return_exceptions=True
        )
        
        final_results: Dict[str, Any] = {}
        exceptions_raised: List[Exception] = []

        for key, res_or_exc in zip(coroutines_map.keys(), results_or_exceptions):
            task_name = self.tasks_dict[key].name
            if isinstance(res_or_exc, Exception):
                context.log_event(f"Node '{self.name}', Branch '{key}' ({task_name}): FAILED with {type(res_or_exc).__name__}: {str(res_or_exc)[:100]}.")
                # Store exception or handle/re-raise as per desired strategy
                exceptions_raised.append(res_or_exc) # Collect all exceptions
                final_results[key] = res_or_exc # Store exception in results for this key
            else:
                final_results[key] = res_or_exc
                context.log_event(f"Node '{self.name}', Branch '{key}' ({task_name}): Completed.")
        
        # If any task failed, re-raise the first exception encountered.
        # More sophisticated error aggregation could be implemented here.
        if exceptions_raised:
            raise exceptions_raised[0]
            
        return final_results


class AsyncSourceParallel(AsyncRunnable):
    """Asynchronous version of SourceParallel. Accepts mixed Runnable types."""
    
    def __init__(self, tasks_dict: Dict[str, Runnable], # Changed to Runnable
                 name: Optional[str] = None, **kwargs):
        super().__init__(name or f"AsyncSourcePar_{'_'.join(tasks_dict.keys())}", **kwargs)
        if not isinstance(tasks_dict, dict) or not all(isinstance(r, Runnable) for r in tasks_dict.values()):
            raise TypeError("tasks_dict must be a dictionary of Runnables.")
        self.tasks_dict = tasks_dict

    async def _internal_invoke_async(self, input_data: Any, 
                                   context: ExecutionContext) -> Dict[str, Any]:
        actual_input = input_data if input_data is not NO_INPUT else context.initial_input
        context.log_event(
            f"Node '{self.name}': Starting async parallel source execution for "
            f"{len(self.tasks_dict)} tasks. Input type: {type(actual_input).__name__}")
        
        coroutines_map = {
            key: task.invoke_async(actual_input, context) # Using invoke_async
            for key, task in self.tasks_dict.items()
        }
        
        results_or_exceptions = await asyncio.gather(
            *coroutines_map.values(), return_exceptions=True
        )

        final_results: Dict[str, Any] = {}
        exceptions_raised: List[Exception] = []

        for key, res_or_exc in zip(coroutines_map.keys(), results_or_exceptions):
            task_name = self.tasks_dict[key].name
            if isinstance(res_or_exc, Exception):
                context.log_event(f"Node '{self.name}', Branch '{key}' ({task_name}): FAILED with {type(res_or_exc).__name__}: {str(res_or_exc)[:100]}.")
                exceptions_raised.append(res_or_exc)
                final_results[key] = res_or_exc # Store exception for this key
            else:
                final_results[key] = res_or_exc
                # Storing individual branch outputs in the context
                context.add_output(f"{self.name}_{key}", res_or_exc) 
                context.log_event(f"Node '{self.name}', Branch '{key}' ({task_name}): Completed.")

        if exceptions_raised:
            raise exceptions_raised[0]
            
        return final_results