import abc
import asyncio
import pickle
import logging # Added for logging consistency
from typing import Any, Optional, Dict, List, Tuple, Union, Type, Coroutine

# Assuming runnables.py is in the same directory or accessible via PYTHONPATH
from .runnables import (
    Runnable, 
    ExecutionContext,
    InMemoryExecutionContext,
    NO_INPUT,
    Pipeline as SyncPipeline, # For operator fallback if needed
    BranchAndFanIn as SyncBranchAndFanIn,
    Conditional as SyncConditional,
    While as SyncWhile,
    _PendingConditional as _SyncPendingConditional,
    # 新增导入
    Router,
    _GraphExportHelper
)
from pydantic import BaseModel, create_model

logger = logging.getLogger(__name__)

class AsyncRunnable(Runnable):
    """Base class for asynchronous Runnables."""
    
    # __init__ is inherited from Runnable

    async def invoke_async(self, input_data: Any = NO_INPUT,
                           context: Optional[ExecutionContext] = None) -> Any:
        """Asynchronous invoke method. Handles input declaration, caching (respecting use_cache), retries, and error handling."""
        effective_context = context if context is not None else InMemoryExecutionContext(
            initial_input=input_data if input_data is not NO_INPUT else None)

        prepared_input_for_invoke = self._prepare_input_payload(input_data, effective_context)
        self._emit_node_status(effective_context, "start")
        cache_key = None # Initialize
        if self.use_cache:
            cache_key = self._get_cache_key(prepared_input_for_invoke, effective_context)
            if cache_key in self._invoke_cache:
                effective_context.log_event(f"Node '{self.name}': Async invoke result from cache.")
                result = self._invoke_cache[cache_key]
                if self.name and effective_context: # Ensure effective_context is not None (it won't be here)
                    effective_context.add_output(self.name, result)
                self._emit_node_status(effective_context, "success")
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
                f"Input type: {type(prepared_input_for_invoke).__name__}")

            try:
                execution_input_payload = self._apply_input_model(prepared_input_for_invoke)
                result_raw = await self._internal_invoke_async(execution_input_payload, effective_context)
                if isinstance(result_raw, Runnable):
                    result = result_raw
                else:
                    result = self._apply_output_model(result_raw)
                if self.use_cache and cache_key is not None: 
                    self._invoke_cache[cache_key] = result
                effective_context.log_event(
                    f"Node '{self.name}': Async invoked successfully. "
                    f"Output type: {type(result).__name__}.")
                
                if self.name:
                    effective_context.add_output(self.name, result)
                self._emit_node_status(effective_context, "success")
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
                                    prepared_input_for_invoke, effective_context)
                                effective_context.log_event(
                                    f"Node '{self.name}': Async error handler "
                                    f"'{getattr(self._error_handler, 'name', 'UnnamedErrorHandler')}' executed.")
                                # 错误处理器的输出可能使用不同的模型，尝试应用输出模型，如果失败则直接使用
                                try:
                                    adjusted_output = self._apply_output_model(error_handler_output)
                                except (ValueError, ValidationError):
                                    # 如果错误处理器的输出模型不匹配，直接使用原始输出
                                    effective_context.log_event(f"Node '{self.name}': Error handler output model mismatch, using raw output.")
                                    adjusted_output = error_handler_output
                                if self.name:
                                    effective_context.add_output(self.name, adjusted_output)
                                if self.use_cache and cache_key is not None: # <<< MODIFIED: Check use_cache
                                    self._invoke_cache[cache_key] = adjusted_output
                                self._emit_node_status(effective_context, "success")
                                return adjusted_output
                            else:
                                effective_context.log_event(f"Node '{self.name}': Error handler is not a Runnable, cannot invoke_async. Raising original error.")
                                raise e # Re-raise original error if handler is not Runnable
                        except Exception as eh_e:
                            effective_context.log_event(
                                f"Node '{self.name}': Async error handler "
                                f"'{getattr(self._error_handler, 'name', 'UnnamedErrorHandler')}' also failed: "
                                f"{type(eh_e).__name__}: {eh_e}")
                            self._emit_node_status(effective_context, "failed", {"exception": type(e).__name__})
                            raise e from eh_e # Re-raise original error, with error handler's error as context
                    self._emit_node_status(effective_context, "failed", {"exception": type(e).__name__})
                    raise e # Re-raise original error if no error_handler or if it wasn't a Runnable
                
                effective_context.log_event(
                    f"Node '{self.name}': Async retrying in {delay_seconds} seconds.")
                if delay_seconds > 0: # Ensure no sleep if delay is zero
                    await asyncio.sleep(delay_seconds)
        
        # This part is reached if all retry attempts fail and the last exception was not handled or re-raised
        if last_exception is not None:
            self._emit_node_status(effective_context, "failed", {"exception": type(last_exception).__name__})
            raise last_exception
        else:
            self._emit_node_status(effective_context, "failed", {"exception": "Unknown"})
            raise RuntimeError(f"AsyncRunnable '{self.name}': Exited retry loop unexpectedly without result or exception.")

    async def check_async(self, data_from_invoke: Any,
                          context: Optional[ExecutionContext] = None) -> bool:
        """Asynchronous version of check method, respecting use_cache."""
        if not self.use_cache: 
            if context: context.log_event(f"Node '{self.name}': Async check caching disabled (use_cache=False).")
            if self._custom_async_check_fn:
                return await self._custom_async_check_fn(data_from_invoke)
            # If there's a sync custom_check_fn, use it via run_in_executor
            if self._custom_check_fn:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, self.check, data_from_invoke, context)
            # If no custom check function, use default async check logic
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
        elif self._custom_check_fn:
            # Use sync check function via run_in_executor
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self.check, data_from_invoke, context)
        else: # No custom check function, use default async
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
        if not isinstance(first, Runnable) or not isinstance(second, Runnable):
            raise TypeError("Both 'first' and 'second' must be Runnable instances.")
        self.first = first
        self.second = second
        self.InputModel = first._input_model_cls
        self.OutputModel = second._output_model_cls
        super().__init__(name or f"AsyncPipeline[{first.name}_then_{second.name}]", **kwargs)

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Any:
        context.log_event(f"Node '{self.name}': First task '{self.first.name}' starting.")
        output_first = await self.first.invoke_async(input_data, context)
        context.log_event(f"Node '{self.name}': First task '{self.first.name}' completed. Second task '{self.second.name}' starting.")
        # Pipeline 中，second 应该接收 first 的输出
        # 如果 second 有 map_inputs 配置，会通过 context 自动处理
        # 否则，直接传递 output_first，让 second 的 _prepare_input_payload 处理
        output_second = await self.second.invoke_async(output_first, context)
        context.log_event(f"Node '{self.name}': Second task '{self.second.name}' completed.")
        return output_second

    async def _default_check_async(self, data_from_invoke: Any) -> bool:
        # Delegate check to the second runnable in the pipeline
        return await self.second.check_async(data_from_invoke)

    def _expand_to_graph(self, graph: 'WorkflowGraph', helper: '_GraphExportHelper') -> Tuple[List[str], List[str]]:
        entry_first, exit_first = self.first._expand_to_graph(graph, helper)
        entry_second, exit_second = self.second._expand_to_graph(graph, helper)
        for parent in exit_first:
            for child in entry_second:
                helper.connect_by_name(graph, parent, child)
        return entry_first, exit_second

class _AsyncCheckAdapterRunnable(AsyncRunnable):
    """
    Auto-generated adapter task for AsyncRunnable that executes check_async()
    and returns a result compatible with a Router ({"condition": bool}).
    Used during graph expansion for AsyncConditional nodes.
    """
    def __init__(self, target_runnable: Runnable, name: Optional[str] = None):
        self.target = target_runnable
        adapter_name = name or f"{target_runnable.name}_CheckAdapter"
        # Inherit InputModel from target's OutputModel for visual continuity
        # Must set as class attribute before calling super().__init__()
        self.__class__.InputModel = target_runnable._output_model_cls
        # Explicit OutputModel for Router compatibility
        self.__class__.OutputModel = create_model(f"{adapter_name}Output", condition=(bool, ...))
        super().__init__(name=adapter_name)

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Dict[str, bool]:
        # Extract the actual value from the model if it's a BaseModel
        from pydantic import BaseModel
        if isinstance(input_data, BaseModel):
            # If the model has a single field, extract it; otherwise pass the model
            model_cls = type(input_data)
            fields = list(model_cls.model_fields.keys())
            if len(fields) == 1:
                check_value = getattr(input_data, fields[0])
            else:
                check_value = input_data
        else:
            check_value = input_data
        # Call the async check method
        result = await self.target.check_async(check_value, context)
        return {"condition": result}


class AsyncConditional(AsyncRunnable):
    def __init__(self, condition_r: Runnable, true_r: Runnable, false_r: Runnable, name: Optional[str] = None, **kwargs):
        self.condition_r = condition_r
        self.true_r = true_r
        self.false_r = false_r
        true_fields = set(getattr(true_r._output_model_cls, "model_fields", {}).keys())
        false_fields = set(getattr(false_r._output_model_cls, "model_fields", {}).keys())
        if true_fields != false_fields:
            raise ValueError("AsyncConditional 的 true/false 分支必须拥有兼容的 OutputModel。")
        self.InputModel = condition_r._input_model_cls
        self.OutputModel = true_r._output_model_cls
        super().__init__(name or f"AsyncCond[{condition_r.name}?{true_r.name}:{false_r.name}]", **kwargs)

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Any:
        context.log_event(f"Node '{self.name}': Evaluating condition '{self.condition_r.name}'.")
        condition_output = await self.condition_r.invoke_async(input_data, context)
        
        context.log_event(f"Node '{self.name}': Checking condition output.")
        # AsyncConditional 中，分支任务应该接收 condition_r 的输出
        # 直接传递 condition_output，让分支任务的 _prepare_input_payload 处理
        if await self.condition_r.check_async(condition_output, context):
            context.log_event(f"Node '{self.name}': Condition TRUE, executing true_branch '{self.true_r.name}'.")
            branch_output = await self.true_r.invoke_async(condition_output, context)
        else:
            context.log_event(f"Node '{self.name}': Condition FALSE, executing false_branch '{self.false_r.name}'.")
            branch_output = await self.false_r.invoke_async(condition_output, context)
        
        # AsyncConditional 的输出应该直接返回分支的输出，不进行额外的模型验证
        # 因为我们已经验证了两个分支的 OutputModel 字段兼容
        return branch_output

    def _expand_to_graph(self, graph: 'WorkflowGraph', helper: '_GraphExportHelper') -> Tuple[List[str], List[str]]:
        # 1. Expand Condition node (A)
        entry_cond, exit_cond = self.condition_r._expand_to_graph(graph, helper)

        # 2. Create AsyncCheckAdapter node
        # Uses _AsyncCheckAdapterRunnable to support async checks (e.g. DB queries)
        check_adapter = _AsyncCheckAdapterRunnable(self.condition_r)
        adapter_node_name = helper.add_node(graph, check_adapter)

        # Connect A -> Adapter
        for parent in exit_cond:
            helper.connect_by_name(graph, parent, adapter_node_name)

        # 3. Create Router node
        router = Router(name=f"{self.condition_r.name}_Router")
        router_node_name = helper.add_node(graph, router)

        # Connect Adapter -> Router
        helper.connect_by_name(graph, adapter_node_name, router_node_name)

        # 4. Expand True/False branches
        entry_true, exit_true = self.true_r._expand_to_graph(graph, helper)
        entry_false, exit_false = self.false_r._expand_to_graph(graph, helper)

        # 5. Connect Router -> Branches (Control Flow)
        for child in entry_true:
            helper.connect_by_name(graph, router_node_name, child, branch="true")
        for child in entry_false:
            helper.connect_by_name(graph, router_node_name, child, branch="false")

        # 6. Connect A -> Branches (Data Bypass)
        for parent in exit_cond:
            for child in entry_true:
                helper.connect_by_name(graph, parent, child)
            for child in entry_false:
                helper.connect_by_name(graph, parent, child)

        return entry_cond, exit_true + exit_false

class AsyncWhile(AsyncRunnable):
    def __init__(self, condition_check_runnable: Runnable, body_runnable: Runnable, max_loops: int = 100, name: Optional[str] = None, **kwargs):
        self.condition_check_runnable = condition_check_runnable
        self.body_runnable = body_runnable
        self.max_loops = max_loops
        loop_name = name or f"AsyncWhile[{condition_check_runnable.name}_do_{body_runnable.name}]"
        body_output_cls = body_runnable._output_model_cls
        self.InputModel = condition_check_runnable._input_model_cls
        self.OutputModel = create_model(f"{loop_name}Output", history=(List[body_output_cls], ...))
        super().__init__(loop_name, **kwargs)

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> List[Any]:
        loop_count = 0
        current_input_for_iteration = input_data
        all_body_outputs = []

        context.log_event(f"Node '{self.name}': Starting async loop (max_loops={self.max_loops}).")
        while loop_count < self.max_loops:
            context.log_event(f"Node '{self.name}': Loop {loop_count + 1}. Evaluating condition '{self.condition_check_runnable.name}'.")
            condition_input = self._unwrap_input_payload(current_input_for_iteration)
            condition_output = await self.condition_check_runnable.invoke_async(condition_input, context)
            
            if not await self.condition_check_runnable.check_async(condition_output, context):
                context.log_event(f"Node '{self.name}': Condition FALSE. Exiting loop after {loop_count} iterations.")
                break
            
            context.log_event(f"Node '{self.name}': Condition TRUE. Executing body '{self.body_runnable.name}'.")
            
            # 提取 current_input_for_iteration 的实际值
            from pydantic import BaseModel
            if isinstance(current_input_for_iteration, BaseModel):
                model_cls = type(current_input_for_iteration)
                fields = list(model_cls.model_fields.keys())
                if len(fields) == 1:
                    last_value = getattr(current_input_for_iteration, fields[0])
                else:
                    last_value = self._unwrap_input_payload(current_input_for_iteration)
            else:
                last_value = self._unwrap_input_payload(current_input_for_iteration)
            
            history_snapshot = [self._unwrap_input_payload(item) for item in all_body_outputs]
            loop_input_payload = {
                "last": last_value,
                "history": history_snapshot,
                "iteration": loop_count,
            }
            body_output = await self.body_runnable.invoke_async(loop_input_payload, context)
            all_body_outputs.append(body_output)
            
            current_input_for_iteration = body_output
            loop_count += 1
        else:
            context.log_event(f"Node '{self.name}': Loop exited due to max_loops ({self.max_loops}) reached.")
        
        context.log_event(f"Node '{self.name}': Loop finished. Returning {len(all_body_outputs)} outputs.")
        return {"history": all_body_outputs}


class AgentLoop(AsyncRunnable):
    """
    动态执行循环，允许生成器 Runnable 在运行过程中返回下一个 Runnable。
    当生成器返回普通结果（非 Runnable）时循环结束。
    """
    def __init__(self, generator: Runnable, max_iterations: int = 25, name: Optional[str] = None, **kwargs):
        self.generator = generator
        self.max_iterations = max_iterations
        loop_name = name or f"AgentLoop[{generator.name}]"
        self.InputModel = generator._input_model_cls
        self.OutputModel = create_model(f"{loop_name}Output", result=(Any, ...))
        super().__init__(loop_name, **kwargs)

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Any:
        current_payload = input_data
        iteration = 0

        while iteration < self.max_iterations:
            iteration += 1
            context.log_event(f"Node '{self.name}': Iteration {iteration}, invoking generator '{self.generator.name}'.")
            generated = await self.generator.invoke_async(current_payload, context)

            if isinstance(generated, Runnable):
                context.log_event(
                    f"Node '{self.name}': Generator produced runnable '{generated.name}'. Executing it next."
                )
                current_payload = await generated.invoke_async(current_payload, context)
                continue

            context.log_event(f"Node '{self.name}': Generator returned terminal result. Loop completed.")
            return {"result": generated}

        raise RuntimeError(
            f"AgentLoop '{self.name}' exceeded max_iterations ({self.max_iterations}) without producing a terminal result."
        )


class AsyncBranchAndFanIn(AsyncRunnable):
    """Asynchronous version of BranchAndFanIn. Accepts mixed Runnable types."""
    
    def __init__(self, tasks_dict: Dict[str, Runnable], # Changed to Runnable
                 name: Optional[str] = None, **kwargs):
        if not isinstance(tasks_dict, dict) or not all(isinstance(r, Runnable) for r in tasks_dict.values()):
            raise TypeError("tasks_dict must be a dictionary of Runnables.")
        self.tasks_dict = tasks_dict
        base_name = name or f"AsyncBranchFanIn_{'_'.join(tasks_dict.keys())}"
        self.InputModel = self._ensure_uniform_input_model()
        self.OutputModel = self._build_output_model(base_name)
        # max_workers is implicit with asyncio.gather
        super().__init__(base_name, **kwargs)

    def _ensure_uniform_input_model(self) -> Type[BaseModel]:
        models = {task._input_model_cls for task in self.tasks_dict.values()}
        if len(models) != 1:
            raise ValueError("AsyncBranchAndFanIn 所有子任务必须共享相同的 InputModel。")
        return next(iter(models))

    def _build_output_model(self, base_name: str) -> Type[BaseModel]:
        fields = {
            key: (task._output_model_cls, ...)
            for key, task in self.tasks_dict.items()
        }
        return create_model(f"{base_name}Output", **fields)

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
        if not isinstance(tasks_dict, dict) or not all(isinstance(r, Runnable) for r in tasks_dict.values()):
            raise TypeError("tasks_dict must be a dictionary of Runnables.")
        self.tasks_dict = tasks_dict
        base_name = name or f"AsyncSourcePar_{'_'.join(tasks_dict.keys())}"
        self.InputModel = self._ensure_uniform_input_model()
        self.OutputModel = self._build_output_model(base_name)
        super().__init__(base_name, **kwargs)

    def _ensure_uniform_input_model(self) -> Type[BaseModel]:
        models = {task._input_model_cls for task in self.tasks_dict.values()}
        if len(models) != 1:
            raise ValueError("AsyncSourceParallel 所有子任务必须共享相同的 InputModel。")
        return next(iter(models))

    def _build_output_model(self, base_name: str) -> Type[BaseModel]:
        fields = {
            key: (task._output_model_cls, ...)
            for key, task in self.tasks_dict.items()
        }
        return create_model(f"{base_name}Output", **fields)

    async def _internal_invoke_async(self, input_data: Any, 
                                   context: ExecutionContext) -> Dict[str, Any]:
        context.log_event(
            f"Node '{self.name}': Starting async parallel source execution for "
            f"{len(self.tasks_dict)} tasks. Input type: {type(input_data).__name__}")
        
        coroutines_map = {
            key: task.invoke_async(input_data, context)
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