import abc
import time
import logging
import hashlib
import pickle
import asyncio # Added for invoke_async
import inspect
from typing import Any, Callable, Optional, Dict, List, Tuple, Union, Type, Coroutine

try:
    from typing import Protocol, runtime_checkable
except ImportError:
    from typing_extensions import Protocol, runtime_checkable
from concurrent.futures import ThreadPoolExecutor, as_completed

# 用于标记没有显式输入的情况
NO_INPUT = object()

logger = logging.getLogger(__name__)
# 为了避免重复配置（如果用户在其他地方也配置了basicConfig），这里不再配置basicConfig
# 而是依赖用户在应用层配置logging

@runtime_checkable
class ExecutionContext(Protocol):
    initial_input: Any
    parent_context: Optional['ExecutionContext']
    node_outputs: Dict[str, Any]
    event_log: List[str]

    def add_output(self, node_name: str, value: Any) -> None: ...

    def get_output(self, node_name: str, default: Any = None) -> Any: ...

    def log_event(self, message: str) -> None: ...


class InMemoryExecutionContext:
    """
    默认的内存型 ExecutionContext，实现了 ExecutionContext 协议。
    """
    def __init__(self, initial_input: Any = NO_INPUT, parent_context: Optional[ExecutionContext] = None):
        self.node_outputs: Dict[str, Any] = {}
        self.initial_input: Any = initial_input
        self.event_log: List[str] = []
        self.parent_context: Optional[ExecutionContext] = parent_context

    def add_output(self, node_name: str, value: Any):
        if node_name:
            self.node_outputs[node_name] = value
            self.log_event(f"Output added for node '{node_name}'. Value type: {type(value).__name__}")
        else:
            self.log_event("Output for unnamed node not added to context's node_outputs.")

    def get_output(self, node_name: str, default: Any = None) -> Any:
        if node_name in self.node_outputs:
            return self.node_outputs[node_name]
        if self.parent_context:
            return self.parent_context.get_output(node_name, default)
        return default

    def log_event(self, message: str):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        full_message = f"[{timestamp}] {message}"
        logger.debug(f"ContextEvent: {message}")
        self.event_log.append(full_message)

    def __repr__(self) -> str:
        return f"<InMemoryExecutionContext id={id(self)} initial_input={str(self.initial_input)[:60]}, node_outputs_keys={list(self.node_outputs.keys())}>"

def _default_cache_key_generator(runnable_name: str, input_data: Any, context: Optional[ExecutionContext], input_declaration: Any) -> Any:
    """
    默认的缓存键生成器。
    尝试序列化输入数据和从上下文中根据 input_declaration 获取的相关数据。
    """
    try:
        input_data_bytes = pickle.dumps(input_data)
        declared_inputs_tuple = None
        if context and input_declaration:
            relevant_context_outputs = {}
            if isinstance(input_declaration, str):
                relevant_context_outputs[input_declaration] = context.get_output(input_declaration)
            elif isinstance(input_declaration, dict):
                for key, source_name in input_declaration.items():
                    if isinstance(source_name, str):
                        relevant_context_outputs[key] = context.get_output(source_name)
            elif callable(input_declaration):
                 relevant_context_outputs["_declaration_callable"] = input_declaration
            
            hashable_context_items = []
            for k, v in sorted(relevant_context_outputs.items()):
                 try:
                      hashable_context_items.append((k, pickle.dumps(v)))
                 except Exception:
                      hashable_context_items.append((k, str(v) + type(v).__name__))
            declared_inputs_tuple = tuple(hashable_context_items)
        declared_inputs_bytes = pickle.dumps(declared_inputs_tuple)

        hasher = hashlib.md5()
        hasher.update(input_data_bytes)
        hasher.update(declared_inputs_bytes)
        hasher.update(runnable_name.encode('utf-8'))
        return hasher.hexdigest()
    except Exception as e:
        return object()


class Runnable(abc.ABC):
    """
    系统的基本构建块。每个 Runnable 代表工作流中的一个操作或步骤。
    """
    def __init__(self,
                 name: Optional[str] = None,
                 input_declaration: Any = None,
                 cache_key_generator: Optional[Callable[[str, Any, Optional[ExecutionContext], Any], Any]] = None,
                 use_cache: bool = False): # <<< NEW PARAMETER, defaulting to False
        self.name: str = name if name else f"{self.__class__.__name__}_{id(self)}"
        if not isinstance(self.name, str) or not self.name:
            self.name = f"{self.__class__.__name__}_{id(self)}"

        self.use_cache: bool = use_cache # <<< NEW LINE: Store the use_cache flag

        self._invoke_cache: Dict[Any, Any] = {}
        self._check_cache: Dict[Any, bool] = {}
        self._custom_check_fn: Optional[Callable[[Any], bool]] = None
        self._custom_async_check_fn: Optional[Callable[[Any], Coroutine[Any, Any, bool]]] = None

        self._error_handler: Optional[Union['Runnable', Callable[[ExecutionContext, Any, Exception], Any]]] = None
        self._retry_config: Optional[Dict[str, Any]] = None
        self.input_declaration: Any = input_declaration
        self._cache_key_generator = cache_key_generator or _default_cache_key_generator

        self._on_start_handler: Optional[Union['Runnable', Callable[[ExecutionContext, Any], None]]] = None
        self._on_complete_handler: Optional[Union['Runnable', Callable[[ExecutionContext, Any, Optional[Exception]], None]]] = None

    def _context_inputs_from_declaration(self, context: Optional[ExecutionContext]) -> Dict[str, Any]:
        if not self.input_declaration or context is None:
            return {}

        if isinstance(self.input_declaration, str):
            return {"_input": context.get_output(self.input_declaration)}

        if isinstance(self.input_declaration, dict):
            resolved: Dict[str, Any] = {}
            for param_name, source_key in self.input_declaration.items():
                if isinstance(source_key, str):
                    resolved[param_name] = context.get_output(source_key)
                else:
                    resolved[param_name] = source_key
            return resolved

        if callable(self.input_declaration):
            value = self.input_declaration(context)
            return self._normalize_callable_input_value(value)

        raise TypeError("input_declaration must be a str, dict, or callable.")

    def _normalize_callable_input_value(self, value: Any) -> Dict[str, Any]:
        if value is None or value is NO_INPUT:
            return {}
        if isinstance(value, dict):
            return dict(value)
        return {"_input": value}

    def _merge_input_payloads(self, context_inputs: Optional[Dict[str, Any]], direct_input: Any) -> Dict[str, Any]:
        merged_input: Dict[str, Any] = dict(context_inputs or {})
        if direct_input is not NO_INPUT:
            if isinstance(direct_input, dict):
                merged_input.update(direct_input)
            else:
                merged_input["_input"] = direct_input
        return merged_input

    def _prepare_input_payload(self, direct_input: Any, effective_context: Optional[ExecutionContext]) -> Dict[str, Any]:
        context_inputs = self._context_inputs_from_declaration(effective_context)
        return self._merge_input_payloads(context_inputs, direct_input)

    @staticmethod
    def _unwrap_input_payload(payload: Any) -> Any:
        if isinstance(payload, dict):
            if not payload:
                return NO_INPUT
            if set(payload.keys()) == {"_input"}:
                return payload["_input"]
        return payload

    def _get_cache_key(self, input_data: Any, context: Optional[ExecutionContext]) -> Any:
        return self._cache_key_generator(self.name, input_data, context, self.input_declaration)

    def set_on_start(self, handler: Union['Runnable', Callable[[ExecutionContext, Any], None]]) -> 'Runnable':
        if not isinstance(handler, Runnable) and not callable(handler):
            raise TypeError("on_start handler must be a Runnable instance or a callable.")
        self._on_start_handler = handler
        return self

    def get_on_start_handler(self) -> Optional[Union['Runnable', Callable[[ExecutionContext, Any], None]]]:
        return self._on_start_handler

    def set_on_complete(self, handler: Union['Runnable', Callable[[ExecutionContext, Any, Optional[Exception]], None]]) -> 'Runnable':
        if not isinstance(handler, Runnable) and not callable(handler):
            raise TypeError("on_complete handler must be a Runnable instance or a callable.")
        self._on_complete_handler = handler
        return self

    def get_on_complete_handler(self) -> Optional[Union['Runnable', Callable[[ExecutionContext, Any, Optional[Exception]], None]]]:
        return self._on_complete_handler

    def on_error(self, handler: Union['Runnable', Callable[[ExecutionContext, Any, Exception], Any]]) -> 'Runnable':
        if not isinstance(handler, Runnable) and not callable(handler):
            raise TypeError("Error handler must be a Runnable instance or a callable.")
        self._error_handler = handler
        return self

    def get_error_handler(self) -> Optional[Union['Runnable', Callable[[ExecutionContext, Any, Exception], Any]]]:
        return self._error_handler

    def invoke(self, input_data: Any = NO_INPUT, context: Optional[ExecutionContext] = None) -> Any:
        effective_context = context if context is not None else InMemoryExecutionContext(initial_input=input_data if input_data is not NO_INPUT else None)
        prepared_input_for_invoke = self._prepare_input_payload(input_data, effective_context)

        task_final_result: Any = NO_INPUT
        task_final_exception: Optional[Exception] = None

        if self._on_start_handler:
            try:
                effective_context.log_event(f"Node '{self.name}': Executing on_start handler.")
                if isinstance(self._on_start_handler, Runnable):
                    self._on_start_handler.invoke(prepared_input_for_invoke, effective_context)
                elif callable(self._on_start_handler):
                    self._on_start_handler(effective_context, prepared_input_for_invoke)
            except Exception as e_start_hook:
                effective_context.log_event(f"Node '{self.name}': Error in on_start handler: {type(e_start_hook).__name__}: {e_start_hook}. Task execution will be skipped.")
                task_final_exception = e_start_hook
        
        if task_final_exception is None:
            cache_key = None # Initialize cache_key
            if self.use_cache: 
                cache_key = self._get_cache_key(prepared_input_for_invoke, effective_context)
                if cache_key in self._invoke_cache:
                    effective_context.log_event(f"Node '{self.name}': Invoke result from cache.")
                    task_final_result = self._invoke_cache[cache_key]
                    if self.name: 
                        effective_context.add_output(self.name, task_final_result)

            if not (self.use_cache and cache_key is not None and cache_key in self._invoke_cache):
                if not self.use_cache:
                    effective_context.log_event(f"Node '{self.name}': Caching disabled (use_cache=False).")

                current_attempt = 0
                max_attempts = (self._retry_config or {}).get("max_attempts", 1)
                delay_seconds = (self._retry_config or {}).get("delay_seconds", 0)
                retry_on_exceptions = (self._retry_config or {}).get("retry_on_exceptions", (Exception,)) 

                while current_attempt < max_attempts:
                    current_attempt += 1
                    effective_context.log_event(f"Node '{self.name}': Invoking (Attempt {current_attempt}/{max_attempts}). Input type: {type(prepared_input_for_invoke).__name__}")
                    
                    try:
                        result_internal = self._internal_invoke(prepared_input_for_invoke, effective_context)
                        task_final_result = result_internal
                        task_final_exception = None 
                        
                        if self.use_cache and cache_key is not None: # <<< MODIFIED: Check use_cache before writing to cache
                            self._invoke_cache[cache_key] = task_final_result
                        effective_context.log_event(f"Node '{self.name}': Invoked successfully. Output type: {type(task_final_result).__name__}.")
                        if self.name:
                            effective_context.add_output(self.name, task_final_result)
                        break 
                    except Exception as e_internal:
                        effective_context.log_event(f"Node '{self.name}': Error during invoke (Attempt {current_attempt}/{max_attempts}): {type(e_internal).__name__}: {e_internal}")
                        task_final_exception = e_internal 

                        is_retryable_exception_type = isinstance(e_internal, retry_on_exceptions)
                        is_last_attempt = (current_attempt == max_attempts)

                        if not is_retryable_exception_type or is_last_attempt:
                            if self._error_handler:
                                effective_context.log_event(f"Node '{self.name}': Attempting to execute on_error handler.")
                                try:
                                    error_handler_output: Any
                                    if isinstance(self._error_handler, Runnable):
                                        error_handler_output = self._error_handler.invoke(prepared_input_for_invoke, effective_context)
                                    elif callable(self._error_handler): 
                                        error_handler_output = self._error_handler(effective_context, prepared_input_for_invoke, e_internal)
                                    
                                    task_final_result = error_handler_output
                                    task_final_exception = None 
                                    
                                    if self.use_cache and cache_key is not None: # <<< MODIFIED: Check use_cache
                                        self._invoke_cache[cache_key] = task_final_result 
                                    effective_context.log_event(f"Node '{self.name}': on_error handler executed successfully.")
                                    if self.name: 
                                        effective_context.add_output(self.name, task_final_result)
                                except Exception as e_error_handler:
                                    effective_context.log_event(f"Node '{self.name}': on_error handler also failed: {type(e_error_handler).__name__}: {e_error_handler}")
                                    task_final_exception = e_error_handler 
                                break 
                            else:
                                break 
                            
                        effective_context.log_event(f"Node '{self.name}': Retrying in {delay_seconds} seconds.")
                        if delay_seconds > 0: 
                            time.sleep(delay_seconds)
        
        if self._on_complete_handler:
            try:
                effective_context.log_event(f"Node '{self.name}': Executing on_complete handler.")
                result_for_callable = None
                if task_final_result is not NO_INPUT:
                    result_for_callable = task_final_result
                
                if isinstance(self._on_complete_handler, Runnable):
                    self._on_complete_handler.invoke(prepared_input_for_invoke, effective_context)
                elif callable(self._on_complete_handler):
                    self._on_complete_handler(effective_context, result_for_callable, task_final_exception)
            except Exception as e_complete_hook:
                effective_context.log_event(f"Node '{self.name}': Error in on_complete handler: {type(e_complete_hook).__name__}: {e_complete_hook}. This error is logged but does not alter task outcome.")

        if task_final_exception is not None:
            raise task_final_exception
        
        return task_final_result

    @abc.abstractmethod
    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        pass

    async def invoke_async(self, input_data: Any = NO_INPUT, context: Optional[ExecutionContext] = None) -> Any:
        loop = asyncio.get_event_loop()

        return await loop.run_in_executor(None, self.invoke, input_data, context)

    def check(self, data_from_invoke: Any, context: Optional[ExecutionContext] = None) -> bool:
        if not self.use_cache: 
            if context: context.log_event(f"Node '{self.name}': Check caching disabled (use_cache=False).")
            if self._custom_check_fn:
                return self._custom_check_fn(data_from_invoke)
            return self._default_check(data_from_invoke)

        cache_key_data = data_from_invoke
        try:
            cache_key = pickle.dumps(cache_key_data)
        except Exception:
            cache_key = object() 

        if cache_key in self._check_cache:
            if context: context.log_event(f"Node '{self.name}': Check result from cache.")
            return self._check_cache[cache_key]

        if self._custom_check_fn:
            result = self._custom_check_fn(data_from_invoke)
        else:
            result = self._default_check(data_from_invoke)

        self._check_cache[cache_key] = result

        if context: context.log_event(f"Node '{self.name}': Check result: {result}.")
        return result

    def _default_check(self, data_from_invoke: Any) -> bool:
        return bool(data_from_invoke)

    async def check_async(self, data_from_invoke: Any, context: Optional[ExecutionContext] = None) -> bool:
        if self._custom_async_check_fn:
            if not self.use_cache: 
                if context: context.log_event(f"Node '{self.name}': Async check caching may be active via custom_async_check_fn despite use_cache=False for invoke.")
            return await self._custom_async_check_fn(data_from_invoke) 
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.check, data_from_invoke, context)

    async def _default_check_async(self, data_from_invoke: Any) -> bool:
        return bool(data_from_invoke) # Default async check is simple

    def set_check(self, func: Union[Callable[[Any], bool], Callable[[Any], Coroutine[Any, Any, bool]]]):
        if asyncio.iscoroutinefunction(func):
            self._custom_async_check_fn = func
            self._custom_check_fn = None 
        elif callable(func):
            self._custom_check_fn = func
            self._custom_async_check_fn = None 
        else:
            raise TypeError("Custom check function must be callable or a coroutine function.")
        self.clear_cache('_check_cache')
        return self

    def retry(self, max_attempts: int = 3, delay_seconds: Union[int, float] = 1, retry_on_exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = (Exception,)):
        if not isinstance(max_attempts, int) or max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer.")
        if not isinstance(delay_seconds, (int, float)) or delay_seconds < 0:
            raise ValueError("delay_seconds must be a non-negative number.")
        
        if isinstance(retry_on_exceptions, type) and issubclass(retry_on_exceptions, Exception):
            retry_exceptions_tuple = (retry_on_exceptions,)
        elif isinstance(retry_on_exceptions, tuple) and all(isinstance(e, type) and issubclass(e, Exception) for e in retry_on_exceptions):
            retry_exceptions_tuple = retry_on_exceptions
        else:
            raise ValueError("retry_on_exceptions must be an Exception type or a tuple of Exception types.")

        self._retry_config = {
            "max_attempts": max_attempts,
            "delay_seconds": delay_seconds,
            "retry_on_exceptions": retry_exceptions_tuple
        }
        return self

    def clear_cache(self, cache_name: str = 'all'):
        if cache_name == '_invoke_cache' or cache_name == 'all':
            self._invoke_cache.clear()
        if cache_name == '_check_cache' or cache_name == 'all':
            self._check_cache.clear()
        return self

    def __or__(self, other: Union['Runnable', Dict[str, 'Runnable']]) -> 'Runnable':
        # This version of __or__ will always create a synchronous Pipeline/BranchAndFanIn.
        # AsyncRunnable will override __or__ and __ror__ to create AsyncPipeline.
        if isinstance(other, Runnable):
            return Pipeline(self, other, name=f"({self.name} | {other.name})")
        elif isinstance(other, dict) and all(isinstance(r, Runnable) for r in other.values()):
            branch_fan_in = BranchAndFanIn(other, name=f"BranchFanIn_after_{self.name}")
            return Pipeline(self, branch_fan_in, name=f"({self.name} | {branch_fan_in.name})")
        return NotImplemented

    def __mod__(self, true_branch: 'Runnable') -> '_PendingConditional':
        if not isinstance(true_branch, Runnable):
            return NotImplemented
        return _PendingConditional(self, true_branch)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name='{self.name}'>"

    def copy(self) -> 'Runnable':
        import copy
        new_runnable = copy.deepcopy(self)
        new_runnable._invoke_cache = {}
        new_runnable._check_cache = {}
        return new_runnable

    def to_graph(self, graph_name: Optional[str] = None) -> 'WorkflowGraph':
        from .graph import WorkflowGraph  # Local import to avoid circular dependency at module load

        graph = WorkflowGraph(name=graph_name or f"GraphFrom_{self.name}")
        helper = _GraphExportHelper()
        entry_nodes, exit_nodes = self._expand_to_graph(graph, helper)

        if not entry_nodes or not exit_nodes:
            raise ValueError(f"Runnable '{self.name}' could not be exported to a graph.")

        for entry in entry_nodes:
            graph.set_entry_point(entry)
        graph.set_output_nodes(exit_nodes)
        return graph

    def _expand_to_graph(self, graph: 'WorkflowGraph', helper: '_GraphExportHelper') -> Tuple[List[str], List[str]]:
        node_name = helper.add_node(graph, self)
        return [node_name], [node_name]


class _PendingConditional:
    def __init__(self, condition_r: Runnable, true_r: Runnable):
        self.condition_r = condition_r
        self.true_r = true_r
        self.name_hint = f"({condition_r.name} % {true_r.name})"

    def __rshift__(self, false_r: Runnable) -> 'Conditional':
        if not isinstance(false_r, Runnable):
            return NotImplemented
        name = f"({self.name_hint} >> {false_r.name})"
        return Conditional(self.condition_r, self.true_r, false_r, name=name)


class SimpleTask(Runnable):
    def __init__(self, func: Callable, name: Optional[str] = None, input_declaration: Any = None, **kwargs):
        default_name = getattr(func, '__name__', None)
        if default_name == '<lambda>': 
             default_name = None 

        super().__init__(name or default_name, input_declaration=input_declaration, **kwargs)
        
        if not callable(func):
            raise TypeError("func must be a callable")
        self.func = func
        self._func_signature = inspect.signature(func)
        self._func_param_names = tuple(self._func_signature.parameters.keys())
        self._accepts_var_kwargs = any(
            param.kind == inspect.Parameter.VAR_KEYWORD
            for param in self._func_signature.parameters.values()
        )
        self._expects_context = 'context' in self._func_signature.parameters

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        if isinstance(input_data, dict):
            payload = dict(input_data)
        elif input_data is NO_INPUT:
            payload = {}
        else:
            payload = {"_input": input_data}

        call_kwargs = dict(payload)
        call_args: List[Any] = []

        direct_value = call_kwargs.pop('_input', NO_INPUT)
        if direct_value is not NO_INPUT:
            if '_input' in self._func_param_names or self._accepts_var_kwargs:
                call_kwargs['_input'] = direct_value
            else:
                call_args.append(direct_value)

        if self._expects_context and 'context' not in call_kwargs:
            call_kwargs['context'] = context

        return self.func(*call_args, **call_kwargs)


class Pipeline(Runnable):
    def __init__(self, first: Runnable, second: Runnable, name: Optional[str] = None, **kwargs):
        effective_name = name or f"Pipeline[{first.name}_then_{second.name}]"
        super().__init__(name=effective_name, **kwargs) 
        
        if not isinstance(first, Runnable) or not isinstance(second, Runnable):
            raise TypeError("Both 'first' and 'second' must be Runnable instances.")
        self.first = first
        self.second = second
        if self.input_declaration is None and first.input_declaration is not None:
            self.input_declaration = first.input_declaration


    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        output_first = self.first.invoke(input_data, context)
        output_second = self.second.invoke(output_first, context)
        return output_second

    def _default_check(self, data_from_invoke: Any) -> bool:
        return self.second.check(data_from_invoke) 

    def _expand_to_graph(self, graph: 'WorkflowGraph', helper: '_GraphExportHelper') -> Tuple[List[str], List[str]]:
        entry_first, exit_first = self.first._expand_to_graph(graph, helper)
        entry_second, exit_second = self.second._expand_to_graph(graph, helper)
        for parent in exit_first:
            for child in entry_second:
                graph.add_edge(parent, child)
        return entry_first, exit_second


class Conditional(Runnable):
    def __init__(self, condition_r: Runnable, true_r: Runnable, false_r: Runnable, name: Optional[str] = None, **kwargs):
        super().__init__(name or f"Cond[{condition_r.name}?{true_r.name}:{false_r.name}]", **kwargs)
        self.condition_r = condition_r
        self.true_r = true_r
        self.false_r = false_r

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        condition_output = self.condition_r.invoke(input_data, context)
        
        context.log_event(f"Node '{self.name}': Condition output type: {type(condition_output).__name__}. Checking condition.")
        if getattr(self.condition_r, "_custom_async_check_fn", None) is not None:
            raise RuntimeError(
                f"Conditional '{self.name}' detected async check logic on '{self.condition_r.name}'. "
                "Use AsyncConditional to evaluate asynchronous checks."
            )
        if self.condition_r.check(condition_output, context): 
            context.log_event(f"Node '{self.name}': Condition TRUE, executing '{self.true_r.name}'.")
            return self.true_r.invoke(condition_output, context)
        else:
            context.log_event(f"Node '{self.name}': Condition FALSE, executing '{self.false_r.name}'.")
            return self.false_r.invoke(condition_output, context)


class BranchAndFanIn(Runnable):
    def __init__(self, tasks_dict: Dict[str, Runnable], name: Optional[str] = None, max_workers: Optional[int] = None, **kwargs):
        super().__init__(name or f"BranchFanIn_{'_'.join(tasks_dict.keys())}", **kwargs)
        if not isinstance(tasks_dict, dict) or not all(isinstance(r, Runnable) for r in tasks_dict.values()):
            raise TypeError("tasks_dict must be a dictionary of Runnables.")
        self.tasks_dict = tasks_dict
        self.max_workers = max_workers

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        context.log_event(f"Node '{self.name}': Starting parallel execution for {len(self.tasks_dict)} tasks.")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_key = {
                executor.submit(
                    task.invoke,
                    input_data if not task.input_declaration else NO_INPUT,
                    context
                ): key
                for key, task in self.tasks_dict.items()
            }
            exceptions = {}
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                task_name = self.tasks_dict[key].name
                try:
                    result = future.result()
                    results[key] = result
                    context.log_event(f"Node '{self.name}', Branch '{key}' ({task_name}): Completed.")
                except Exception as e:
                    context.log_event(f"Node '{self.name}', Branch '{key}' ({task_name}): Failed: {type(e).__name__}: {e}")
                    exceptions[key] = e
            
            if exceptions:
                # Aggregate or raise the first/most significant exception
                # For now, raising the first one encountered by key order
                first_failed_key = sorted(exceptions.keys())[0]
                raise exceptions[first_failed_key]


        context.log_event(f"Node '{self.name}': All parallel tasks completed.")
        return results


class SourceParallel(Runnable):
    def __init__(self, tasks_dict: Dict[str, Runnable], name: Optional[str] = None, max_workers: Optional[int] = None, **kwargs):
        super().__init__(name or f"SourceParallel_{'_'.join(tasks_dict.keys())}", **kwargs)
        if not isinstance(tasks_dict, dict) or not all(isinstance(r, Runnable) for r in tasks_dict.values()):
            raise TypeError("tasks_dict must be a dictionary of Runnables.")
        self.tasks_dict = tasks_dict
        self.max_workers = max_workers

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Dict[str, Any]:
        unwrapped_input = self._unwrap_input_payload(input_data)
        actual_input = context.initial_input if unwrapped_input is NO_INPUT else unwrapped_input
        context.log_event(f"Node '{self.name}': Starting parallel execution for {len(self.tasks_dict)} tasks. Input type: {type(actual_input).__name__}")

        aggregated_results: Dict[str, Any] = {}
        # Keep track of futures to handle exceptions properly
        future_to_branch_key: Dict[Any, str] = {}
        exceptions = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for key, task_runnable in self.tasks_dict.items():
                # Each branch in SourceParallel should typically get its own sub-context
                # if isolation is desired, or use the passed context directly.
                # For simplicity here, using the main context for each branch.
                # A more advanced version might create sub-contexts.
                future = executor.submit(task_runnable.invoke, actual_input, context)
                future_to_branch_key[future] = key
            
            for future in as_completed(future_to_branch_key):
                key = future_to_branch_key[future]
                task_name = self.tasks_dict[key].name
                try:
                    branch_output_value = future.result()
                    aggregated_results[key] = branch_output_value
                    context.log_event(f"Node '{self.name}', Branch '{key}' ({task_name}): Completed. Result type: {type(branch_output_value).__name__}.")
                except Exception as e:
                    context.log_event(f"Node '{self.name}', Branch '{key}' ({task_name}): FAILED with {type(e).__name__}: {str(e)[:100]}.")
                    exceptions[key] = e
        
        if exceptions:
            first_failed_key = sorted(exceptions.keys())[0]
            raise exceptions[first_failed_key]
        
        for key_of_branch, branch_output_value in aggregated_results.items():
            context_key_for_branch_output = f"{self.name}_{key_of_branch}"
            context.add_output(context_key_for_branch_output, branch_output_value)
            # context.log_event(f"Node '{self.name}': Added output for branch '{key_of_branch}' as '{context_key_for_branch_output}' to main context.")

        context.log_event(f"Node '{self.name}': All branches completed. Aggregated results: {list(aggregated_results.keys())}.")
        return aggregated_results


class While(Runnable):
    def __init__(self, condition_check_runnable: Runnable, body_runnable: Runnable, max_loops: int = 100, name: Optional[str] = None, **kwargs):
        super().__init__(name or f"While[{condition_check_runnable.name}_do_{body_runnable.name}]", **kwargs)
        self.condition_check_runnable = condition_check_runnable
        self.body_runnable = body_runnable
        self.max_loops = max_loops

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> List[Any]:
        loop_count = 0
        current_input_for_iteration = input_data 
        all_body_outputs = []

        context.log_event(f"Node '{self.name}': Starting loop (max_loops={self.max_loops}).")

        while loop_count < self.max_loops:
            context.log_event(f"Node '{self.name}': Loop {loop_count + 1}/{self.max_loops}. Evaluating condition '{self.condition_check_runnable.name}'. Input type: {type(current_input_for_iteration).__name__}")
            
            condition_input = self._unwrap_input_payload(current_input_for_iteration)
            condition_output = self.condition_check_runnable.invoke(condition_input, context)
            
            context.log_event(f"Node '{self.name}': Loop {loop_count + 1}. Condition output type: {type(condition_output).__name__}. Checking condition.")
            if not self.condition_check_runnable.check(condition_output, context):
                context.log_event(f"Node '{self.name}': Condition FALSE. Exiting loop after {loop_count} iterations.")
                break
            
            context.log_event(f"Node '{self.name}': Loop {loop_count + 1}, Condition TRUE, executing body '{self.body_runnable.name}'. Body input type: {type(current_input_for_iteration).__name__}")
            
            history_snapshot = [self._unwrap_input_payload(item) for item in all_body_outputs]
            loop_input_payload = {
                "last": self._unwrap_input_payload(current_input_for_iteration),
                "history": history_snapshot,
                "iteration": loop_count,
            }
            body_output = self.body_runnable.invoke(loop_input_payload, context)
            all_body_outputs.append(body_output)
            
            current_input_for_iteration = body_output 
            loop_count += 1
        else: 
            context.log_event(f"Node '{self.name}': Loop exited due to max_loops ({self.max_loops}) reached.")

        context.log_event(f"Node '{self.name}': Loop finished. Returning {len(all_body_outputs)} outputs.")
        return all_body_outputs


class MergeInputs(Runnable):
    def __init__(self, input_sources: Dict[str, str], merge_function: Callable[..., Any], name: Optional[str] = None, **kwargs):
        super().__init__(name or f"MergeInputs_{getattr(merge_function, '__name__', 'custom')}", **kwargs)
        self.input_sources = input_sources
        self.merge_function = merge_function
        self.input_declaration = self.input_sources 

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        kwargs_for_func = {}
        context.log_event(f"Node '{self.name}': Fetching inputs from context: {list(self.input_sources.values())}")

        for param_name, source_node_name in self.input_sources.items():
            value = context.get_output(source_node_name)
            kwargs_for_func[param_name] = value
        
        context.log_event(f"Node '{self.name}': Calling merge_function '{getattr(self.merge_function, '__name__', 'custom')}'.")
        return self.merge_function(**kwargs_for_func)


class _GraphExportHelper:
    def __init__(self):
        self._name_counter: Dict[str, int] = {}

    def _unique_name(self, base_name: Optional[str]) -> str:
        sanitized = base_name or "Node"
        count = self._name_counter.get(sanitized, 0)
        unique = sanitized if count == 0 else f"{sanitized}_{count}"
        self._name_counter[sanitized] = count + 1
        return unique

    def add_node(self, graph: 'WorkflowGraph', runnable: Runnable) -> str:
        node_name = self._unique_name(runnable.name)
        graph.add_node(runnable, node_name=node_name)
        return node_name