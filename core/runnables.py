import abc
import time
import logging
import hashlib
import pickle
from typing import Any, Callable, Optional, Dict, List, Tuple, Union, Type
from concurrent.futures import ThreadPoolExecutor, as_completed

# 用于标记没有显式输入的情况
NO_INPUT = object()

logger = logging.getLogger(__name__)
# 为了避免重复配置（如果用户在其他地方也配置了basicConfig）
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class ExecutionContext:
    """
    一个在工作流执行期间携带状态和节点输出的对象。
    """
    def __init__(self, initial_input: Any = NO_INPUT, parent_context: Optional['ExecutionContext'] = None):
        # logger.debug(f"DEBUG_CONTEXT_INIT: ExecutionContext {id(self)} CREATED. Initial input type: {type(initial_input).__name__}. Parent context ID: {id(parent_context) if parent_context else 'None'}")
        self.node_outputs: Dict[str, Any] = {}
        self.initial_input: Any = initial_input
        self.event_log: List[str] = []
        self.parent_context: Optional['ExecutionContext'] = parent_context # 用于图的嵌套上下文
        # logger.debug(f"DEBUG: ExecutionContext {id(self)} created. Parent: {id(parent_context) if parent_context else None}") # Debug print

    def add_output(self, node_name: str, value: Any):
        """将节点输出添加到上下文中。"""
        # logger.debug(f"DEBUG: ExecutionContext {id(self)}: Adding output for node '{node_name}'. Value: {str(value)[:60]}...") # Debug print
        if node_name:
            self.node_outputs[node_name] = value
            self.log_event(f"Output added for node '{node_name}'. Value type: {type(value).__name__}")
        else:
            self.log_event(f"Output for unnamed node not added to context's node_outputs.")

    def get_output(self, node_name: str, default: Any = None) -> Any:
        """
        从上下文中获取节点输出。
        如果当前上下文没有，且存在父上下文，则尝试从父上下文获取。
        """
        logger.debug(f"DEBUG: ExecutionContext {id(self)}: Getting output for '{node_name}'. Current outputs: {list(self.node_outputs.keys())}") # Debug print
        if node_name in self.node_outputs:
            return self.node_outputs[node_name]
        if self.parent_context:
            # logger.debug(f"Context '{id(self)}': Node '{node_name}' not in current context, trying parent context '{id(self.parent_context)}'.")
            return self.parent_context.get_output(node_name, default)
        # logger.debug(f"Context '{id(self)}': Node '{node_name}' not found in current or parent contexts.")
        return default

    def log_event(self, message: str):
        """记录一个事件到事件日志。"""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        full_message = f"[{timestamp}] {message}"
        logger.debug(f"ContextEvent: {message}") # 使用 debug级别以减少默认输出
        self.event_log.append(full_message)

    def __repr__(self) -> str:
        return f"<ExecutionContext id={id(self)} initial_input={str(self.initial_input)[:60]}, node_outputs_keys={list(self.node_outputs.keys())}>"

def _default_cache_key_generator(runnable_name: str, input_data: Any, context: Optional[ExecutionContext], input_declaration: Any) -> Any:
    """
    默认的缓存键生成器。
    尝试序列化输入数据和从上下文中根据 input_declaration 获取的相关数据。
    """
    try:
        # 1. 直接输入数据
        input_data_bytes = pickle.dumps(input_data)

        # 2. 从上下文中获取的声明式输入
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
                 # For callable declaration, we might need to hash the function itself or its source,
                 # and potentially the context values it *would* use. This is complex.
                 # For simplicity, if declaration is callable, we might just include the declaration object itself
                 # or skip context-based hashing for this case. Let's include the declaration object for now.
                 relevant_context_outputs["_declaration_callable"] = input_declaration # Not ideal for hashing
            
            # Convert to a hashable format
            # Handle potential unhashable values within relevant_context_outputs
            hashable_context_items = []
            for k, v in sorted(relevant_context_outputs.items()):
                 try:
                      hashable_context_items.append((k, pickle.dumps(v)))
                 except Exception:
                      # If value is not picklable, use its string representation and type as a fallback hash component
                      hashable_context_items.append((k, str(v) + type(v).__name__))

            declared_inputs_tuple = tuple(hashable_context_items)

        declared_inputs_bytes = pickle.dumps(declared_inputs_tuple)

        # Combine and hash
        hasher = hashlib.md5()
        hasher.update(input_data_bytes)
        hasher.update(declared_inputs_bytes)
        hasher.update(runnable_name.encode('utf-8')) # Ensure name is bytes
        return hasher.hexdigest()
    except Exception as e: # pickle or other error during key generation
        # logger.warning(f"Cache key generation failed for {runnable_name} with error: {e}. Using object() as key (cache disabled for this call).", exc_info=True)
        return object() # Return a unique object, effectively disabling cache for this specific call


class Runnable(abc.ABC):
    """
    系统的基本构建块。每个 Runnable 代表工作流中的一个操作或步骤。
    """
    def __init__(self, name: Optional[str] = None, input_declaration: Any = None, cache_key_generator: Optional[Callable[[str, Any, Optional[ExecutionContext], Any], Any]] = None):
        # Ensure name is always a non-empty string
        self.name: str = name if name else f"{self.__class__.__name__}_{id(self)}"
        if not isinstance(self.name, str) or not self.name:
             self.name = f"{self.__class__.__name__}_{id(self)}" # Fallback if provided name is invalid

        self._invoke_cache: Dict[Any, Any] = {}
        self._check_cache: Dict[Any, bool] = {}
        self._custom_check_fn: Optional[Callable[[Any], bool]] = None
        self._error_handler: Optional['Runnable'] = None
        self._retry_config: Optional[Dict[str, Any]] = None
        self.input_declaration: Any = input_declaration
        self._cache_key_generator = cache_key_generator or _default_cache_key_generator

    def _get_cache_key(self, input_data: Any, context: Optional[ExecutionContext]) -> Any:
        return self._cache_key_generator(self.name, input_data, context, self.input_declaration)

    def invoke(self, input_data: Any = NO_INPUT, context: Optional[ExecutionContext] = None) -> Any:
        # logger.debug(f"DEBUG: Runnable '{self.name}' invoke called. Input type: {type(input_data).__name__}. Passed Context ID: {id(context)}") # Debug print
        effective_context = context if context is not None else ExecutionContext(initial_input=input_data if input_data is not NO_INPUT else None)
        # logger.debug(f"DEBUG: Runnable '{self.name}' using effective_context ID: {id(effective_context)}") # Debug print
        
        # Determine the actual input data for _internal_invoke and cache key generation
        actual_input_for_invoke = input_data
        if self.input_declaration and input_data is NO_INPUT and effective_context:
            if isinstance(self.input_declaration, str):
                actual_input_for_invoke = effective_context.get_output(self.input_declaration)
                # logger.debug(f"Runnable '{self.name}': Fetched input '{self.input_declaration}' from context: {str(actual_input_for_invoke)[:60]}...")
            elif isinstance(self.input_declaration, dict):
                kwargs_from_context = {}
                for param_name, source_key in self.input_declaration.items():
                    if isinstance(source_key, str): # Ensure source_key is a string for context lookup
                        kwargs_from_context[param_name] = effective_context.get_output(source_key)
                    else:
                         # Handle cases where source_key might be a literal value or other type
                         kwargs_from_context[param_name] = source_key # Use the source_key directly
                         # logger.warning(f"Runnable '{self.name}': input_declaration source key for '{param_name}' is not a string ('{source_key}'). Using value directly.")

                actual_input_for_invoke = kwargs_from_context
                # logger.debug(f"Runnable '{self.name}': Fetched inputs from context based on declaration: {list(kwargs_from_context.keys())}")
            elif callable(self.input_declaration): # If input_declaration is a function
                actual_input_for_invoke = self.input_declaration(effective_context)
                # logger.debug(f"Runnable '{self.name}': Fetched input from context using a custom function.")
            # else: input_declaration is some other type, actual_input_for_invoke remains NO_INPUT or original input_data

        cache_key = self._get_cache_key(actual_input_for_invoke, effective_context)

        if cache_key in self._invoke_cache:
            # logger.info(f"Runnable '{self.name}': Using cached invoke result for key '{str(cache_key)[:60]}'.")
            effective_context.log_event(f"Node '{self.name}': Invoke result from cache.")
            result = self._invoke_cache[cache_key]
            # Even if cached, add to context for downstream nodes in the current run
            if self.name and effective_context: # Ensure context exists
                effective_context.add_output(self.name, result)
            
            return result

        current_attempt = 0
        max_attempts = (self._retry_config or {}).get("max_attempts", 1)
        delay_seconds = (self._retry_config or {}).get("delay_seconds", 0)
        retry_on_exceptions = (self._retry_config or {}).get("retry_on_exceptions", (Exception,)) # Default to retry on all exceptions

        while current_attempt < max_attempts:
            current_attempt += 1
            # logger.debug(f"Runnable '{self.name}': Attempt {current_attempt}/{max_attempts}. Input type: {type(actual_input_for_invoke).__name__}")
            effective_context.log_event(f"Node '{self.name}': Invoking (Attempt {current_attempt}/{max_attempts}). Input type: {type(actual_input_for_invoke).__name__}")

            try:
                result = self._internal_invoke(actual_input_for_invoke, effective_context)
                
                self._invoke_cache[cache_key] = result
                # logger.info(f"Runnable '{self.name}': Invoked successfully. Output type: {type(result).__name__}. Output: {str(result)[:60]}...")
                effective_context.log_event(f"Node '{self.name}': Invoked successfully. Output type: {type(result).__name__}.")

                # Always add output to context if the node has a name
                if self.name:
                    effective_context.add_output(self.name, result)
                
                return result

            except Exception as e:
                # logger.error(f"Runnable '{self.name}': Error during invoke (Attempt {current_attempt}/{max_attempts}): {type(e).__name__}: {e}", exc_info=logger.level <= logging.DEBUG)
                effective_context.log_event(f"Node '{self.name}': Error during invoke (Attempt {current_attempt}/{max_attempts}): {type(e).__name__}: {e}")

                is_retryable = isinstance(e, retry_on_exceptions)
                is_last_attempt = (current_attempt == max_attempts)

                if not is_retryable or is_last_attempt:
                    # If not retryable, or it's the last attempt and still failed
                    if self._error_handler:
                        # logger.info(f"Runnable '{self.name}': Error handler '{self._error_handler.name}' is defined. Invoking error handler.")
                        effective_context.log_event(f"Node '{self.name}': Invoking error handler '{self._error_handler.name}'.")
                        try:
                            # Error handler receives the input that caused the failure and the context
                            error_handler_output = self._error_handler.invoke(actual_input_for_invoke, effective_context)
                            # logger.info(f"Runnable '{self.name}': Error handler '{self._error_handler.name}' executed. Output type: {type(error_handler_output).__name__}. Output: {str(error_handler_output)[:60]}...")
                            effective_context.log_event(f"Node '{self.name}': Error handler '{self._error_handler.name}' executed.")
                            # Add error handler's output to context under the original node's name
                            if self.name:
                                effective_context.add_output(self.name, error_handler_output)
                            self._invoke_cache[cache_key] = error_handler_output # Cache error handler's result
                            return error_handler_output # Return error handler's output as this node's result
                        except Exception as eh_e:
                            # logger.error(f"Runnable '{self.name}': Error handler '{self._error_handler.name}' also failed: {type(eh_e).__name__}: {eh_e}", exc_info=logger.level <= logging.DEBUG)
                            effective_context.log_event(f"Node '{self.name}': Error handler '{self._error_handler.name}' also failed: {type(eh_e).__name__}: {eh_e}")
                            # If error handler fails, re-raise the original exception (or the handler's exception?)
                            # Re-raising the original exception seems more predictable for upstream error handling
                            raise e from eh_e # Chain the exceptions

                    else:
                        # No error handler and no more retries or not retryable
                        # Before raising, let's see the context state
                        # logger.debug(f"DEBUG_INVOKE_EXIT (RAISE): Runnable '{self.name}' (id: {id(self)}) PRE-RAISE. EFFECTIVE_CONTEXT ({id(effective_context)}) outputs: {list(effective_context.node_outputs.keys())}")
                        raise e # Re-raise the original exception
            
            # If retryable and not last attempt
            # logger.info(f"Runnable '{self.name}': Retrying in {delay_seconds} seconds...")
                effective_context.log_event(f"Node '{self.name}': Retrying in {delay_seconds} seconds.")
                time.sleep(delay_seconds)
        
        # This part should ideally not be reached if max_attempts >= 1 and exceptions are handled/retried
        # If we reach here, it means the loop finished without returning or raising, which is unexpected.
        # Re-raising the last error is a safe fallback.
        # The last error 'e' is still available here due to Python's exception handling scope.
        try:
            # Check if 'e' was defined in the last except block
            _ = e
            raise e # Re-raise the last exception
        except NameError:
             # Should not happen if max_attempts > 0 and loop was entered
             raise RuntimeError(f"Runnable '{self.name}': Unexpected exit from retry loop.")


    @abc.abstractmethod
    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        pass

    def check(self, data_from_invoke: Any, context: Optional[ExecutionContext] = None) -> bool:
        # Check cache key should ideally also consider context if check function uses it.
        # For simplicity, using data_from_invoke directly as key for now.
        # A more robust key would be similar to _get_cache_key.
        cache_key_data = data_from_invoke
        try:
            # Attempt to make it more robust by pickling
            cache_key = pickle.dumps(cache_key_data)
        except Exception:
            cache_key = object() # Fallback if not picklable

        if cache_key in self._check_cache:
            # logger.debug(f"Runnable '{self.name}': Using cached check result for key '{str(cache_key)[:60]}'.")
            if context: context.log_event(f"Node '{self.name}': Check result from cache.")
            return self._check_cache[cache_key]

        if self._custom_check_fn:
            # logger.debug(f"Runnable '{self.name}': Using custom check function.")
            result = self._custom_check_fn(data_from_invoke)
        else:
            # logger.debug(f"Runnable '{self.name}': Using default check function.")
            result = self._default_check(data_from_invoke)

        self._check_cache[cache_key] = result
        # logger.debug(f"Runnable '{self.name}': Check result: {result}.")
        if context: context.log_event(f"Node '{self.name}': Check result: {result}.")
        return result

    def _default_check(self, data_from_invoke: Any) -> bool:
        """
        默认的检查逻辑，通常是基于真值测试。
        MockRunnable 的 _check_side_effect 逻辑不应在此处。
        """
        return bool(data_from_invoke)

    def set_check(self, func: Callable[[Any], bool]):
        """允许用户自定义 check 方法的逻辑。"""
        if not callable(func):
             raise TypeError("Custom check function must be callable.")
        self._custom_check_fn = func
        self.clear_cache('_check_cache') # 自定义检查函数改变，应清除检查缓存
        # logger.debug(f"Runnable '{self.name}': Custom check function set.")
        return self

    def on_error(self, error_handler_runnable: 'Runnable'):
        """指定一个 Runnable 作为错误处理器。"""
        if not isinstance(error_handler_runnable, Runnable):
            raise TypeError("Error handler must be a Runnable instance.")
        self._error_handler = error_handler_runnable
        # logger.debug(f"Runnable '{self.name}': Error handler set to '{error_handler_runnable.name}'.")
        return self

    def retry(self, max_attempts: int = 3, delay_seconds: Union[int, float] = 1, retry_on_exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = (Exception,)):
        """配置重试逻辑。"""
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
        # logger.debug(f"Runnable '{self.name}': Retry configured (max_attempts={max_attempts}, delay_seconds={delay_seconds}s).")
        return self

    def clear_cache(self, cache_name: str = 'all'):
        if cache_name == '_invoke_cache' or cache_name == 'all':
            self._invoke_cache.clear()
            # logger.debug(f"Runnable '{self.name}': Invoke cache cleared.")
        if cache_name == '_check_cache' or cache_name == 'all':
            self._check_cache.clear()
            # logger.debug(f"Runnable '{self.name}': Check cache cleared.")
        return self

    def __or__(self, other: Union['Runnable', Dict[str, 'Runnable']]) -> 'Runnable':
        if isinstance(other, Runnable):
            # Simple A | B
            return Pipeline(self, other, name=f"({self.name} | {other.name})")
        elif isinstance(other, dict) and all(isinstance(r, Runnable) for r in other.values()):
            # A | {"C": C_task, "D": D_task}
            # This implies a BranchAndFanIn immediately after self.
            # So, self -> BranchAndFanIn(other)
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
        # Try deepcopy first for better isolation
        # logger.debug(f"Runnable '{self.name}': Attempting deepcopy...")
        new_runnable = copy.deepcopy(self)
        # logger.debug(f"Runnable '{self.name}': Deepcopy successful. New ID: {id(new_runnable)}")
        
        # Reset caches and potentially other runtime state for the copy
        new_runnable._invoke_cache = {}
        new_runnable._check_cache = {}
        # Name will be reassigned by WorkflowGraph if added as a new node
        # new_runnable.name = f"{self.name}_copy_{id(new_runnable)}" # Temporary unique name
        return new_runnable


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
        # Use func.__name__ as default name, fallback to class name + id
        default_name = getattr(func, '__name__', None)
        if default_name == '<lambda>': # Handle lambda names specifically
             default_name = None # Let base class generate name if it's just <lambda>

        super().__init__(name or default_name, input_declaration=input_declaration, **kwargs)
        
        if not callable(func):
            raise TypeError("func must be a callable")
        self.func = func

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        # logger.debug(f"SimpleTask '{self.name}': _internal_invoke called with input_data type: {type(input_data)}")
        if input_data is NO_INPUT:
            # This case implies that input_declaration (if any) did not resolve to a value,
            # or there was no input_declaration and no direct input.
            # logger.debug(f"SimpleTask '{self.name}': Calling func without arguments (input_data is NO_INPUT).")
            return self.func()
        elif isinstance(input_data, dict):
            # logger.debug(f"SimpleTask '{self.name}': Calling func with kwargs: {list(input_data.keys())}")
            return self.func(**input_data)
        else:
            # logger.debug(f"SimpleTask '{self.name}': Calling func with positional arg: {str(input_data)[:60]}")
            return self.func(input_data)


class Pipeline(Runnable):
    def __init__(self, first: Runnable, second: Runnable, name: Optional[str] = None, **kwargs):
        # Ensure unique name if not provided
        effective_name = name or f"Pipeline[{first.name}_then_{second.name}]"
        super().__init__(name=effective_name, **kwargs) # Pass kwargs to base for cache_key_generator etc.
        
        if not isinstance(first, Runnable) or not isinstance(second, Runnable):
            raise TypeError("Both 'first' and 'second' must be Runnable instances.")
        self.first = first
        self.second = second
        # Pipeline's input_declaration could be inherited from `first` if not specified for Pipeline itself.
        if self.input_declaration is None and first.input_declaration is not None:
            self.input_declaration = first.input_declaration


    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        # logger.debug(f"DEBUG: Pipeline '{self.name}': Invoking 'first' ({self.first.name}). Input type: {type(input_data).__name__}. Context ID: {id(context)}") # Debug print
        output_first = self.first.invoke(input_data, context)
        
        # logger.debug(f"DEBUG: Pipeline '{self.name}': 'first' ({self.first.name}) output type: {type(output_first).__name__}. Invoking 'second' ({self.second.name}). Context ID: {id(context)}") # Debug print
        output_second = self.second.invoke(output_first, context)
        # logger.debug(f"DEBUG: Pipeline '{self.name}': 'second' ({self.second.name}) output type: {type(output_second).__name__}. Context ID: {id(context)}") # Debug print
        return output_second

    def _default_check(self, data_from_invoke: Any) -> bool:
        # data_from_invoke is the result of self.second.invoke
        # The check should be performed on the context of self.second
        return self.second.check(data_from_invoke) # Pass context if check needs it

    def set_check(self, func: Callable[[Any], bool]):
        # logger.warning(f"Pipeline '{self.name}': Setting custom check. Default behavior uses check of the last element ('{self.second.name}').")
        return super().set_check(func)


class Conditional(Runnable):
    def __init__(self, condition_r: Runnable, true_r: Runnable, false_r: Runnable, name: Optional[str] = None, **kwargs):
        super().__init__(name or f"Cond[{condition_r.name}?{true_r.name}:{false_r.name}]", **kwargs)
        self.condition_r = condition_r
        self.true_r = true_r
        self.false_r = false_r

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        # logger.debug(f"DEBUG: Conditional '{self.name}': Evaluating condition '{self.condition_r.name}'. Input type: {type(input_data).__name__}. Context ID: {id(context)}") # Debug print
        condition_output = self.condition_r.invoke(input_data, context)
        
        # logger.debug(f"DEBUG: Conditional '{self.name}': Condition '{self.condition_r.name}' output type: {type(condition_output).__name__}. Checking condition. Context ID: {id(context)}") # Debug print
        context.log_event(f"Node '{self.name}': Condition output type: {type(condition_output).__name__}. Checking condition.")
        if self.condition_r.check(condition_output, context): # Pass context to check
            # logger.info(f"Conditional '{self.name}': Condition is TRUE. Executing true_branch '{self.true_r.name}'.")
            context.log_event(f"Node '{self.name}': Condition TRUE, executing '{self.true_r.name}'.")
            # True branch receives the output of the condition runnable
            return self.true_r.invoke(condition_output, context)
        else:
            # logger.info(f"Conditional '{self.name}': Condition is FALSE. Executing false_branch '{self.false_r.name}'.")
            context.log_event(f"Node '{self.name}': Condition FALSE, executing '{self.false_r.name}'.")
            # False branch also receives the output of the condition runnable
            return self.false_r.invoke(condition_output, context)


class BranchAndFanIn(Runnable):
    """
    Implements the {"C": C_task, "D": D_task} part of A | {"C": C, "D": D} | F.
    Receives a single input, fans it out to multiple parallel tasks,
    and aggregates their results into a dictionary.
    """
    def __init__(self, tasks_dict: Dict[str, Runnable], name: Optional[str] = None, max_workers: Optional[int] = None, **kwargs):
        super().__init__(name or f"BranchFanIn_{'_'.join(tasks_dict.keys())}", **kwargs)
        if not isinstance(tasks_dict, dict) or not all(isinstance(r, Runnable) for r in tasks_dict.values()):
            raise TypeError("tasks_dict must be a dictionary of Runnables.")
        self.tasks_dict = tasks_dict
        self.max_workers = max_workers

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        # logger.info(f"BranchAndFanIn '{self.name}': Starting parallel execution for {len(self.tasks_dict)} tasks. Input type: {type(input_data)}")
        context.log_event(f"Node '{self.name}': Starting parallel execution for {len(self.tasks_dict)} tasks.")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_key = {
                executor.submit(task.invoke, input_data, context): key
                for key, task in self.tasks_dict.items()
            }
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                task_name = self.tasks_dict[key].name
                try:
                    result = future.result()
                    results[key] = result
                    # logger.debug(f"BranchAndFanIn '{self.name}': Task '{key}' ({task_name}) completed. Result type: {type(result)}")
                    context.log_event(f"Node '{self.name}', Branch '{key}' ({task_name}): Completed.")
                    # Optionally, add individual branch outputs to context with prefixed names
                    # context.add_output(f"{self.name}_{key}", result)
                except Exception as e:
                    # logger.error(f"BranchAndFanIn '{self.name}': Task '{key}' ({task_name}) failed: {type(e).__name__}: {e}", exc_info=logger.level <= logging.DEBUG)
                    context.log_event(f"Node '{self.name}', Branch '{key}' ({task_name}): Failed: {type(e).__name__}: {e}")
                    # How to handle partial failures?
                    # Option 1: Re-raise immediately.
                    # Option 2: Collect all results/exceptions and decide.
                    # Option 3: Let individual tasks' error handlers/retry logic work. If they fail through, then this re-raises.
                    raise e # For now, re-raise the first exception encountered.

        # logger.info(f"BranchAndFanIn '{self.name}': All parallel tasks completed. Aggregated results: {list(results.keys())}")
        context.log_event(f"Node '{self.name}': All parallel tasks completed.")
        return results


class SourceParallel(Runnable):
    """
    Implements the {"A": A_chain, "B": B_chain} part of {"A": A_chain, "B": B_chain} | H.
    Receives an initial input (or uses context's initial_input if NO_INPUT),
    fans it out to multiple parallel task chains, and aggregates their results.
    """
    def __init__(self, tasks_dict: Dict[str, Runnable], name: Optional[str] = None, max_workers: Optional[int] = None, **kwargs):
        super().__init__(name or f"SourceParallel_{'_'.join(tasks_dict.keys())}", **kwargs)
        if not isinstance(tasks_dict, dict) or not all(isinstance(r, Runnable) for r in tasks_dict.values()):
            raise TypeError("tasks_dict must be a dictionary of Runnables.")
        self.tasks_dict = tasks_dict
        self.max_workers = max_workers

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Dict[str, Any]:
        actual_input = input_data if input_data is not NO_INPUT else context.initial_input
        context.log_event(f"Node '{self.name}': Starting parallel execution for {len(self.tasks_dict)} tasks. Input type: {type(actual_input).__name__}")

        aggregated_results: Dict[str, Any] = {}
        future_to_branch_info: Dict[Any, Tuple[str, Runnable, ExecutionContext]] = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for key, task_runnable in self.tasks_dict.items():
                # Create a new, isolated context for each branch.
                branch_context = ExecutionContext(initial_input=actual_input, parent_context=context)
                future = executor.submit(task_runnable.invoke, actual_input, branch_context)
                future_to_branch_info[future] = (key, task_runnable, branch_context)

            for future in as_completed(future_to_branch_info):
                key, task_runnable, branch_ctx_for_this_branch = future_to_branch_info[future]
                task_name = task_runnable.name
                try:
                    branch_output_value = future.result()
                    aggregated_results[key] = branch_output_value
                    context.log_event(f"Node '{self.name}', Branch '{key}' ({task_name}): Completed. Result type: {type(branch_output_value).__name__}.")
                except Exception as e:
                    context.log_event(f"Node '{self.name}', Branch '{key}' ({task_name}): FAILED with {type(e).__name__}: {str(e)[:100]}.")
                    raise
        
        # After all branches are completed, add individual branch outputs to the main context
        # using parentName_branchKey naming convention.
        for key_of_branch, task_in_branch in self.tasks_dict.items():
            if key_of_branch in aggregated_results: # Check if this branch actually produced a result
                branch_output_value = aggregated_results[key_of_branch]
                context_key_for_branch_output = f"{self.name}_{key_of_branch}"
                context.add_output(context_key_for_branch_output, branch_output_value)
                context.log_event(f"Node '{self.name}': Added output for branch '{key_of_branch}' as '{context_key_for_branch_output}' to main context.")

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
        current_input_for_iteration = input_data # Input for the first condition check AND the first body run
        all_body_outputs = []

        # logger.info(f"While '{self.name}': Starting loop. Max loops: {self.max_loops}. Initial input type: {type(current_input_for_iteration).__name__}")
        context.log_event(f"Node '{self.name}': Starting loop (max_loops={self.max_loops}).")

        while loop_count < self.max_loops:
            # logger.debug(f"While '{self.name}': Loop {loop_count + 1}. Evaluating condition '{self.condition_check_runnable.name}'. Input type: {type(current_input_for_iteration).__name__}")
            context.log_event(f"Node '{self.name}': Loop {loop_count + 1}/{self.max_loops}. Evaluating condition '{self.condition_check_runnable.name}'. Input type: {type(current_input_for_iteration).__name__}")
            
            # Condition check runnable receives the input for the current iteration
            condition_output = self.condition_check_runnable.invoke(current_input_for_iteration, context)
            
            # logger.debug(f"While '{self.name}': Loop {loop_count + 1}. Condition '{self.condition_check_runnable.name}' output type: {type(condition_output).__name__}. Checking condition.")
            context.log_event(f"Node '{self.name}': Loop {loop_count + 1}. Condition output type: {type(condition_output).__name__}. Checking condition.")
            if not self.condition_check_runnable.check(condition_output, context):
                # logger.info(f"While '{self.name}': Condition is FALSE. Exiting loop after {loop_count} iterations.")
                context.log_event(f"Node '{self.name}': Condition FALSE. Exiting loop after {loop_count} iterations.")
                break
            
            # logger.debug(f"While '{self.name}': Condition is TRUE. Executing body '{self.body_runnable.name}'. Input type: {type(current_input_for_iteration).__name__}")
            context.log_event(f"Node '{self.name}': Loop {loop_count + 1}, Condition TRUE, executing body '{self.body_runnable.name}'. Body input type: {type(current_input_for_iteration).__name__}")
            
            # The body's input is the input for the current iteration
            body_output = self.body_runnable.invoke(current_input_for_iteration, context)
            all_body_outputs.append(body_output)
            
            # The output of the body becomes the input for the next iteration
            current_input_for_iteration = body_output # Updates input for the next iteration
            loop_count += 1
        else: # Executed if the loop finished due to max_loops, not break
            # logger.debug(f"DEBUG: While '{self.name}': Max loops reached block entered.") # Debug print
            context.log_event(f"Node '{self.name}': Loop exited due to max_loops ({self.max_loops}) reached.")

        # What to return? Last body_output? List of all body_outputs?
        # Design choice: return list of all outputs from the body in each iteration.
        # logger.info(f"While '{self.name}': Loop finished. Returning {len(all_body_outputs)} outputs.")
        context.log_event(f"Node '{self.name}': Loop finished. Returning {len(all_body_outputs)} outputs.")
        return all_body_outputs


class MergeInputs(Runnable):
    """
    Aggregates multiple inputs from the context based on a mapping,
    and then calls a merge_function with these inputs as keyword arguments.
    """
    def __init__(self, input_sources: Dict[str, str], merge_function: Callable[..., Any], name: Optional[str] = None, **kwargs):
        # input_sources: {"param_name_for_func": "node_name_in_context", ...}
        super().__init__(name or f"MergeInputs_{getattr(merge_function, '__name__', 'custom')}", **kwargs)
        self.input_sources = input_sources
        self.merge_function = merge_function
        # This Runnable inherently declares its inputs via input_sources.
        # We can set self.input_declaration here for consistency, though it's somewhat redundant.
        self.input_declaration = self.input_sources # Or a transformation of it if needed by base class

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        # input_data to MergeInputs is typically NO_INPUT, as it fetches from context.
        # If input_data is provided, it's currently ignored by this implementation.
        
        kwargs_for_func = {}
        # logger.debug(f"MergeInputs '{self.name}': Fetching inputs from context based on sources: {self.input_sources}")
        context.log_event(f"Node '{self.name}': Fetching inputs from context: {list(self.input_sources.values())}")

        for param_name, source_node_name in self.input_sources.items():
            value = context.get_output(source_node_name)
            # logger.debug(f"MergeInputs '{self.name}': Fetched '{source_node_name}' -> '{param_name}': {str(value)[:60]}")
            # Decide on behavior if value is None or not found. Passing None for now.
            kwargs_for_func[param_name] = value
        
        # logger.info(f"MergeInputs '{self.name}': Calling merge_function '{getattr(self.merge_function, '__name__', 'custom')}' with keys: {list(kwargs_for_func.keys())}")
        context.log_event(f"Node '{self.name}': Calling merge_function '{getattr(self.merge_function, '__name__', 'custom')}'.")
        return self.merge_function(**kwargs_for_func)

# WorkflowGraph and CompiledGraph will be substantial and are best placed in a separate file
# or added later in this file. For now, this covers the initial set of Runnables.