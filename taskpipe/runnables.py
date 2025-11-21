import abc
import time
import logging
import hashlib
import pickle
import asyncio # Added for invoke_async
import inspect
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    Coroutine,
    cast,
    get_origin,
    get_type_hints,
)

try:
    from typing import Protocol, runtime_checkable
except ImportError:
    from typing_extensions import Protocol, runtime_checkable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pydantic import BaseModel, ValidationError, create_model

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

    def remove_output(self, node_name: str) -> None: ...

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
        self.node_status_events: List[Dict[str, Any]] = []

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

    def remove_output(self, node_name: str) -> None:
        """从上下文中移除指定节点的输出（用于垃圾回收）。"""
        if node_name in self.node_outputs:
            del self.node_outputs[node_name]
            # 可选：记录调试日志，这里暂时注释掉以保持日志清洁
            # logger.debug(f"GC: Output for node '{node_name}' removed from context.")

    def log_event(self, message: str):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        full_message = f"[{timestamp}] {message}"
        logger.debug(f"ContextEvent: {message}")
        self.event_log.append(full_message)

    def notify_status(self, node_name: str, status: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        event_payload = {
            "node": node_name,
            "status": status,
            "metadata": metadata or {},
            "timestamp": time.time()
        }
        self.node_status_events.append(event_payload)

    def __repr__(self) -> str:
        return f"<InMemoryExecutionContext id={id(self)} initial_input={str(self.initial_input)[:60]}, node_outputs_keys={list(self.node_outputs.keys())}>"

def _default_cache_key_generator(runnable_name: str, input_data: Any, context: Optional[ExecutionContext], bindings_signature: Any) -> Any:
    """
    默认的缓存键生成器。
    尝试序列化输入数据和输入绑定（map_inputs/static_inputs）相关信息。
    """
    try:
        input_data_bytes = pickle.dumps(input_data)
        bindings_bytes = pickle.dumps(bindings_signature)

        hasher = hashlib.md5()
        hasher.update(input_data_bytes)
        hasher.update(bindings_bytes)
        hasher.update(runnable_name.encode('utf-8'))
        return hasher.hexdigest()
    except Exception as e:
        return object()


ConfigModelType = TypeVar("ConfigModelType", bound=BaseModel)
InputModelType = TypeVar("InputModelType", bound=BaseModel)
OutputModelType = TypeVar("OutputModelType", bound=BaseModel)


@dataclass(frozen=True)
class _OutputFieldRef:
    source: 'Runnable'
    field_name: str


@dataclass
class _InputBinding:
    target_field: str
    source: Optional['Runnable'] = None
    source_field: Optional[str] = None
    static_value: Any = None

    def is_dynamic(self) -> bool:
        return self.source is not None and self.source_field is not None

    def is_static(self) -> bool:
        return self.source is None


class _OutputAccessor:
    def __init__(self, runnable: 'Runnable'):
        self._runnable = runnable

    def __getattr__(self, item: str) -> _OutputFieldRef:
        return _OutputFieldRef(source=self._runnable, field_name=item)


class Runnable(abc.ABC):
    """
    系统的基本构建块。每个 Runnable 代表工作流中的一个操作或步骤。
    """
    InputModel: Optional[Type[InputModelType]] = None
    OutputModel: Optional[Type[OutputModelType]] = None
    ConfigModel: Optional[Type[ConfigModelType]] = None

    def __init__(self,
                 name: Optional[str] = None,
                 cache_key_generator: Optional[Callable[[str, Any, Optional[ExecutionContext], Any], Any]] = None,
                 use_cache: bool = False,
                 config: Optional[Union[BaseModel, Dict[str, Any]]] = None): # <<< NEW PARAMETER, defaulting to False
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
        self._cache_key_generator = cache_key_generator or _default_cache_key_generator

        self._on_start_handler: Optional[Union['Runnable', Callable[[ExecutionContext, Any], None]]] = None
        self._on_complete_handler: Optional[Union['Runnable', Callable[[ExecutionContext, Any, Optional[Exception]], None]]] = None
        self.config = self._initialize_config(config)
        self._input_bindings: Dict[str, _InputBinding] = {}
        self._input_model_cls: Type[BaseModel] = self._resolve_model_cls("InputModel")
        self._output_model_cls: Type[BaseModel] = self._resolve_model_cls("OutputModel")

    def _resolve_model_cls(self, attr_name: str) -> Type[BaseModel]:
        model_cls = getattr(self, attr_name, None)
        if model_cls is None:
            raise TypeError(f"{self.__class__.__name__} 必须定义 {attr_name}.")
        if not inspect.isclass(model_cls) or not issubclass(model_cls, BaseModel):
            raise TypeError(f"{self.__class__.__name__}.{attr_name} 必须继承 BaseModel.")
        return model_cls

    @property
    def Output(self) -> _OutputAccessor:
        return _OutputAccessor(self)

    def map_inputs(self, **mappings: Any) -> 'Runnable':
        clone = self.copy()
        updated = dict(clone._input_bindings)
        for target_field, value in mappings.items():
            if target_field not in getattr(clone._input_model_cls, "model_fields", {}):
                raise ValueError(f"{clone.name}: InputModel 不包含字段 '{target_field}'")
            if isinstance(value, _OutputFieldRef):
                source_fields = getattr(value.source._output_model_cls, "model_fields", {})
                if value.field_name not in source_fields:
                    raise ValueError(
                        f"{clone.name}: {value.source.name}.OutputModel 缺少字段 '{value.field_name}'."
                    )
                updated[target_field] = _InputBinding(
                    target_field=target_field,
                    source=value.source,
                    source_field=value.field_name,
                )
            else:
                updated[target_field] = _InputBinding(
                    target_field=target_field,
                    static_value=value,
                )
        clone._input_bindings = updated
        return clone
    def _initialize_config(self, config_data: Optional[Union[BaseModel, Dict[str, Any]]]) -> Any:
        config_cls = getattr(self, "ConfigModel", None)
        if config_cls is None:
            return config_data
        if config_data is None:
            return config_cls()
        if isinstance(config_data, config_cls):
            return config_data
        if isinstance(config_data, dict):
            try:
                return config_cls(**config_data)
            except ValidationError as exc:
                raise ValueError(f"Runnable '{self.name}' config validation failed: {exc}") from exc
        raise TypeError(f"Config for Runnable '{self.name}' must be dict or {config_cls.__name__}")

    def get_config(self) -> Any:
        return self.config

    def get_config_dict(self) -> Optional[Dict[str, Any]]:
        if isinstance(self.config, BaseModel):
            return cast(BaseModel, self.config).model_dump()
        if isinstance(self.config, dict):
            return dict(self.config)
        return None


    def describe_input_schema(self) -> Dict[str, str]:
        fields = getattr(self._input_model_cls, "model_fields", {})
        return {
            field_name: self._type_name(field.annotation)
            for field_name, field in fields.items()
        }

    def describe_output_schema(self) -> Dict[str, str]:
        fields = getattr(self._output_model_cls, "model_fields", {})
        return {
            field_name: self._type_name(field.annotation)
            for field_name, field in fields.items()
        }

    @staticmethod
    def _type_name(tp: Any) -> str:
        if tp is None:
            return "None"
        origin = get_origin(tp)
        if origin is not None:
            return str(tp)
        try:
            return tp.__name__
        except AttributeError:
            return str(tp)

    def _normalize_direct_input(self, direct_input: Any) -> Dict[str, Any]:
        if direct_input is NO_INPUT or direct_input is None:
            return {}
        if isinstance(direct_input, BaseModel):
            # 如果是 Pydantic 模型，转换为字典
            input_dict = direct_input.model_dump()
            # 如果当前任务的 InputModel 只有一个字段，且输入模型也只有一个字段，尝试自动映射
            input_fields = list(getattr(self._input_model_cls, "model_fields", {}).keys())
            if len(input_fields) == 1 and len(input_dict) == 1:
                # 单字段自动映射：将输入的值映射到目标字段
                input_value = list(input_dict.values())[0]
                return {input_fields[0]: input_value}
            return input_dict
        if isinstance(direct_input, dict):
            # 如果输入是字典，且当前任务的 InputModel 只有一个字段，尝试自动映射
            input_fields = list(getattr(self._input_model_cls, "model_fields", {}).keys())
            if len(input_fields) == 1 and len(direct_input) == 1:
                # 单字段自动映射：将输入的值映射到目标字段
                input_value = list(direct_input.values())[0]
                return {input_fields[0]: input_value}
            return dict(direct_input)
        model_fields = list(getattr(self._input_model_cls, "model_fields", {}).keys())
        if len(model_fields) == 1:
            return {model_fields[0]: direct_input}
        raise ValueError(
            f"{self.name}: 无法将标量输入映射到多字段 InputModel。请使用 map_inputs 或传入字典。"
        )

    def _resolve_inputs_from_bindings(
        self,
        context: Optional[ExecutionContext],
    ) -> Dict[str, Any]:
        resolved: Dict[str, Any] = {}
        for target_field, binding in self._input_bindings.items():
            if binding.is_dynamic():
                if not context or not binding.source:
                    raise RuntimeError(
                        f"{self.name}: 缺少 ExecutionContext，无法解析映射输入 '{target_field}'."
                    )
                parent_output = context.get_output(binding.source.name)
                parent_dict = self._model_to_plain(parent_output)
                if binding.source_field not in parent_dict:
                    raise KeyError(
                        f"{self.name}: 上游 {binding.source.name} 输出中不存在字段 '{binding.source_field}'."
                    )
                resolved[target_field] = parent_dict[binding.source_field]
            elif binding.is_static():
                resolved[target_field] = binding.static_value
        return resolved

    def _dynamic_mapping_for_source(self, source: 'Runnable') -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for target_field, binding in self._input_bindings.items():
            if binding.is_dynamic() and binding.source is source:
                mapping[target_field] = binding.source_field
        return mapping

    def _static_binding_values(self) -> Dict[str, Any]:
        static_values: Dict[str, Any] = {}
        for target_field, binding in self._input_bindings.items():
            if binding.is_static():
                static_values[target_field] = binding.static_value
        return static_values

    @staticmethod
    def _model_to_plain(payload: Any) -> Dict[str, Any]:
        if payload is None or payload is NO_INPUT:
            return {}
        if isinstance(payload, BaseModel):
            return payload.model_dump()
        if isinstance(payload, dict):
            return dict(payload)
        return {"value": payload}

    def _prepare_input_payload(self, direct_input: Any, effective_context: Optional[ExecutionContext]) -> Any:
        payload = self._resolve_inputs_from_bindings(effective_context)
        payload.update(self._normalize_direct_input(direct_input))
        if not payload:
            return NO_INPUT
        return payload

    @staticmethod
    def _unwrap_input_payload(payload: Any) -> Any:
        if isinstance(payload, dict):
            if not payload:
                return NO_INPUT
            if set(payload.keys()) == {"_input"}:
                return payload["_input"]
        return payload

    def _get_cache_key(self, input_data: Any, context: Optional[ExecutionContext]) -> Any:
        binding_signature = tuple(
            sorted(
                (
                    target,
                    (
                        binding.source.name if binding.source else None,
                        binding.source_field,
                        binding.static_value,
                    ),
                )
                for target, binding in self._input_bindings.items()
            )
        )
        return self._cache_key_generator(self.name, input_data, context, binding_signature)

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

    def _apply_input_model(self, payload: Any) -> Any:
        input_model_cls = getattr(self, "InputModel", None)
        if input_model_cls is None:
            return payload
        try:
            if payload is NO_INPUT:
                payload = {}
            if isinstance(payload, input_model_cls):
                return payload
            if isinstance(payload, dict):
                return input_model_cls(**payload)
            return input_model_cls.model_validate(payload)
        except ValidationError as exc:
            raise ValueError(f"Runnable '{self.name}' input validation failed: {exc}") from exc

    def _apply_output_model(self, result: Any) -> Any:
        output_model_cls = getattr(self, "OutputModel", None)
        if output_model_cls is None:
            return result
        
        # 对于 Conditional 和 AsyncConditional，如果结果已经是 BaseModel 且字段兼容，直接返回
        # 这样可以避免类型不匹配的问题（true_r 和 false_r 可能有不同的模型类型但字段相同）
        from .async_runnables import AsyncConditional
        if (isinstance(self, (Conditional, AsyncConditional)) and isinstance(result, BaseModel)):
            result_fields = set(getattr(type(result), "model_fields", {}).keys())
            expected_fields = set(getattr(output_model_cls, "model_fields", {}).keys())
            if result_fields == expected_fields:
                return result
        
        try:
            if isinstance(result, output_model_cls):
                return result
            if isinstance(result, dict):
                return output_model_cls(**result)
            return output_model_cls.model_validate(result)
        except ValidationError as exc:
            raise ValueError(f"Runnable '{self.name}' output validation failed: {exc}") from exc

    def _emit_node_status(self,
                          context: Optional[ExecutionContext],
                          status: str,
                          metadata: Optional[Dict[str, Any]] = None) -> None:
        if not context:
            return
        notifier = getattr(context, "notify_status", None)
        if callable(notifier):
            try:
                notifier(self.name, status, metadata or {})
            except Exception:  # pragma: no cover - never block execution due to observer failure
                logger.debug("notify_status failed for node '%s'", self.name, exc_info=True)

    def invoke(self, input_data: Any = NO_INPUT, context: Optional[ExecutionContext] = None) -> Any:
        effective_context = context if context is not None else InMemoryExecutionContext(initial_input=input_data if input_data is not NO_INPUT else None)
        prepared_input_for_invoke = self._prepare_input_payload(input_data, effective_context)

        task_final_result: Any = NO_INPUT
        task_final_exception: Optional[Exception] = None

        self._emit_node_status(effective_context, "start")

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
                        execution_input_payload = self._apply_input_model(prepared_input_for_invoke)
                        result_internal = self._internal_invoke(execution_input_payload, effective_context)
                        task_final_result = self._apply_output_model(result_internal)
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
                                    
                                    # 错误处理器的输出可能使用不同的模型，尝试应用输出模型，如果失败则直接使用
                                    try:
                                        task_final_result = self._apply_output_model(error_handler_output)
                                    except (ValueError, ValidationError):
                                        # 如果错误处理器的输出模型不匹配，直接使用原始输出
                                        effective_context.log_event(f"Node '{self.name}': Error handler output model mismatch, using raw output.")
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
            self._emit_node_status(effective_context, "failed", {"exception": type(task_final_exception).__name__})
            raise task_final_exception
        
        self._emit_node_status(effective_context, "success")
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
        # Check if other is AsyncRunnable - if so, create AsyncPipeline
        from .async_runnables import AsyncRunnable, AsyncPipeline
        if isinstance(other, Runnable):
            # If either self or other is AsyncRunnable, use AsyncPipeline
            if isinstance(self, AsyncRunnable) or isinstance(other, AsyncRunnable):
                return AsyncPipeline(self, other, name=f"({self.name} | {other.name})")
            return Pipeline(self, other, name=f"({self.name} | {other.name})")
        elif isinstance(other, dict) and all(isinstance(r, Runnable) for r in other.values()):
            # Check if any runnable in dict is AsyncRunnable
            has_async = isinstance(self, AsyncRunnable) or any(isinstance(r, AsyncRunnable) for r in other.values())
            if has_async:
                from .async_runnables import AsyncBranchAndFanIn
                branch_fan_in = AsyncBranchAndFanIn(other, name=f"BranchFanIn_after_{self.name}")
                return AsyncPipeline(self, branch_fan_in, name=f"({self.name} | {branch_fan_in.name})")
            else:
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
            helper.ensure_static_inputs(graph, entry)
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


def _build_input_model_from_callable(func: Callable, task_name: str) -> Type[BaseModel]:
    signature = inspect.signature(func)
    hints = get_type_hints(func)
    field_defs: Dict[str, Tuple[Any, Any]] = {}
    for param in signature.parameters.values():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            if param.name != "context":
                raise TypeError(
                    f"@task 函数 '{func.__name__}' 不能使用 *args/**kwargs（除非参数名为 context）。"
                )
            continue
        if param.name == "context":
            continue
        annotation = hints.get(param.name, Any)
        default = param.default if param.default is not inspect._empty else ...
        field_defs[param.name] = (annotation, default)

    model_name = f"{task_name}Input"
    if not field_defs:
        return create_model(model_name)
    return create_model(model_name, **field_defs)


def _build_output_model_from_callable(func: Callable, task_name: str) -> Type[BaseModel]:
    hints = get_type_hints(func)
    return_hint = hints.get("return", Any)

    if inspect.isclass(return_hint) and issubclass(return_hint, BaseModel):
        return return_hint

    annotations = getattr(return_hint, "__annotations__", None)
    if isinstance(annotations, dict) and annotations:
        fields = {
            field_name: (field_type, ...)
            for field_name, field_type in annotations.items()
        }
        return create_model(f"{task_name}Output", **fields)

    return create_model(f"{task_name}Output", result=(return_hint, ...))


def task(func: Optional[Callable] = None, *, name: Optional[str] = None):
    """
    将普通函数转换为带强类型契约的 Runnable 子类。
    支持同步函数 (生成 Runnable) 和异步函数 (生成 AsyncRunnable)。
    """

    def decorator(callable_obj: Callable) -> Type[Runnable]:
        if not callable(callable_obj):
            raise TypeError("@task 只能装饰可调用对象。")

        task_name = name or getattr(callable_obj, "__name__", "Task")
        input_model = _build_input_model_from_callable(callable_obj, task_name)
        output_model = _build_output_model_from_callable(callable_obj, task_name)
        signature = inspect.signature(callable_obj)
        expects_context = "context" in signature.parameters

        # 检测是否为异步函数
        is_async = inspect.iscoroutinefunction(callable_obj)

        # 动态选择基类
        if is_async:
            # 局部导入以避免循环依赖 (runnables <-> async_runnables)
            from .async_runnables import AsyncRunnable
            BaseClass = AsyncRunnable
        else:
            BaseClass = Runnable

        class GeneratedTask(BaseClass):
            InputModel = input_model
            OutputModel = output_model

            def __init__(self, **kwargs):
                runtime_name = kwargs.pop("name", task_name)
                super().__init__(name=runtime_name, **kwargs)

            def _prepare_payload(self, input_data: Any, context: ExecutionContext) -> dict:
                """辅助方法：准备调用参数"""
                payload = input_data.model_dump() if isinstance(input_data, BaseModel) else dict(input_data or {})
                if expects_context:
                    payload["context"] = context
                return payload

            def _process_result(self, result: Any) -> Any:
                """辅助方法：处理返回值"""
                if isinstance(result, BaseModel):
                    return result
                if isinstance(result, dict):
                    return result
                output_fields = list(getattr(self.OutputModel, "model_fields", {}).keys())
                if len(output_fields) == 1:
                    return {output_fields[0]: result}
                return result

            # 根据是否为异步函数，有条件地定义执行方法
            if is_async:
                async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Any:
                    payload = self._prepare_payload(input_data, context)
                    result = await callable_obj(**payload)
                    return self._process_result(result)
            else:
                def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
                    payload = self._prepare_payload(input_data, context)
                    result = callable_obj(**payload)
                    return self._process_result(result)

        GeneratedTask.__name__ = task_name
        GeneratedTask.__doc__ = getattr(callable_obj, "__doc__", None)
        GeneratedTask.__module__ = callable_obj.__module__
        return GeneratedTask

    if func is not None:
        return decorator(func)
    return decorator


class Pipeline(Runnable):
    def __init__(self, first: Runnable, second: Runnable, name: Optional[str] = None, **kwargs):
        effective_name = name or f"Pipeline[{first.name}_then_{second.name}]"
        if not isinstance(first, Runnable) or not isinstance(second, Runnable):
            raise TypeError("Both 'first' and 'second' must be Runnable instances.")
        self.first = first
        self.second = second
        self.InputModel = first._input_model_cls
        self.OutputModel = second._output_model_cls
        super().__init__(name=effective_name, **kwargs)


    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        output_first = self.first.invoke(input_data, context)
        # Pipeline 中，second 应该接收 first 的输出
        # 如果 second 有 map_inputs 配置，会通过 context 自动处理
        # 否则，直接传递 output_first，让 second 的 _prepare_input_payload 处理
        output_second = self.second.invoke(output_first, context)
        return output_second

    def _default_check(self, data_from_invoke: Any) -> bool:
        return self.second.check(data_from_invoke) 

    def _expand_to_graph(self, graph: 'WorkflowGraph', helper: '_GraphExportHelper') -> Tuple[List[str], List[str]]:
        entry_first, exit_first = self.first._expand_to_graph(graph, helper)
        entry_second, exit_second = self.second._expand_to_graph(graph, helper)
        for parent in exit_first:
            for child in entry_second:
                helper.connect_by_name(graph, parent, child)
        return entry_first, exit_second


class _CheckAdapterRunnable(Runnable):
    """
    Auto-generated adapter task that executes a Runnable's check() method
    and returns a result compatible with a Router ({"condition": bool}).
    Used during graph expansion for Conditional nodes.
    """
    def __init__(self, target_runnable: Runnable, name: Optional[str] = None):
        self.target = target_runnable
        adapter_name = name or f"{target_runnable.name}_CheckAdapter"
        # Inherit InputModel from target's OutputModel to maintain graph continuity visual semantics
        # This allows the graph visualizer to see matching types.
        # Must set as class attribute before calling super().__init__()
        self.__class__.InputModel = target_runnable._output_model_cls
        # Explicit OutputModel for Router compatibility
        self.__class__.OutputModel = create_model(f"{adapter_name}Output", condition=(bool, ...))
        super().__init__(name=adapter_name)

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Dict[str, bool]:
        # input_data here is already processed by _apply_input_model.
        # If self.InputModel matches target's OutputModel, input_data is a valid result from target.
        # We pass this result to target.check()
        # Extract the actual value from the model if it's a BaseModel
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
        result = self.target.check(check_value, context)
        return {"condition": result}


class Conditional(Runnable):
    def __init__(self, condition_r: Runnable, true_r: Runnable, false_r: Runnable, name: Optional[str] = None, **kwargs):
        self.condition_r = condition_r
        self.true_r = true_r
        self.false_r = false_r
        true_fields = set(getattr(true_r._output_model_cls, "model_fields", {}).keys())
        false_fields = set(getattr(false_r._output_model_cls, "model_fields", {}).keys())
        if true_fields != false_fields:
            raise ValueError("Conditional 的 true/false 分支必须拥有兼容的 OutputModel。")
        self.InputModel = condition_r._input_model_cls
        self.OutputModel = true_r._output_model_cls
        super().__init__(name or f"Cond[{condition_r.name}?{true_r.name}:{false_r.name}]", **kwargs)

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        condition_output = self.condition_r.invoke(input_data, context)
        
        context.log_event(f"Node '{self.name}': Condition output type: {type(condition_output).__name__}. Checking condition.")
        if getattr(self.condition_r, "_custom_async_check_fn", None) is not None:
            raise RuntimeError(
                f"Conditional '{self.name}' detected async check logic on '{self.condition_r.name}'. "
                "Use AsyncConditional to evaluate asynchronous checks."
            )
        # Conditional 中，分支任务应该接收 condition_r 的输出
        # 直接传递 condition_output，让分支任务的 _prepare_input_payload 处理
        if self.condition_r.check(condition_output, context): 
            context.log_event(f"Node '{self.name}': Condition TRUE, executing '{self.true_r.name}'.")
            branch_output = self.true_r.invoke(condition_output, context)
        else:
            context.log_event(f"Node '{self.name}': Condition FALSE, executing '{self.false_r.name}'.")
            branch_output = self.false_r.invoke(condition_output, context)
        
        # Conditional 的输出应该直接返回分支的输出，不进行额外的模型验证
        # 因为我们已经验证了两个分支的 OutputModel 字段兼容
        return branch_output

    def _expand_to_graph(self, graph: 'WorkflowGraph', helper: '_GraphExportHelper') -> Tuple[List[str], List[str]]:
        # 1. Expand Condition node (A)
        entry_cond, exit_cond = self.condition_r._expand_to_graph(graph, helper)

        # 2. Create CheckAdapter node
        # This adapter runs condition_r.check() and outputs {"condition": bool} for the Router
        check_adapter = _CheckAdapterRunnable(self.condition_r)
        adapter_node_name = helper.add_node(graph, check_adapter)

        # Connect A -> Adapter
        for parent in exit_cond:
            helper.connect_by_name(graph, parent, adapter_node_name)

        # 3. Create Router node
        # Ensure Router is available. It is defined later in this file, but at runtime (when to_graph is called), it will be defined.
        # Using a string lookup or deferred import might be safer if Router definition moves, but Router is a core Runnable.
        # Assuming Router is available in the module scope.
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
        # Allows branches to receive original data from A, bypassing the Router/CheckAdapter
        for parent in exit_cond:
            for child in entry_true:
                helper.connect_by_name(graph, parent, child)
            for child in entry_false:
                helper.connect_by_name(graph, parent, child)

        # Return entry points (A's entries) and exit points (union of True/False exits)
        return entry_cond, exit_true + exit_false

class BranchAndFanIn(Runnable):
    def __init__(self, tasks_dict: Dict[str, Runnable], name: Optional[str] = None, max_workers: Optional[int] = None, **kwargs):
        if not isinstance(tasks_dict, dict) or not all(isinstance(r, Runnable) for r in tasks_dict.values()):
            raise TypeError("tasks_dict must be a dictionary of Runnables.")
        self.tasks_dict = tasks_dict
        self.max_workers = max_workers
        branch_names = "_".join(tasks_dict.keys())
        self.InputModel = self._ensure_uniform_input_model()
        self.OutputModel = self._build_output_model(name or f"BranchFanIn_{branch_names}")
        super().__init__(name or f"BranchFanIn_{branch_names}", **kwargs)

    def _ensure_uniform_input_model(self) -> Type[BaseModel]:
        models = {task._input_model_cls for task in self.tasks_dict.values()}
        if len(models) != 1:
            raise ValueError("BranchAndFanIn 所有子任务必须共享相同的 InputModel。")
        return next(iter(models))

    def _build_output_model(self, base_name: str) -> Type[BaseModel]:
        fields = {
            key: (task._output_model_cls, ...)
            for key, task in self.tasks_dict.items()
        }
        return create_model(f"{base_name}Output", **fields)

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        context.log_event(f"Node '{self.name}': Starting parallel execution for {len(self.tasks_dict)} tasks.")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_key = {
                executor.submit(
                    task.invoke,
                    input_data,
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
        if not isinstance(tasks_dict, dict) or not all(isinstance(r, Runnable) for r in tasks_dict.values()):
            raise TypeError("tasks_dict must be a dictionary of Runnables.")
        self.tasks_dict = tasks_dict
        self.max_workers = max_workers
        branch_names = "_".join(tasks_dict.keys())
        self.InputModel = self._ensure_uniform_input_model()
        self.OutputModel = self._build_output_model(name or f"SourceParallel_{branch_names}")
        super().__init__(name or f"SourceParallel_{branch_names}", **kwargs)

    def _ensure_uniform_input_model(self) -> Type[BaseModel]:
        models = {task._input_model_cls for task in self.tasks_dict.values()}
        if len(models) != 1:
            raise ValueError("SourceParallel 所有子任务必须共享相同的 InputModel。")
        return next(iter(models))

    def _build_output_model(self, base_name: str) -> Type[BaseModel]:
        fields = {
            key: (task._output_model_cls, ...)
            for key, task in self.tasks_dict.items()
        }
        return create_model(f"{base_name}Output", **fields)

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Dict[str, Any]:
        context.log_event(
            f"Node '{self.name}': Starting parallel execution for {len(self.tasks_dict)} tasks. "
            f"Input type: {type(input_data).__name__}"
        )

        aggregated_results: Dict[str, Any] = {}
        future_to_branch_key: Dict[Any, str] = {}
        exceptions = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for key, task_runnable in self.tasks_dict.items():
                future = executor.submit(task_runnable.invoke, input_data, context)
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

        context.log_event(f"Node '{self.name}': All branches completed. Aggregated results: {list(aggregated_results.keys())}.")
        return aggregated_results


class While(Runnable):
    def __init__(self, condition_check_runnable: Runnable, body_runnable: Runnable, max_loops: int = 100, name: Optional[str] = None, **kwargs):
        self.condition_check_runnable = condition_check_runnable
        self.body_runnable = body_runnable
        self.max_loops = max_loops
        loop_name = name or f"While[{condition_check_runnable.name}_do_{body_runnable.name}]"
        body_output_cls = body_runnable._output_model_cls
        self.InputModel = condition_check_runnable._input_model_cls
        self.OutputModel = create_model(f"{loop_name}Output", history=(List[body_output_cls], ...))
        super().__init__(loop_name, **kwargs)

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
            
            # 提取 current_input_for_iteration 的实际值
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
            body_output = self.body_runnable.invoke(loop_input_payload, context)
            all_body_outputs.append(body_output)
            
            current_input_for_iteration = body_output 
            loop_count += 1
        else: 
            context.log_event(f"Node '{self.name}': Loop exited due to max_loops ({self.max_loops}) reached.")

        context.log_event(f"Node '{self.name}': Loop finished. Returning {len(all_body_outputs)} outputs.")
        return {"history": all_body_outputs}


class MergeInputs(Runnable):
    """
    兼容旧 API 的胶水节点。input_sources 参数仅用于向后兼容，
    实际数据映射应通过 map_inputs 配置。
    """

    def __init__(self, input_sources: Optional[Dict[str, Any]], merge_function: Callable[..., Any], name: Optional[str] = None, **kwargs):
        task_name = name or f"MergeInputs_{getattr(merge_function, '__name__', 'custom')}"
        self.merge_function = merge_function
        self._legacy_input_sources = input_sources or {}
        self.InputModel = _build_input_model_from_callable(merge_function, task_name)
        self.OutputModel = _build_output_model_from_callable(merge_function, task_name)
        super().__init__(task_name, **kwargs)

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        payload = input_data.model_dump() if isinstance(input_data, BaseModel) else dict(input_data or {})
        context.log_event(f"Node '{self.name}': Calling merge_function '{getattr(self.merge_function, '__name__', 'custom')}'.")
        return self.merge_function(**payload)


class Router(Runnable):
    class InputModel(BaseModel):
        condition: bool

    class OutputModel(BaseModel):
        decision: bool

    def __init__(self, name: Optional[str] = None, **kwargs):
        super().__init__(name or "Router", **kwargs)

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        decision = bool(input_data.condition if isinstance(input_data, BaseModel) else input_data.get("condition"))
        context.log_event(f"Node '{self.name}': Routing based on condition={decision}.")
        return {"decision": decision}


class ScriptRunnable(Runnable):
    class ConfigModel(BaseModel):
        code: str
        input_keys: List[str]
        output_keys: List[str]

    def __init__(self, name: Optional[str] = None, config: Optional[Dict[str, Any]] = None, **kwargs):
        config_obj = config if isinstance(config, ScriptRunnable.ConfigModel) else ScriptRunnable.ConfigModel(**(config or {}))
        task_name = name or "ScriptRunnable"
        self.InputModel = create_model(
            f"{task_name}Input",
            **{key: (Any, ...) for key in config_obj.input_keys}
        ) if config_obj.input_keys else create_model(f"{task_name}Input")
        self.OutputModel = create_model(
            f"{task_name}Output",
            **{key: (Any, ...) for key in config_obj.output_keys}
        ) if config_obj.output_keys else create_model(f"{task_name}Output", result=(Any, ...))
        super().__init__(name=task_name, config=config_obj, **kwargs)

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        payload = input_data.model_dump() if isinstance(input_data, BaseModel) else dict(input_data or {})
        local_vars = {key: payload.get(key) for key in self.config.input_keys}
        local_vars["context"] = context
        exec(self.config.code, {}, local_vars)
        if self.config.output_keys:
            return {key: local_vars.get(key) for key in self.config.output_keys}
        return {"result": local_vars.get("result")}

class _GraphExportHelper:
    def __init__(self):
        self._name_counter: Dict[str, int] = {}
        self._node_name_to_runnable: Dict[str, Runnable] = {}
        self._static_attached: Set[str] = set()

    def _unique_name(self, base_name: Optional[str]) -> str:
        sanitized = base_name or "Node"
        count = self._name_counter.get(sanitized, 0)
        unique = sanitized if count == 0 else f"{sanitized}_{count}"
        self._name_counter[sanitized] = count + 1
        return unique

    def add_node(self, graph: 'WorkflowGraph', runnable: Runnable) -> str:
        node_name = self._unique_name(runnable.name)
        graph.add_node(runnable, node_name=node_name)
        self._node_name_to_runnable[node_name] = runnable
        self._attach_static_inputs(graph, runnable, node_name)
        return node_name

    def connect_by_name(self,
                        graph: 'WorkflowGraph',
                        parent_node_name: str,
                        child_node_name: str,
                        branch: Optional[str] = None) -> None:
        parent_runnable = self._node_name_to_runnable.get(parent_node_name)
        child_runnable = self._node_name_to_runnable.get(child_node_name)
        if parent_runnable is None or child_runnable is None:
            raise ValueError("Graph export helper lost track of runnable references.")

        data_mapping = child_runnable._dynamic_mapping_for_source(parent_runnable)
        if not data_mapping:
            data_mapping = self._infer_auto_mapping(parent_runnable, child_runnable)
        graph.add_edge(parent_node_name, child_node_name, data_mapping=data_mapping, branch=branch)
        self._attach_static_inputs(graph, child_runnable, child_node_name)

    def ensure_static_inputs(self, graph: 'WorkflowGraph', node_name: str) -> None:
        runnable = self._node_name_to_runnable.get(node_name)
        if runnable:
            self._attach_static_inputs(graph, runnable, node_name)

    def _attach_static_inputs(self, graph: 'WorkflowGraph', runnable: Runnable, node_name: str) -> None:
        if node_name in self._static_attached:
            return
        static_values = runnable._static_binding_values()
        if static_values:
            graph.add_edge("__static__", node_name, static_inputs=static_values)
            self._static_attached.add(node_name)

    def _infer_auto_mapping(self, parent: Runnable, child: Runnable) -> Dict[str, str]:
        parent_fields = list(parent._output_model_cls.model_fields.keys())
        child_fields = list(child._input_model_cls.model_fields.keys())
        if len(parent_fields) == 1 and len(child_fields) == 1:
            parent_schema = parent.describe_output_schema()
            child_schema = child.describe_input_schema()
            if parent_schema.get(parent_fields[0]) == child_schema.get(child_fields[0]):
                return {child_fields[0]: parent_fields[0]}
        return {"*": "*"}