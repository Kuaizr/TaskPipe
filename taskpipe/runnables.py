import abc
import time
import logging
import hashlib
import pickle
import asyncio
import inspect
from dataclasses import dataclass, field
from typing import (
    Any, Callable, Dict, List, Optional, Set, Tuple, Type, TypeVar, Union, Coroutine, cast, get_origin, get_type_hints
)
try:
    from typing import Protocol, runtime_checkable
except ImportError:
    from typing_extensions import Protocol, runtime_checkable
from concurrent.futures import ThreadPoolExecutor, as_completed
import pydantic
from pydantic import BaseModel, ValidationError, create_model

NO_INPUT = object()
logger = logging.getLogger(__name__)

# =============================================================================
# Pydantic V1/V2 动态兼容层
# =============================================================================
IS_V1 = pydantic.VERSION.startswith("1.")

def _get_model_fields(model_cls: Type[BaseModel]) -> Dict[str, Any]:
    """获取字段定义，优先 V2，回退 V1"""
    if model_cls is None: return {}
    return getattr(model_cls, "model_fields", getattr(model_cls, "__fields__", {}))

def _model_to_dict(model_obj: BaseModel) -> Dict[str, Any]:
    """模型转字典"""
    if hasattr(model_obj, "model_dump"): return model_obj.model_dump()
    return model_obj.dict()

def _model_validate(model_cls: Type[BaseModel], data: Any) -> BaseModel:
    """数据校验"""
    if hasattr(model_cls, "model_validate"): return model_cls.model_validate(data)
    return model_cls.parse_obj(data)

def _get_field_type(field_info: Any) -> Any:
    """获取字段类型"""
    return getattr(field_info, "annotation", getattr(field_info, "outer_type_", Any))

# =============================================================================
# 异常与工具
# =============================================================================
class SuspendExecution(Exception):
    def __init__(self, snapshot: Any = None):
        self.snapshot = snapshot
        super().__init__("Execution Suspended")

def _deep_get(obj: Any, path: Union[str, Tuple[str, ...]]) -> Any:
    if isinstance(path, str): path = tuple(path.split('.'))
    current = obj
    try:
        for segment in path:
            if current is None: return None
            if isinstance(current, dict): current = current[segment]
            else: current = getattr(current, segment)
        return current
    except (KeyError, AttributeError, TypeError): return None

def _safe_eval(expr: str, context: Dict[str, Any]) -> Any:
    try:
        return eval(expr, {"__builtins__": None}, context)
    except Exception as e:
        logger.error(f"Eval error: {e}")
        return False

# =============================================================================
# 辅助类
# =============================================================================
@dataclass(frozen=True)
class _OutputFieldRef:
    source: 'Runnable'; field_path: Tuple[str, ...] 
    def __getattr__(self, item: str) -> '_OutputFieldRef':
        return _OutputFieldRef(self.source, self.field_path + (item,))

@dataclass
class _InputBinding:
    target_field: str; source: Optional['Runnable'] = None; source_path: Optional[Tuple[str, ...]] = None; static_value: Any = None
    def is_dynamic(self) -> bool: return self.source is not None and self.source_path is not None
    def is_static(self) -> bool: return self.source is None

class _OutputAccessor:
    def __init__(self, runnable: 'Runnable'): self._runnable = runnable
    def __getattr__(self, item: str) -> _OutputFieldRef:
        return _OutputFieldRef(source=self._runnable, field_path=(item,))

# =============================================================================
# Context
# =============================================================================
@runtime_checkable
class ExecutionContext(Protocol):
    initial_input: Any; parent_context: Optional['ExecutionContext']; node_outputs: Dict[str, Any]; event_log: List[str]
    def add_output(self, node_name: str, value: Any) -> None: ...
    def get_output(self, node_name: str, default: Any = None) -> Any: ...
    def remove_output(self, node_name: str) -> None: ...
    def log_event(self, message: str) -> None: ...
    def notify_status(self, node_name: str, status: str, metadata: Optional[Dict[str, Any]] = None) -> None: ...
    def register_status_callback(self, callback: Callable[[str, str, Dict[str, Any]], None]) -> None: ...
    def send_stream(self, node_name: str, chunk: Any) -> None: ...

class InMemoryExecutionContext:
    def __init__(self, initial_input: Any = NO_INPUT, parent_context: Optional[ExecutionContext] = None):
        self.node_outputs: Dict[str, Any] = {}; self.initial_input: Any = initial_input
        self.event_log: List[str] = []; self.parent_context: Optional[ExecutionContext] = parent_context
        self.node_status_events: List[Dict[str, Any]] = []; self._status_callbacks: List[Callable] = []; self._stream_callbacks: List[Callable] = []
    
    def add_output(self, node_name: str, value: Any): self.node_outputs[node_name] = value; self.log_event(f"Output added: {node_name}")
    def get_output(self, node_name: str, default: Any = None) -> Any:
        return self.node_outputs.get(node_name, self.parent_context.get_output(node_name, default) if self.parent_context else default)
    def remove_output(self, node_name: str) -> None: 
        if node_name in self.node_outputs: del self.node_outputs[node_name]
    def log_event(self, message: str): self.event_log.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}")
    def register_status_callback(self, callback: Callable): self._status_callbacks.append(callback)
    def notify_status(self, node_name, status, metadata=None):
        self.node_status_events.append({"node": node_name, "status": status, "ts": time.time()})
        for cb in self._status_callbacks: 
            try: cb(node_name, status, metadata or {})
            except: pass
    def register_stream_callback(self, callback: Callable): self._stream_callbacks.append(callback)
    def send_stream(self, node_name, chunk):
        # 1. 触发当前上下文的回调
        for cb in self._stream_callbacks: 
            try: cb(node_name, chunk)
            except: pass
        # 2. 向上冒泡给父上下文
        if self.parent_context and hasattr(self.parent_context, "send_stream"):
            self.parent_context.send_stream(node_name, chunk)

def _default_cache_key_generator(name, data, ctx, sig):
    try: return hashlib.md5(pickle.dumps(data) + pickle.dumps(sig) + name.encode()).hexdigest()
    except: return object()

ConfigModelType = TypeVar("ConfigModelType", bound=BaseModel)
InputModelType = TypeVar("InputModelType", bound=BaseModel)
OutputModelType = TypeVar("OutputModelType", bound=BaseModel)

# =============================================================================
# Runnable Base
# =============================================================================
class Runnable(abc.ABC):
    InputModel: Optional[Type[InputModelType]] = None
    OutputModel: Optional[Type[OutputModelType]] = None
    ConfigModel: Optional[Type[ConfigModelType]] = None

    def __init__(self, name: Optional[str] = None, cache_key_generator=None, use_cache: bool = False, config: Optional[Union[BaseModel, Dict[str, Any]]] = None):
        self.name = name or f"{self.__class__.__name__}_{id(self)}"
        self.use_cache = use_cache
        self._invoke_cache = {}
        self._error_handler = None
        self._retry_config = None
        self._cache_key_generator = cache_key_generator or _default_cache_key_generator
        self._on_start_handler = None
        self._on_complete_handler = None
        self.config = self._initialize_config(config)
        self._input_bindings = {}
        self._input_model_cls = self._resolve_model_cls("InputModel")
        self._output_model_cls = self._resolve_model_cls("OutputModel")

    def _resolve_model_cls(self, attr_name: str) -> Optional[Type[BaseModel]]:
        model_cls = getattr(self, attr_name, None)
        if model_cls is not None and (inspect.isclass(model_cls) and issubclass(model_cls, BaseModel)):
            return model_cls
        return None

    @property
    def Output(self) -> _OutputAccessor: return _OutputAccessor(self)

    def map_inputs(self, **mappings: Any) -> 'Runnable':
        clone = self.copy()
        updated = dict(clone._input_bindings)
        model_fields = _get_model_fields(clone._input_model_cls)
        
        for target, value in mappings.items():
            if clone._input_model_cls and target not in model_fields:
                raise ValueError(f"{clone.name}: InputModel missing field '{target}'")
            
            if isinstance(value, _OutputFieldRef):
                updated[target] = _InputBinding(target, value.source, value.field_path)
            else:
                updated[target] = _InputBinding(target, static_value=value)
        clone._input_bindings = updated
        return clone

    def _resolve_inputs_from_bindings(self, context: Optional[ExecutionContext]) -> Dict[str, Any]:
        resolved = {}
        for target, binding in self._input_bindings.items():
            if binding.is_dynamic():
                if not context: raise RuntimeError(f"{self.name} missing context")
                val = _deep_get(context.get_output(binding.source.name), binding.source_path)
                resolved[target] = val
            elif binding.is_static():
                resolved[target] = binding.static_value
        return resolved

    def _dynamic_mapping_for_source(self, source: 'Runnable') -> Dict[str, str]:
        mapping = {}
        for target, binding in self._input_bindings.items():
            if binding.is_dynamic() and binding.source is source:
                mapping[target] = ".".join(binding.source_path)
        return mapping

    def _static_binding_values(self) -> Dict[str, Any]:
        return {t: b.static_value for t, b in self._input_bindings.items() if b.is_static()}

    def _initialize_config(self, config_data):
        cls = getattr(self, "ConfigModel", None)
        if not cls: return config_data
        if isinstance(config_data, dict): return cls(**config_data)
        return config_data or cls()

    def get_config(self) -> Any: return self.config
    
    def describe_input_schema(self) -> Dict[str, str]:
        if not self._input_model_cls: return {}
        return {k: self._type_name(_get_field_type(f)) for k, f in _get_model_fields(self._input_model_cls).items()}

    def describe_output_schema(self) -> Dict[str, str]:
        if not self._output_model_cls: return {}
        return {k: self._type_name(_get_field_type(f)) for k, f in _get_model_fields(self._output_model_cls).items()}

    @staticmethod
    def _type_name(tp: Any) -> str:
        try: return tp.__name__
        except AttributeError: return str(tp)

    def _normalize_direct_input(self, direct_input: Any) -> Any:
        if direct_input is NO_INPUT or direct_input is None: return {}
        
        if isinstance(direct_input, BaseModel): 
            d = _model_to_dict(direct_input)
            fields = _get_model_fields(self._input_model_cls) if self._input_model_cls else {}
            if self._input_model_cls and len(d) == 1 and len(fields) == 1:
                return {list(fields.keys())[0]: list(d.values())[0]}
            return d
            
        if isinstance(direct_input, dict):
            if self._input_model_cls:
                fields = _get_model_fields(self._input_model_cls)
                if len(fields) == 1 and len(direct_input) == 1:
                    target_key = list(fields.keys())[0]
                    if target_key not in direct_input:
                        return {target_key: list(direct_input.values())[0]}
            return direct_input
        
        if self._input_model_cls:
            fields = _get_model_fields(self._input_model_cls)
            if len(fields) == 1:
                return {list(fields.keys())[0]: direct_input}
        
        return direct_input

    def _prepare_input_payload(self, direct_input, context):
        payload = self._resolve_inputs_from_bindings(context)
        normalized = self._normalize_direct_input(direct_input)
        if isinstance(normalized, dict): payload.update(normalized)
        elif not payload: return normalized
        return payload if payload else NO_INPUT

    @staticmethod
    def _model_to_plain(payload: Any) -> Dict[str, Any]:
        if isinstance(payload, BaseModel): return _model_to_dict(payload)
        return dict(payload) if isinstance(payload, dict) else {"value": payload}

    def _get_cache_key(self, input_data, context):
        binding_sig = tuple(sorted((t, (b.source.name if b.source else None, b.source_path, b.static_value)) for t, b in self._input_bindings.items()))
        return self._cache_key_generator(self.name, input_data, context, binding_sig)

    def set_on_start(self, handler): self._on_start_handler = handler; return self
    def set_on_complete(self, handler): self._on_complete_handler = handler; return self
    def on_error(self, handler): self._error_handler = handler; return self
    def retry(self, max_attempts=3, delay_seconds=1, retry_on_exceptions=(Exception,)):
        self._retry_config = {"max_attempts": max_attempts, "delay_seconds": delay_seconds, "retry_on_exceptions": retry_on_exceptions}
        return self
    def clear_cache(self): self._invoke_cache = {}; return self
    def copy(self):
        import copy
        n = copy.deepcopy(self)
        n._invoke_cache = {}
        n._input_model_cls = self._input_model_cls
        n._output_model_cls = self._output_model_cls
        return n
    
    def to_graph(self, graph_name=None):
        from .graph import WorkflowGraph, _GraphExportHelper
        g = WorkflowGraph(name=graph_name or f"GraphFrom_{self.name}")
        h = _GraphExportHelper()
        entries, exits = self._expand_to_graph(g, h)
        for e in entries: 
            g.set_entry_point(e); h.ensure_static_inputs(g, e)
        g.set_output_nodes(exits)
        return g

    def _expand_to_graph(self, graph, helper):
        name = helper.add_node(graph, self)
        return [name], [name]

    def to_dict(self) -> Dict[str, Any]:
        """序列化自身配置"""
        data = {
            "name": self.name,
            "type": self.__class__.__name__,
        }
        if self.config:
            if isinstance(self.config, BaseModel):
                data["config"] = _model_to_dict(self.config)
            else:
                data["config"] = self.config
        return data

    def __or__(self, other):
        from .async_runnables import AsyncRunnable, AsyncPipeline
        if isinstance(other, Runnable) and not isinstance(other, dict):
            if isinstance(self, AsyncRunnable) or isinstance(other, AsyncRunnable): return AsyncPipeline(self, other)
            return Pipeline(self, other)
        elif isinstance(other, dict):
            if isinstance(self, Switch): return SwitchBranches(self, other)
            from .async_runnables import AsyncBranchAndFanIn
            has_async = isinstance(self, AsyncRunnable) or any(isinstance(r, AsyncRunnable) for r in other.values())
            if has_async: return AsyncPipeline(self, AsyncBranchAndFanIn(other))
            return Pipeline(self, BranchAndFanIn(other))
        return NotImplemented

    # INVOKE
    def invoke(self, input_data=NO_INPUT, context=None, resume_state=None):
        ctx = context or InMemoryExecutionContext(initial_input=input_data if input_data is not NO_INPUT else None)
        prepared_input = self._prepare_input_payload(input_data, ctx)
        
        # Start Hook
        if not resume_state:
            self._emit_node_status(ctx, "start")
            if self._on_start_handler:
                try: 
                    if isinstance(self._on_start_handler, Runnable): self._on_start_handler.invoke(prepared_input, ctx)
                    else: self._on_start_handler(ctx, prepared_input)
                except Exception: pass

        # Cache Check
        result = NO_INPUT
        cache_key = self._get_cache_key(prepared_input, ctx) if self.use_cache else None
        
        if not resume_state and self.use_cache and cache_key in self._invoke_cache:
            result = self._invoke_cache[cache_key]
            if self.name: ctx.add_output(self.name, result)
            self._emit_node_status(ctx, "success")
            return result

        # Retry Logic Setup
        retry_cfg = self._retry_config or {}
        max_attempts = retry_cfg.get("max_attempts", 1) if not resume_state else 1
        delay = retry_cfg.get("delay_seconds", 0)
        exceptions_to_retry = retry_cfg.get("retry_on_exceptions", (Exception,))
        
        attempt = 0
        last_exception = None
        
        while attempt < max_attempts:
            attempt += 1
            try:
                skip_input = (resume_state is not None and (prepared_input is NO_INPUT or not prepared_input))
                valid_input = NO_INPUT if skip_input else self._apply_input_model(prepared_input)
                
                sig = inspect.signature(self._internal_invoke)
                if 'resume_state' in sig.parameters:
                    raw_res = self._internal_invoke(valid_input, ctx, resume_state=resume_state)
                else:
                    raw_res = self._internal_invoke(valid_input, ctx)
                
                result = self._apply_output_model(raw_res)
                
                # Success
                if self.use_cache and not resume_state: 
                    self._invoke_cache[cache_key] = result
                if self.name: ctx.add_output(self.name, result)
                self._emit_node_status(ctx, "success")
                
                # Complete Hook (Success)
                if self._on_complete_handler:
                    try:
                        if isinstance(self._on_complete_handler, Runnable): self._on_complete_handler.invoke(prepared_input, ctx)
                        else: self._on_complete_handler(ctx, result if result is not NO_INPUT else None, None)
                    except Exception: pass
                
                return result

            except SuspendExecution:
                raise 
                
            except Exception as e:
                last_exception = e
                is_retryable = isinstance(e, exceptions_to_retry)
                
                if is_retryable and attempt < max_attempts:
                    self._emit_node_status(ctx, "retry", {"attempt": attempt, "error": str(e)})
                    if delay > 0: time.sleep(delay)
                    continue
                else:
                    # Final Failure -> Error Handler
                    if self._error_handler and not resume_state:
                        try:
                            if isinstance(self._error_handler, Runnable): 
                                result = self._apply_output_model(self._error_handler.invoke(prepared_input, ctx))
                            else: 
                                result = self._apply_output_model(self._error_handler(ctx, prepared_input, e))
                            
                            if self.name: ctx.add_output(self.name, result)
                            self._emit_node_status(ctx, "success") # Handled
                            return result
                        except Exception as eh:
                            last_exception = eh 
                    
                    # Complete Hook (Failed)
                    if self._on_complete_handler:
                        try:
                            if isinstance(self._on_complete_handler, Runnable): self._on_complete_handler.invoke(prepared_input, ctx)
                            else: self._on_complete_handler(ctx, None, last_exception)
                        except Exception: pass
                        
                    self._emit_node_status(ctx, "failed", {"error": str(last_exception)})
                    raise last_exception

        raise last_exception if last_exception else RuntimeError("Invoke failed unexpectedly")

    @abc.abstractmethod
    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any: pass

    async def invoke_async(self, input_data=NO_INPUT, context=None, resume_state=None):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.invoke, input_data, context, resume_state)

    def _emit_node_status(self, ctx, status, metadata=None):
        if ctx: ctx.notify_status(self.name, status, metadata)

    def _apply_input_model(self, payload):
        if not self._input_model_cls: return payload
        try: return self._input_model_cls(**(payload if payload is not NO_INPUT else {}))
        except ValidationError as e: raise ValueError(f"{self.name} Input Error: {e}")

    def _apply_output_model(self, result):
        if not self._output_model_cls: return result
        try:
            if isinstance(result, self._output_model_cls): return result
            if isinstance(result, BaseModel): return _model_validate(self._output_model_cls, result)
            
            if isinstance(result, dict):
                try: return self._output_model_cls(**result)
                except ValidationError: pass
            
            fields = _get_model_fields(self._output_model_cls)
            if len(fields) == 1:
                return self._output_model_cls(**{list(fields.keys())[0]: result})
            
            return self._output_model_cls(**(result if isinstance(result, dict) else {}))
        except ValidationError as e: raise ValueError(f"{self.name} Output Error: {e}")
    
    @staticmethod
    def _convert_to_payload(input_data):
        return Runnable._model_to_plain(input_data)

# =============================================================================
# 核心组件
# =============================================================================

class START(Runnable):
    def __init__(self, schema: Type[BaseModel], name="START"):
        self.InputModel = schema; self.OutputModel = schema
        super().__init__(name=name)
    def _internal_invoke(self, input_data, context): return input_data

class END(Runnable):
    def __init__(self, schema: Type[BaseModel], name="END"):
        self.InputModel = schema; self.OutputModel = schema
        super().__init__(name=name)
    def _internal_invoke(self, input_data, context): return input_data

class Switch(Runnable):
    class ConfigModel(BaseModel):
        rules: List[Tuple[str, str]]; default_branch: str = "DEFAULT"
    class OutputModel(BaseModel): decision: str
    def __init__(self, name=None, config=None, **kwargs):
        super().__init__(name=name or "Switch", config=config, **kwargs)
    def _internal_invoke(self, input_data, context):
        payload = self._normalize_direct_input(input_data)
        matched = self.config.default_branch
        for expr, branch in self.config.rules:
            try:
                if _safe_eval(expr, dict(payload)): matched = branch; break
            except Exception: pass
        return {"decision": matched}

class Loop(Runnable):
    class ConfigModel(BaseModel): condition: str; max_loops: int = 100
    def __init__(self, body: Runnable, name=None, config=None, **kwargs):
        self.body_runnable = body
        self.InputModel = body._input_model_cls; self.OutputModel = body._output_model_cls
        super().__init__(name=name or f"Loop[{body.name}]", config=config, **kwargs)

    def _internal_invoke(self, input_data, context, resume_state=None):
        return self._run_loop_sync(input_data, context, resume_state)

    async def _internal_invoke_async(self, input_data, context, resume_state=None):
        return await self._run_loop_async(input_data, context, resume_state)

    def _run_loop_sync(self, input_data, context, resume_state):
        loop_cnt = resume_state.get("loop_cnt", 0) if resume_state else 0
        payload = resume_state.get("payload", self._normalize_direct_input(input_data)) if resume_state else self._normalize_direct_input(input_data)
        child_resume = resume_state.get("child_state") if resume_state else None

        while loop_cnt < self.config.max_loops:
            eval_ctx = dict(payload); eval_ctx['loop_count'] = loop_cnt
            try: should_continue = _safe_eval(self.config.condition, eval_ctx)
            except: break
            if not should_continue: break
            
            try:
                res = self.body_runnable.invoke(payload, context, resume_state=child_resume)
            except SuspendExecution as e:
                raise SuspendExecution({"loop_cnt": loop_cnt, "payload": payload, "child_state": e.snapshot})
            
            child_resume = None
            payload = self._model_to_plain(res)
            loop_cnt += 1
        return payload

    async def _run_loop_async(self, input_data, context, resume_state):
        loop_cnt = resume_state.get("loop_cnt", 0) if resume_state else 0
        payload = resume_state.get("payload", self._normalize_direct_input(input_data)) if resume_state else self._normalize_direct_input(input_data)
        child_resume = resume_state.get("child_state") if resume_state else None

        while loop_cnt < self.config.max_loops:
            eval_ctx = dict(payload); eval_ctx['loop_count'] = loop_cnt
            try: should_continue = _safe_eval(self.config.condition, eval_ctx)
            except: break
            if not should_continue: break
            
            try:
                res = await self.body_runnable.invoke_async(payload, context, resume_state=child_resume)
            except SuspendExecution as e:
                raise SuspendExecution({"loop_cnt": loop_cnt, "payload": payload, "child_state": e.snapshot})
            
            child_resume = None
            payload = self._model_to_plain(res)
            loop_cnt += 1
        return payload

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["body"] = self.body_runnable.to_dict()
        return data

class Map(Runnable):
    class ConfigModel(BaseModel): max_concurrency: int = 5
    def __init__(self, body: Runnable, name=None, config=None, **kwargs):
        self.body_runnable = body
        self.InputModel = None; self.OutputModel = None
        super().__init__(name=name or f"Map[{body.name}]", config=config, **kwargs)

    def _internal_invoke(self, input_data, context, resume_state=None):
        input_list = self._extract_list(input_data)
        results = resume_state.get("results", [None]*len(input_list)) if resume_state else [None]*len(input_list)
        child_states = resume_state.get("child_states", {}) if resume_state else {}
        pending = [i for i, r in enumerate(results) if r is None]
        
        has_suspension = False
        with ThreadPoolExecutor(self.config.max_concurrency) as ex:
            f_map = {ex.submit(self.body_runnable.invoke, input_list[i], context, resume_state=child_states.get(i)): i for i in pending}
            for f in as_completed(f_map):
                i = f_map[f]
                try: results[i] = f.result(); 
                except SuspendExecution as e: has_suspension = True; child_states[i] = e.snapshot
                except Exception as e: results[i] = {"error": str(e)}
        
        if has_suspension: raise SuspendExecution({"results": results, "child_states": child_states})
        return results

    async def _internal_invoke_async(self, input_data, context, resume_state=None):
        input_list = self._extract_list(input_data)
        results = resume_state.get("results", [None]*len(input_list)) if resume_state else [None]*len(input_list)
        child_states = resume_state.get("child_states", {}) if resume_state else {}
        pending = [i for i, r in enumerate(results) if r is None]
        
        tasks = [self.body_runnable.invoke_async(input_list[i], context, resume_state=child_states.get(i)) for i in pending]
        batch = await asyncio.gather(*tasks, return_exceptions=True)
        
        has_suspension = False
        for i, res in zip(pending, batch):
            if isinstance(res, SuspendExecution): child_states[i] = res.snapshot; has_suspension = True
            elif isinstance(res, Exception): results[i] = {"error": str(res)}
            else: results[i] = res
        
        if has_suspension: raise SuspendExecution({"results": results, "child_states": child_states})
        return results

    def _extract_list(self, data):
        if isinstance(data, list): return data
        payload = self._normalize_direct_input(data)
        for v in payload.values(): 
            if isinstance(v, list): return v
        return [payload]

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["body"] = self.body_runnable.to_dict()
        return data

class WaitForInput(Runnable):
    class InputModel(BaseModel): pass 
    class OutputModel(BaseModel): result: Any
    def __init__(self, name=None, **kwargs): super().__init__(name=name or "WaitForInput", **kwargs)
    def _internal_invoke(self, input_data, context, resume_state=None):
        if resume_state is not None:
            context.log_event(f"Node '{self.name}': Resumed with input.")
            return {"result": resume_state}
        context.log_event(f"Node '{self.name}': Suspending.")
        raise SuspendExecution(snapshot=None)

class Pipeline(Runnable):
    def __init__(self, first: Runnable, second: Runnable, name=None, **kwargs):
        self.first = first; self.second = second
        self.InputModel = first._input_model_cls; self.OutputModel = second._output_model_cls
        super().__init__(name or f"Pipe[{first.name}->{second.name}]", **kwargs)
    def _internal_invoke(self, input_data, context, resume_state=None):
        step = resume_state.get("step", 0) if resume_state else 0
        data = resume_state.get("data", input_data) if resume_state else input_data
        
        if step == 0:
            try:
                child = resume_state.get("child") if (resume_state and resume_state.get("step")==0) else None
                data = self.first.invoke(data, context, resume_state=child)
            except SuspendExecution as e: raise SuspendExecution({"step": 0, "data": data, "child": e.snapshot})
            step = 1
        
        if step == 1:
            try:
                child = resume_state.get("child") if (resume_state and resume_state.get("step")==1) else None
                return self.second.invoke(data, context, resume_state=child)
            except SuspendExecution as e: raise SuspendExecution({"step": 1, "data": data, "child": e.snapshot})

    def _expand_to_graph(self, graph, helper):
        ef, xf = self.first._expand_to_graph(graph, helper)
        es, xs = self.second._expand_to_graph(graph, helper)
        for p in xf:
            for c in es: helper.connect_by_name(graph, p, c)
        return ef, xs

class BranchAndFanIn(Runnable):
    def __init__(self, tasks_dict: Dict[str, Runnable], name=None, max_workers=None, **kwargs):
        self.tasks_dict = tasks_dict; self.max_workers = max_workers
        self.InputModel = next(iter(tasks_dict.values()))._input_model_cls
        fields = {k: (t._output_model_cls, ...) for k, t in tasks_dict.items()}
        self.OutputModel = create_model(f"BranchOutput", **fields)
        super().__init__(name or "BranchAndFanIn", **kwargs)
    def _internal_invoke(self, input_data, context):
        with ThreadPoolExecutor(self.max_workers) as ex:
            futures = {k: ex.submit(t.invoke, input_data, context) for k, t in self.tasks_dict.items()}
            return {k: f.result() for k, f in futures.items()}

class SourceParallel(Runnable):
    def __init__(self, tasks_dict: Dict[str, Runnable], name=None, max_workers=None, **kwargs):
        self.tasks_dict = tasks_dict; self.max_workers = max_workers
        self.InputModel = next(iter(tasks_dict.values()))._input_model_cls
        fields = {k: (t._output_model_cls, ...) for k, t in tasks_dict.items()}
        self.OutputModel = create_model(f"SourceParOutput", **fields)
        super().__init__(name or "SourceParallel", **kwargs)
    def _internal_invoke(self, input_data, context):
        with ThreadPoolExecutor(self.max_workers) as ex:
            futures = {k: ex.submit(t.invoke, input_data, context) for k, t in self.tasks_dict.items()}
            res = {k: f.result() for k, f in futures.items()}
            for k, v in res.items(): context.add_output(f"{self.name}_{k}", v)
            return res

class SwitchBranches(Runnable):
    def __init__(self, switch_node: Runnable, branches: Dict[str, Runnable]):
        self.switch_node = switch_node; self.branches = branches
        self.InputModel = switch_node.InputModel
        self.OutputModel = create_model("SwitchOut", decision=(str, ...)) 
        super().__init__(name=f"SwitchBranches_{switch_node.name}")
    def _internal_invoke(self, input_data, context): raise NotImplementedError("Graph-only")
    def _expand_to_graph(self, graph, helper):
        sw = helper.add_node(graph, self.switch_node)
        exits = []
        for branch, task in self.branches.items():
            en, ex = task._expand_to_graph(graph, helper)
            for e in en: helper.connect_by_name(graph, sw, e, branch=branch)
            exits.extend(ex)
        return [sw], exits

class ScriptRunnable(Runnable):
    class ConfigModel(BaseModel): code: str; input_keys: List[str] = []; output_keys: List[str] = []
    def __init__(self, name=None, config=None, **kwargs):
        super().__init__(name or "Script", config=config, **kwargs)
        self.InputModel = create_model(f"{self.name}In", **{k: (Any, ...) for k in self.config.input_keys})
        self.OutputModel = create_model(f"{self.name}Out", **{k: (Any, ...) for k in self.config.output_keys})
    def _internal_invoke(self, input_data, context):
        payload = self._normalize_direct_input(input_data)
        loc = {k: payload.get(k) for k in self.config.input_keys}; loc["context"] = context
        exec(self.config.code, {}, loc)
        return {k: loc.get(k) for k in self.config.output_keys} if self.config.output_keys else {"result": loc.get("result")}

def task(func=None, *, name=None):
    def decorator(fn):
        is_async = inspect.iscoroutinefunction(fn)
        from .async_runnables import AsyncRunnable
        Base = AsyncRunnable if is_async else Runnable
        hints = get_type_hints(fn)
        in_fields = {k: (v, ...) for k, v in hints.items() if k != 'return' and k != 'context'}
        out_hint = hints.get('return', Any)
        out_fields = getattr(out_hint, "model_fields", {"result": (out_hint, ...)}) if hasattr(out_hint, "model_fields") else (out_hint if isinstance(out_hint, dict) else {"result": (out_hint, ...)})
        if not isinstance(out_fields, dict): out_fields = {"result": (out_hint, ...)}
        class TaskWrapper(Base):
            InputModel = create_model(f"{fn.__name__}In", **in_fields)
            OutputModel = out_hint if inspect.isclass(out_hint) and issubclass(out_hint, BaseModel) else create_model(f"{fn.__name__}Out", **out_fields)
            def __init__(self, **kw): 
                runtime_name = kw.pop('name', None) or name or fn.__name__
                super().__init__(name=runtime_name, **kw)
            if is_async:
                async def _internal_invoke_async(self, d, c): 
                    p = self._normalize_direct_input(d); 
                    if 'context' in inspect.signature(fn).parameters: p['context'] = c
                    return await fn(**p)
            else:
                def _internal_invoke(self, d, c): 
                    p = self._normalize_direct_input(d); 
                    if 'context' in inspect.signature(fn).parameters: p['context'] = c
                    return fn(**p)
        return TaskWrapper
    return decorator(func) if func else decorator