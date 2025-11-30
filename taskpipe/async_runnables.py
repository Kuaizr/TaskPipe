import abc
import asyncio
import time
from typing import Any, Optional, Dict, List
from .runnables import (
    Runnable, ExecutionContext, InMemoryExecutionContext, NO_INPUT, 
    Pipeline as SyncPipeline, BranchAndFanIn as SyncBranchAndFanIn,
    _deep_get, SuspendExecution
)
from pydantic import BaseModel, create_model
import inspect

class AsyncRunnable(Runnable):
    async def invoke_async(self, input_data: Any = NO_INPUT, context: Optional[ExecutionContext] = None, resume_state: Any = None) -> Any:
        ctx = context or InMemoryExecutionContext(initial_input=input_data if input_data is not NO_INPUT else None)
        prepared_input = self._prepare_input_payload(input_data, ctx)
        
        # Start Hook
        if not resume_state:
            self._emit_node_status(ctx, "start")
            if self._on_start_handler:
                try:
                    if isinstance(self._on_start_handler, Runnable): await self._on_start_handler.invoke_async(prepared_input, ctx)
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
                skip_input_validation = (resume_state is not None and (prepared_input is NO_INPUT or not prepared_input))
                
                if skip_input_validation:
                    valid_input = NO_INPUT
                else:
                    valid_input = self._apply_input_model(prepared_input)
                
                sig = inspect.signature(self._internal_invoke_async)
                if 'resume_state' in sig.parameters:
                    raw_res = await self._internal_invoke_async(valid_input, ctx, resume_state=resume_state)
                else:
                    raw_res = await self._internal_invoke_async(valid_input, ctx)
                    
                result = self._apply_output_model(raw_res)
                
                # Success
                if self.use_cache and not resume_state: 
                    self._invoke_cache[cache_key] = result
                if self.name: ctx.add_output(self.name, result)
                self._emit_node_status(ctx, "success")
                
                # Complete Hook (Success)
                if self._on_complete_handler:
                    try:
                        if isinstance(self._on_complete_handler, Runnable): await self._on_complete_handler.invoke_async(prepared_input, ctx)
                    except Exception: pass
                
                return result
                
            except SuspendExecution:
                raise # 挂起异常直接抛出
    
            except Exception as e:
                last_exception = e
                is_retryable = isinstance(e, exceptions_to_retry)
                
                if is_retryable and attempt < max_attempts:
                    self._emit_node_status(ctx, "retry", {"attempt": attempt, "error": str(e)})
                    if delay > 0: await asyncio.sleep(delay)
                    continue
                else:
                    # Final Failure -> Error Handler
                    if self._error_handler and not resume_state:
                        try:
                            if isinstance(self._error_handler, Runnable): 
                                result = self._apply_output_model(await self._error_handler.invoke_async(prepared_input, ctx))
                            else: 
                                # 这里如果 error_handler 是同步函数，我们在 async 上下文中可能无法直接 await? 
                                # 但 Runnable.on_error 签名通常接收 sync 或 Runnable。
                                # 如果是 sync function, 我们在这里同步执行它。
                                # 之前 _internal_invoke 是直接调用的。
                                result = self._apply_output_model(self._error_handler(ctx, prepared_input, e))
                            
                            if self.name: ctx.add_output(self.name, result)
                            self._emit_node_status(ctx, "success") # Handled
                            return result
                        except Exception as eh: 
                            last_exception = eh
                    
                    # Complete Hook (Failed)
                    if self._on_complete_handler:
                        try:
                            if isinstance(self._on_complete_handler, Runnable): await self._on_complete_handler.invoke_async(prepared_input, ctx)
                        except Exception: pass
                        
                    self._emit_node_status(ctx, "failed", {"error": str(last_exception)})
                    raise last_exception

        raise last_exception if last_exception else RuntimeError("Async invoke failed unexpectedly")

    @abc.abstractmethod
    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Any: pass

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running(): raise RuntimeError("Cannot sync invoke AsyncRunnable inside event loop.")
        except RuntimeError: pass
        return asyncio.run(self._internal_invoke_async(input_data, context))

class AsyncPipeline(AsyncRunnable):
    def __init__(self, first: Runnable, second: Runnable, name=None, **kwargs):
        self.first = first; self.second = second
        self.InputModel = first._input_model_cls; self.OutputModel = second._output_model_cls
        super().__init__(name or f"AsyncPipe[{first.name}->{second.name}]", **kwargs)

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext, resume_state=None) -> Any:
        step = resume_state.get("step", 0) if resume_state else 0
        data = resume_state.get("data", input_data) if resume_state else input_data
        
        if step == 0:
            try:
                child_state = resume_state.get("child") if (resume_state and resume_state.get("step") == 0) else None
                data = await self.first.invoke_async(data, context, resume_state=child_state)
            except SuspendExecution as e:
                raise SuspendExecution({"step": 0, "data": data, "child": e.snapshot})
            step = 1
        
        if step == 1:
            try:
                child_state = resume_state.get("child") if (resume_state and resume_state.get("step") == 1) else None
                return await self.second.invoke_async(data, context, resume_state=child_state)
            except SuspendExecution as e:
                raise SuspendExecution({"step": 1, "data": data, "child": e.snapshot})

    def _expand_to_graph(self, graph, helper):
        ef, xf = self.first._expand_to_graph(graph, helper)
        es, xs = self.second._expand_to_graph(graph, helper)
        for p in xf:
            for c in es: helper.connect_by_name(graph, p, c)
        return ef, xs

class AsyncBranchAndFanIn(AsyncRunnable):
    def __init__(self, tasks_dict: Dict[str, Runnable], name=None, **kwargs):
        self.tasks_dict = tasks_dict
        self.InputModel = next(iter(tasks_dict.values()))._input_model_cls
        fields = {k: (t._output_model_cls, ...) for k, t in tasks_dict.items()}
        self.OutputModel = create_model(f"AsyncBranchOut", **fields)
        super().__init__(name or "AsyncBranchAndFanIn", **kwargs)

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Dict[str, Any]:
        keys = list(self.tasks_dict.keys())
        tasks = [self.tasks_dict[k].invoke_async(input_data, context) for k in keys]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        final_res = {}
        for k, r in zip(keys, results):
            if isinstance(r, Exception): raise r
            final_res[k] = r
        return final_res

class AsyncSourceParallel(AsyncRunnable):
    def __init__(self, tasks_dict: Dict[str, Runnable], name=None, **kwargs):
        self.tasks_dict = tasks_dict
        self.InputModel = next(iter(tasks_dict.values()))._input_model_cls
        fields = {k: (t._output_model_cls, ...) for k, t in tasks_dict.items()}
        self.OutputModel = create_model(f"AsyncSourceParOut", **fields)
        super().__init__(name or "AsyncSourceParallel", **kwargs)

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Dict[str, Any]:
        keys = list(self.tasks_dict.keys())
        tasks = [self.tasks_dict[k].invoke_async(input_data, context) for k in keys]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        final_res = {}
        for k, r in zip(keys, results):
            if isinstance(r, Exception): raise r
            final_res[k] = r
            context.add_output(f"{self.name}_{k}", r)
        return final_res