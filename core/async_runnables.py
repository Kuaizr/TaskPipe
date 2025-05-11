import abc
import asyncio
import pickle
from typing import Any, Optional, Dict, List, Tuple, Union, Type, Coroutine
from .runnables import (
    Runnable, 
    ExecutionContext,
    NO_INPUT,
    _default_cache_key_generator
)

class AsyncRunnable(Runnable):
    """异步Runnable基类，继承自Runnable"""
    
    async def invoke_async(self, input_data: Any = NO_INPUT, 
                         context: Optional[ExecutionContext] = None) -> Any:
        """异步版本的invoke方法"""
        effective_context = context if context is not None else ExecutionContext(
            initial_input=input_data if input_data is not NO_INPUT else None)
            
        # 确定实际输入数据
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
                actual_input_for_invoke = self.input_declaration(effective_context)

        cache_key = self._get_cache_key(actual_input_for_invoke, effective_context)
        
        if cache_key in self._invoke_cache:
            effective_context.log_event(f"Node '{self.name}': Async invoke result from cache.")
            result = self._invoke_cache[cache_key]
            if self.name and effective_context:
                effective_context.add_output(self.name, result)
            return result

        current_attempt = 0
        max_attempts = (self._retry_config or {}).get("max_attempts", 1)
        delay_seconds = (self._retry_config or {}).get("delay_seconds", 0)
        retry_on_exceptions = (self._retry_config or {}).get("retry_on_exceptions", (Exception,))

        while current_attempt < max_attempts:
            current_attempt += 1
            effective_context.log_event(
                f"Node '{self.name}': Async invoking (Attempt {current_attempt}/{max_attempts}). "
                f"Input type: {type(actual_input_for_invoke).__name__}")

            try:
                result = await self._internal_invoke_async(actual_input_for_invoke, effective_context)
                self._invoke_cache[cache_key] = result
                effective_context.log_event(
                    f"Node '{self.name}': Async invoked successfully. "
                    f"Output type: {type(result).__name__}.")
                
                if self.name:
                    effective_context.add_output(self.name, result)
                return result

            except Exception as e:
                effective_context.log_event(
                    f"Node '{self.name}': Error during async invoke "
                    f"(Attempt {current_attempt}/{max_attempts}): "
                    f"{type(e).__name__}: {e}")

                is_retryable = isinstance(e, retry_on_exceptions)
                is_last_attempt = (current_attempt == max_attempts)

                if not is_retryable or is_last_attempt:
                    if self._error_handler:
                        effective_context.log_event(
                            f"Node '{self.name}': Invoking async error handler "
                            f"'{self._error_handler.name}'.")
                        try:
                            error_handler_output = await self._error_handler.invoke_async(
                                actual_input_for_invoke, effective_context)
                            effective_context.log_event(
                                f"Node '{self.name}': Async error handler "
                                f"'{self._error_handler.name}' executed.")
                            if self.name:
                                effective_context.add_output(self.name, error_handler_output)
                            self._invoke_cache[cache_key] = error_handler_output
                            return error_handler_output
                        except Exception as eh_e:
                            effective_context.log_event(
                                f"Node '{self.name}': Async error handler "
                                f"'{self._error_handler.name}' also failed: "
                                f"{type(eh_e).__name__}: {eh_e}")
                            raise e from eh_e
                    raise e
                
                effective_context.log_event(
                    f"Node '{self.name}': Async retrying in {delay_seconds} seconds.")
                await asyncio.sleep(delay_seconds)

    async def check_async(self, data_from_invoke: Any, 
                         context: Optional[ExecutionContext] = None) -> bool:
        """异步版本的check方法"""
        try:
            cache_key = pickle.dumps(data_from_invoke)
        except Exception:
            cache_key = object()

        if cache_key in self._check_cache:
            if context: 
                context.log_event(f"Node '{self.name}': Async check result from cache.")
            return self._check_cache[cache_key]

        if self._custom_check_fn:
            result = await self._custom_check_fn(data_from_invoke)
        else:
            result = await self._default_check_async(data_from_invoke)

        self._check_cache[cache_key] = result
        if context: 
            context.log_event(f"Node '{self.name}': Async check result: {result}.")
        return result

    async def _default_check_async(self, data_from_invoke: Any) -> bool:
        """异步默认检查逻辑"""
        return bool(data_from_invoke)

    @abc.abstractmethod
    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Any:
        """子类必须实现的异步内部调用方法"""
        pass
    
    # 实现父类的同步_internal_invoke方法
    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        """同步版本的invoke方法，通过asyncio.run调用异步实现"""
        return asyncio.run(self._internal_invoke_async(input_data, context))


class AsyncBranchAndFanIn(AsyncRunnable):
    """异步版本的BranchAndFanIn"""
    
    def __init__(self, tasks_dict: Dict[str, AsyncRunnable], 
                name: Optional[str] = None, **kwargs):
        super().__init__(name or f"AsyncBranchFanIn_{'_'.join(tasks_dict.keys())}", **kwargs)
        self.tasks_dict = tasks_dict

    async def _internal_invoke_async(self, input_data: Any, 
                                   context: ExecutionContext) -> Dict[str, Any]:
        context.log_event(
            f"Node '{self.name}': Starting async parallel execution for "
            f"{len(self.tasks_dict)} tasks.")
        
        # 创建任务列表
        tasks = {
            key: asyncio.create_task(task.invoke_async(input_data, context))
            for key, task in self.tasks_dict.items()
        }
        
        # 等待所有任务完成
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        # 构建结果字典
        final_results = {
            key: result if not isinstance(result, Exception) else 
            (await self._handle_task_error(key, result, input_data, context))
            for key, result in zip(tasks.keys(), results)
        }
        
        return final_results


class AsyncSourceParallel(AsyncRunnable):
    """异步版本的SourceParallel"""
    
    def __init__(self, tasks_dict: Dict[str, AsyncRunnable], 
                name: Optional[str] = None, **kwargs):
        super().__init__(name or f"AsyncSourcePar_{'_'.join(tasks_dict.keys())}", **kwargs)
        self.tasks_dict = tasks_dict

    async def _internal_invoke_async(self, input_data: Any, 
                                   context: ExecutionContext) -> Dict[str, Any]:
        actual_input = input_data if input_data is not NO_INPUT else context.initial_input
        context.log_event(
            f"Node '{self.name}': Starting async parallel source execution for "
            f"{len(self.tasks_dict)} tasks.")
        
        # 创建任务列表
        tasks = {
            key: asyncio.create_task(task.invoke_async(actual_input, context))
            for key, task in self.tasks_dict.items()
        }
        
        # 等待所有任务完成
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        # 构建结果字典
        final_results = {
            key: result if not isinstance(result, Exception) else 
            (await self._handle_task_error(key, result, actual_input, context))
            for key, result in zip(tasks.keys(), results)
        }
        
        return final_results

    async def _handle_task_error(self, task_key: str, exception: Exception, 
                               input_data: Any, context: ExecutionContext) -> Any:
        """处理任务中的异常"""
        # 这里可以添加更详细的错误处理逻辑
        raise exception