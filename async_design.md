# 异步功能设计方案

## 目标
为TaskPipe框架添加异步支持，包括：
- AsyncRunnable基类
- 异步版本的invoke_async和check_async方法
- 修改BranchAndFanIn和SourceParallel以支持asyncio.gather

## 设计

### 1. AsyncRunnable基类
```python
class AsyncRunnable(Runnable):
    async def invoke_async(self, input_data: Any = NO_INPUT, context: Optional[ExecutionContext] = None) -> Any:
        # 异步版本的invoke
        pass

    async def check_async(self, data_from_invoke: Any, context: Optional[ExecutionContext] = None) -> bool:
        # 异步版本的check
        pass

    @abc.abstractmethod
    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Any:
        # 子类必须实现的异步内部调用
        pass
```

### 2. 异步并行执行
修改BranchAndFanIn和SourceParallel，使用asyncio.gather替代ThreadPoolExecutor

### 3. 兼容性
- 保持现有同步API不变
- 提供async/await接口作为扩展

## 测试计划
1. 基本异步功能测试
2. 异步并行执行测试
3. 与同步API的互操作性测试