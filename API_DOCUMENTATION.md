# 工作流框架 API 文档

本文档介绍了基于 `Runnable`/`AsyncRunnable` 和 `WorkflowGraph` 的 Python 工作流框架的核心 API，支持同步和异步执行模式。

## 异步支持

### 关键特性

- 所有操作均支持同步和异步模式
- 使用 Python 原生 `asyncio` 实现
- 支持异步上下文管理器和协程
- 内置异步错误处理和重试机制

### 使用方法

```python
from taskpipe import AsyncRunnable, AsyncContext

class MyAsyncTask(AsyncRunnable):
    async def _internal_invoke_async(self, input_data, context):
        result = await some_async_operation()
        return result

async def main():
    task = MyAsyncTask()
    context = AsyncContext()
    result = await task.invoke_async("input", context)
```

### 性能优化

- 使用事件循环提高 I/O 密集型任务性能
- 支持并发执行多个任务
- 内存占用更小

## 目录结构

```
.
├── core/
│   ├── __init__.py
│   ├── graph.py       # 包含 WorkflowGraph 和 CompiledGraph
│   └── runnables.py   # 包含 Runnable/AsyncRunnable 基类及其各种实现, ExecutionContext
├── test_graph.py
└── test_runnables.py
```

## 类关系图 (Mermaid)

```mermaid
classDiagram
    direction LR

    class Runnable {
        <<Abstract>>
        +name: str
        +input_declaration: Any
        +_invoke_cache: Dict
        +_check_cache: Dict
        +_custom_check_fn: Optional[Callable]
        +_error_handler: Optional[Runnable]
        +_retry_config: Optional[Dict]
        +_cache_key_generator: Callable
        +invoke(input_data: Any, context: ExecutionContext) Any
        +_internal_invoke(input_data: Any, context: ExecutionContext) Any
        +check(data_from_invoke: Any, context: ExecutionContext) bool
        +_default_check(data_from_invoke: Any) bool
        +set_check(func: Callable) Runnable
        +on_error(error_handler_runnable: Runnable) Runnable
        +retry(max_attempts: int, delay_seconds: Union[int, float], retry_on_exceptions: Union[Type[Exception], Tuple]) Runnable
        +clear_cache(cache_name: str) Runnable
        +copy() Runnable
    }

    class AsyncRunnable {
        <<Abstract>>
        +invoke_async(input_data: Any, context: ExecutionContext) Coroutine[Any]
        +check_async(data_from_invoke: Any, context: ExecutionContext) Coroutine[bool]
        +_internal_invoke_async(input_data: Any, context: ExecutionContext) Coroutine[Any]
        +_default_check_async(data_from_invoke: Any) Coroutine[bool]
    }
    Runnable <|-- AsyncRunnable
        <<Abstract>>
        +name: str
        +input_declaration: Any
        +_invoke_cache: Dict
        +_check_cache: Dict
        +_custom_check_fn: Optional[Callable]
        +_error_handler: Optional[Runnable]
        +_retry_config: Optional[Dict]
        +_cache_key_generator: Callable
        +invoke(input_data: Any, context: ExecutionContext) Any
        +_internal_invoke(input_data: Any, context: ExecutionContext) Any
        +check(data_from_invoke: Any, context: ExecutionContext) bool
        +_default_check(data_from_invoke: Any) bool
        +set_check(func: Callable) Runnable
        +on_error(error_handler_runnable: Runnable) Runnable
        +retry(max_attempts: int, delay_seconds: Union[int, float], retry_on_exceptions: Union[Type[Exception], Tuple]) Runnable
        +clear_cache(cache_name: str) Runnable
        +copy() Runnable
    }

    class ExecutionContext {
        +node_outputs: Dict[str, Any]
        +initial_input: Any
        +event_log: List[str]
        +parent_context: Optional[ExecutionContext]
        +add_output(node_name: str, value: Any)
        +get_output(node_name: str, default: Any) Any
        +log_event(message: str)
    }
    ExecutionContext "1" --o "0..1" ExecutionContext : parent_context >

    class SimpleTask {
        +func: Callable
    }
    Runnable <|-- SimpleTask

    class Pipeline {
        +first: Runnable
        +second: Runnable
    }
    Runnable <|-- Pipeline
    Pipeline o-- "2" Runnable : contains

    class _PendingConditional {
        +condition_r: Runnable
        +true_r: Runnable
    }
    _PendingConditional o-- "2" Runnable : holds

    class Conditional {
        +condition_r: Runnable
        +true_r: Runnable
        +false_r: Runnable
    }
    Runnable <|-- Conditional
    Conditional o-- "3" Runnable : branches

    class BranchAndFanIn {
        +tasks_dict: Dict[str, Runnable]
        +max_workers: Optional[int]
    }
    Runnable <|-- BranchAndFanIn
    BranchAndFanIn o-- "*" Runnable : fans out to

    class SourceParallel {
        +tasks_dict: Dict[str, Runnable]
        +max_workers: Optional[int]
    }
    Runnable <|-- SourceParallel
    SourceParallel o-- "*" Runnable : fans out to

    class While {
        +condition_check_runnable: Runnable
        +body_runnable: Runnable
        +max_loops: int
    }
    Runnable <|-- While
    While o-- "2" Runnable : uses

    class MergeInputs {
        +input_sources: Dict[str, str]
        +merge_function: Callable
    }
    Runnable <|-- MergeInputs

    class WorkflowGraph {
        +name: str
        +nodes: Dict[str, Runnable]
        +edges: Dict[str, List[Tuple[str, Optional[Callable]]]]
        +entry_points: List[str]
        +output_node_names: List[str]
        +add_node(runnable: Runnable, node_name: Optional[str]) str
        +add_edge(source_node_name: str, dest_node_name: str, input_mapper: Optional[Callable])
        +set_entry_point(node_name: str) WorkflowGraph
        +set_output_nodes(node_names: List[str]) WorkflowGraph
        +compile() CompiledGraph
    }
    WorkflowGraph o-- "*" Runnable : has nodes
    WorkflowGraph ..> CompiledGraph : compiles to

    class CompiledGraph {
        +nodes: Dict[str, Runnable]
        +edges: Dict[str, List[Tuple[str, Optional[Callable]]]]
        +sorted_nodes: List[str]
        +entry_points: List[str]
        +output_nodes: List[str]
        +graph_def_name: str
    }
    Runnable <|-- CompiledGraph
    CompiledGraph o-- "*" Runnable : executes nodes
    CompiledGraph ..> ExecutionContext : uses internally

    Runnable "0..1" --* ExecutionContext : uses
    _PendingConditional ..> Conditional : creates
```

## 核心 API

### 1. `core.runnables.ExecutionContext` (同步/异步)

此类用于在工作流执行期间携带状态和节点输出。

**主要属性:**

*   `initial_input: Any`: 工作流或当前图执行的初始输入数据。
*   `node_outputs: Dict[str, Any]`: 存储已执行节点的输出，键是节点名称。
*   `event_log: List[str]`: 记录工作流执行期间的事件。
*   `parent_context: Optional['ExecutionContext']`: 指向父执行上下文的引用，用于嵌套图。

**主要方法:**

*   `__init__(self, initial_input: Any = NO_INPUT, parent_context: Optional['ExecutionContext'] = None)`: 构造函数。
*   `add_output(self, node_name: str, value: Any)`: 将节点输出添加到上下文中。
*   `get_output(self, node_name: str, default: Any = None) -> Any`: 从上下文（或父上下文）获取节点输出。
*   `log_event(self, message: str)`: 记录一个带时间戳的事件。

**特殊值:**

*   `NO_INPUT`: 一个哨兵对象，标记 `Runnable` 没有接收到直接输入，应尝试从上下文获取。

### 2. `core.runnables.Runnable` (ABC) 和 `AsyncRunnable`

所有可执行单元的抽象基类。

**主要属性:**

*   `name: str`: `Runnable` 实例的名称。
*   `input_declaration: Any`: 定义如何从 `ExecutionContext` 获取输入。可以是：
    *   字符串: 从上下文中获取单个节点输出。
    *   字典: 将上下文中的多个节点输出映射到内部调用所需的参数。
    *   可调用对象: 一个函数 `(ExecutionContext) -> Any`，返回所需输入。
*   `_invoke_cache: Dict`: 缓存 `invoke` 方法的结果。
*   `_check_cache: Dict`: 缓存 `check` 方法的结果。
*   `_error_handler: Optional[Runnable]`: 错误处理 `Runnable`。
*   `_retry_config: Optional[Dict]`: 重试配置。

**主要方法:**

*   `__init__(self, name: Optional[str] = None, input_declaration: Any = None, cache_key_generator: Optional[Callable] = None)`: 构造函数。
*   `invoke(self, input_data: Any = NO_INPUT, context: Optional[ExecutionContext] = None) -> Any`: 执行 `Runnable` 的核心公共方法。处理输入解析、缓存、重试和错误处理，并调用 `_internal_invoke`。
*   `_internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any`: (抽象方法) 子类必须实现此方法以定义其核心逻辑。
*   `check(self, data_from_invoke: Any, context: Optional[ExecutionContext] = None) -> bool`: 对 `invoke` 的结果进行验证或条件检查。
*   `_default_check(self, data_from_invoke: Any) -> bool`: 默认的检查逻辑 (通常是布尔测试)。
*   `set_check(self, func: Callable[[Any], bool]) -> 'Runnable'`: 设置自定义的 `check` 函数。
*   `on_error(self, error_handler_runnable: 'Runnable') -> 'Runnable'`: 指定一个错误处理器。
*   `retry(self, max_attempts: int = 3, delay_seconds: Union[int, float] = 1, retry_on_exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = (Exception,)) -> 'Runnable'`: 配置重试逻辑。
*   `clear_cache(self, cache_name: str = 'all') -> 'Runnable'`: 清除缓存。
*   `copy(self) -> 'Runnable'`: 创建 `Runnable` 的深拷贝。
*   `__or__` (`|`): 用于链接 `Runnable` (创建 `Pipeline` 或 `BranchAndFanIn`)。
*   `__mod__` (`%`), `__rshift__` (`>>`): 用于创建 `Conditional` `Runnable`。

### 3. `core.runnables` 中的具体实现

#### 同步实现

*   **`SimpleTask(Runnable)`**: 将普通函数包装成 `Runnable`。
    *   `__init__(self, func: Callable, name: Optional[str] = None, input_declaration: Any = None, **kwargs)`
*   **`Pipeline(Runnable)`**: 按顺序执行两个 `Runnable`。
    *   `__init__(self, first: Runnable, second: Runnable, name: Optional[str] = None, **kwargs)`
*   **`Conditional(Runnable)`**: 根据条件执行两个分支之一。
    *   `__init__(self, condition_r: Runnable, true_r: Runnable, false_r: Runnable, name: Optional[str] = None, **kwargs)`
*   **`BranchAndFanIn(Runnable)`**: 将单个输入扇出到多个并行任务，并聚合结果。
    *   `__init__(self, tasks_dict: Dict[str, Runnable], name: Optional[str] = None, max_workers: Optional[int] = None, **kwargs)`
*   **`SourceParallel(Runnable)`**: 并行执行多个任务链，每个链接收相同的初始输入。
    *   `__init__(self, tasks_dict: Dict[str, Runnable], name: Optional[str] = None, max_workers: Optional[int] = None, **kwargs)`
*   **`While(Runnable)`**: 根据条件重复执行一个 `Runnable` 主体。
    *   `__init__(self, condition_check_runnable: Runnable, body_runnable: Runnable, max_loops: int = 100, name: Optional[str] = None, **kwargs)`
*   **`MergeInputs(Runnable)`**: 从上下文收集多个输入，并传递给一个合并函数。
    *   `__init__(self, input_sources: Dict[str, str], merge_function: Callable[..., Any], name: Optional[str] = None, **kwargs)`

#### 异步实现

* **`AsyncBranchAndFanIn`**: 异步分支合并实现
* **`AsyncSourceParallel`**: 异步并行源实现

### 4. `core.graph.WorkflowGraph` (同步/异步)

用于构建工作流图的构建器类。

**主要属性:**

*   `name: str`: 图的名称。
*   `nodes: Dict[str, Runnable]`: 图中节点的字典。
*   `edges: Dict[str, List[Tuple[str, Optional[Callable]]]]`: 图中边的字典，定义依赖和数据映射。
*   `entry_points: List[str]`: 图的入口节点名称列表。
*   `output_node_names: List[str]`: 图的指定输出节点名称列表。

**主要方法:**

*   `__init__(self, name: Optional[str] = None)`: 构造函数。
*   `add_node(self, runnable: Runnable, node_name: Optional[str] = None) -> str`: 向图中添加一个节点（`Runnable` 的副本）。
*   `add_edge(self, source_node_name: str, dest_node_name: str, input_mapper: Optional[Callable[[Any, Dict[str, Any]], Any]] = None)`: 添加一条从源节点到目标节点的边，可选的 `input_mapper` 用于转换数据。
*   `set_entry_point(self, node_name: str) -> 'WorkflowGraph'`: 设置图的入口节点。
*   `set_output_nodes(self, node_names: List[str]) -> 'WorkflowGraph'`: 设置图的输出节点。
*   `compile(self) -> 'CompiledGraph'`: 分析图结构，执行拓扑排序，并返回一个可执行的 `CompiledGraph` 实例。

### 5. `core.graph.CompiledGraph(Runnable)`

`WorkflowGraph` 的可运行表示。

**主要属性 (从 `WorkflowGraph` 编译而来):**

*   `nodes: Dict[str, Runnable]`
*   `edges: Dict[str, List[Tuple[str, Optional[Callable]]]]`
*   `sorted_nodes: List[str]`: 按拓扑顺序排序的节点名称列表。
*   `entry_points: List[str]`
*   `output_nodes: List[str]`
*   `graph_def_name: str`: 从中编译此图的 `WorkflowGraph` 的名称。

**主要方法:**

*   `_internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any`: 执行编译后的图。它会遍历排序后的节点，处理输入依赖（包括 `input_declaration` 和 `input_mapper`），并调用每个节点的 `invoke` 方法。
*   `clear_cache(self, cache_name: str = 'all') -> 'CompiledGraph'`: 清除此编译图及其所有内部节点的缓存。

## 快速入门 (Quick Start)

本节通过几个具体示例展示如何使用本框架构建和执行工作流。这些示例直接使用了 `Runnable` 对象及其操作符重载，不依赖于 `WorkflowGraph` 的显式定义，展示了框架的灵活性。

### 示例 1: 条件处理流水线

此示例演示了如何创建一个带有条件分支的线性流水线。工作流接收一个初始值，根据该值是否大于10来选择不同的处理路径，最后对结果进行统一的后续处理。

```python
from core.runnables import SimpleTask, ExecutionContext, Runnable, NO_INPUT

# 1. 定义任务函数
def get_initial_value_input(start_val: int = 2) -> int: # 允许外部传入，默认为2
    print(f"Executing: get_initial_value_input, returning {start_val}")
    return start_val

def process_if_large(val: int) -> str:
    print(f"Executing: process_if_large with {val}")
    return str(val * 10)

def process_if_small(val: int) -> str:
    print(f"Executing: process_if_small with {val}")
    return str(val + 5)

def add_three_to_result(val_str: str) -> str:
    print(f"Executing: add_three_to_result with {val_str}")
    return str(int(val_str) + 3)

# 2. 定义条件 Runnable
class IsValueGreaterThanTen(Runnable):
    def _internal_invoke(self, input_data: int, context: ExecutionContext) -> int:
        # 这个 invoke 的结果会传递给 check 方法，也会传递给 true/false 分支
        print(f"Condition Check (IsValueGreaterThanTen): Input is {input_data}")
        return input_data

    def _default_check(self, data_from_invoke: int) -> bool:
        is_greater = data_from_invoke > 10
        print(f"Condition Check (IsValueGreaterThanTen): {data_from_invoke} > 10 is {is_greater}")
        return is_greater

# 3. 创建 Runnable 实例
task_get_initial = SimpleTask(get_initial_value_input, name="GetInitialValue")
condition_checker = IsValueGreaterThanTen(name="ValueIsGreaterThanTen")
task_process_large = SimpleTask(process_if_large, name="ProcessIfLarge")
task_process_small = SimpleTask(process_if_small, name="ProcessIfSmall")
task_add_three = SimpleTask(add_three_to_result, name="AddThreeToResult")

# 4. 构建工作流
# Workflow: GetInitialValue -> (ValueIsGreaterThanTen % ProcessIfLarge >> ProcessIfSmall) -> AddThreeToResult
conditional_pipeline = task_get_initial | \
                       (condition_checker % task_process_large >> task_process_small) | \
                       task_add_three

# 5. 执行
print("--- Example 1: Conditional Pipeline ---")
ctx1_val2 = ExecutionContext()
# task_get_initial 会使用默认值2，或者你可以传递一个值给 conditional_pipeline.invoke
result1_val2 = conditional_pipeline.invoke(2, ctx1_val2) # 直接给整个pipeline输入2
print(f"Result for Example 1 (Input 2): {result1_val2}")
# 预期: Result for Example 1 (Input 2): 10

ctx1_val12 = ExecutionContext()
result1_val12 = conditional_pipeline.invoke(12, ctx1_val12) # 输入12
print(f"Result for Example 1 (Input 12): {result1_val12}")
# 预期: Result for Example 1 (Input 12): 123
```

### 示例 2: 并行处理与结果聚合

此示例展示了如何并行执行两个（或多个）独立的工作流或任务，然后将它们的输出聚合起来进行后续处理。这里我们使用 `SourceParallel` 来启动并行分支，每个分支都从相同的初始输入开始（或者像这里一样，它们是自包含的源）。

```python
from core.runnables import SimpleTask, ExecutionContext, NO_INPUT, SourceParallel

# (假设前面示例中的函数和类已定义，或在此处重新定义简化版本)
# For brevity, let's define simplified work1_like and work2_like tasks
def source_task_alpha() -> str:
    print("Executing: source_task_alpha")
    return "alpha_output"

def source_task_beta() -> str:
    print("Executing: source_task_beta")
    return "beta_output"

task_alpha = SimpleTask(source_task_alpha, name="SourceAlpha")
task_beta = SimpleTask(source_task_beta, name="SourceBeta")

# 定义聚合函数，它将接收一个字典
def combine_parallel_outputs(**kwargs: str) -> str:
    alpha = kwargs.get("alpha_key", "N/A")
    beta = kwargs.get("beta_key", "N/A")
    print(f"Combining: alpha='{alpha}', beta='{beta}'")
    return f"Combined Alpha: {alpha}, Combined Beta: {beta}"

task_combiner = SimpleTask(combine_parallel_outputs, name="CombineOutputs")

# 构建工作流
# SourceParallel 执行 task_alpha 和 task_beta
# 其输出是 {"alpha_key": "alpha_output", "beta_key": "beta_output"}
# 这个字典传递给 task_combiner
parallel_then_combine_flow = SourceParallel({
    "alpha_key": task_alpha,
    "beta_key": task_beta
}) | task_combiner

# 执行
print("\n--- Example 2: Parallel Execution and Aggregation ---")
ctx2 = ExecutionContext()
# SourceParallel 中的任务是源，所以整体流程可以从 NO_INPUT 开始
result2 = parallel_then_combine_flow.invoke(NO_INPUT, ctx2)
print(f"Result for Example 2: {result2}")
# 预期:
# Executing: source_task_alpha
# Executing: source_task_beta
# (顺序可能不同)
# Combining: alpha='alpha_output', beta='beta_output'
# Result for Example 2: Combined Alpha: alpha_output, Combined Beta: beta_output
```

### 示例 3: 扇出处理与后续链式操作 (A | {B,C} | D)

此示例演示了如何将一个任务(A)的输出扇出到多个并行任务(B,C)，然后将这些并行任务的聚合结果（一个字典）传递给下一个处理步骤(D)。

```python
from core.runnables import SimpleTask, ExecutionContext, NO_INPUT # SourceParallel (if needed for A)

# 1. 定义初始任务 (A)
def get_base_data_for_fan_out() -> str:
    print("Executing: get_base_data_for_fan_out")
    return "100" # 假设输出 "100"
task_A_main_source = SimpleTask(get_base_data_for_fan_out, name="MainSourceA")

# 2. 定义并行分支任务 (B, C)
def multiply_by_five(val_str: str) -> str:
    print(f"Executing: multiply_by_five with {val_str}")
    return str(int(val_str) * 5)

def subtract_twenty(val_str: str) -> str:
    print(f"Executing: subtract_twenty with {val_str}")
    return str(int(val_str) - 20)

task_B_multiplier = SimpleTask(multiply_by_five, name="BranchB_Multiplier")
task_C_subtractor = SimpleTask(subtract_twenty, name="BranchC_Subtractor")

# 3. 定义最终处理任务 (D)
# D 会接收 {"res_B": output_of_task_B, "res_C": output_of_task_C}
def create_final_report(**kwargs: str) -> str:
    multiplied_val = kwargs.get("res_B", "N/A")
    subtracted_val = kwargs.get("res_C", "N/A")
    print(f"Creating final report from: multiplied='{multiplied_val}', subtracted='{subtracted_val}'")
    return f"Report: Value*5 is {multiplied_val}, Value-20 is {subtracted_val}."

task_D_reporter = SimpleTask(create_final_report, name="FinalReporterD")

# 4. 构建 A | {"key_B": B, "key_C": C} | D 工作流
# task_A_main_source 的输出 ("100") 会传递给 task_B_multiplier 和 task_C_subtractor
# 它们的结果 {"res_B": "500", "res_C": "80"} 会传递给 task_D_reporter
pipeline_A_fan_out_D = task_A_main_source | \
                       {"res_B": task_B_multiplier, "res_C": task_C_subtractor} | \
                       task_D_reporter

# 5. 执行
print("\n--- Example 3: Fan-out and Chained Processing (A | {B,C} | D) ---")
ctx3 = ExecutionContext()
# task_A_main_source 是源，所以整体流程可以从 NO_INPUT 开始
result3 = pipeline_A_fan_out_D.invoke(NO_INPUT, ctx3)
print(f"Result for Example 3: {result3}")
# 预期输出:
# Executing: get_base_data_for_fan_out
# Executing: multiply_by_five with 100
# Executing: subtract_twenty with 100
# (B,C 顺序可能不同)
# Creating final report from: multiplied='500', subtracted='80'
# Result for Example 3: Report: Value*5 is 500, Value-20 is 80.