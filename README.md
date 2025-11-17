# TaskPipe: 一个可组合的 Python 工作流框架

TaskPipe 是一个 Python 框架，用于构建、组合和执行复杂的工作流。它专注于清晰、可运行的组件和直관적인流水线定义，并内置了对同步和异步任务混合编排的支持。通过 TaskPipe，开发者可以轻松定义、链接和管理数据处理的各个阶段，无论是简单的线性序列还是复杂的分支与并行结构。

## 核心特性

* **可组合的 `Runnable` 任务**: 将工作流的每一步定义为 `Runnable` 的子类。框架提供了 `Runnable` (同步基类) 和 `AsyncRunnable` (异步基类)，以及多种预构建的 `Runnable` 类型，如 `SimpleTask`、`Pipeline`、`Conditional`、`BranchAndFanIn`、`SourceParallel` 及其对应的异步版本。
* **同步与异步的无缝集成**:
    * 可以自由地在工作流中混合使用同步 (`Runnable`) 和异步 (`AsyncRunnable`) 任务。
    * 当组合中包含任何异步任务时，整个工作流通常会自动升级为异步工作流 (例如，`AsyncPipeline`, `AsyncConditional`)，以确保非阻塞执行。
    * 在异步工作流中，同步任务的 `invoke` 方法会自动通过其 `invoke_async` 包装（通常在线程池中执行），从而避免阻塞 `asyncio` 事件循环。
* **直观的操作符重载**:
    * `task_a | task_b`: 创建顺序流水线。如果任一任务是异步的，则结果通常是 `AsyncPipeline`。
    * `condition_task % true_task >> false_task`: 定义条件逻辑。如果条件任务或任一分支是异步的，则结果通常是 `AsyncConditional`。
    * `source_task | {"branch1": task_b, "branch2": task_c}`: 将 `source_task` 的输出扇出到多个并行分支 `task_b` 和 `task_c`。结果是 `BranchAndFanIn` (或 `AsyncBranchAndFanIn`) 作为流水线的下一步。
    * `SourceParallel({"branch1": task_a, "branch2": task_b})`: 从共同的（或无）输入并行运行多个任务或流水线。
* **执行上下文 (`ExecutionContext`)**: `ExecutionContext` 现在是一个协议（接口），默认实现 `InMemoryExecutionContext` 在工作流执行期间流动，允许任务之间共享数据、访问先前执行任务的输出，并记录事件。你也可以实现 `RedisExecutionContext` / `DatabaseExecutionContext` 以满足持久化与分布式需求。
* **Pydantic 数据契约**：可为任意 `Runnable` 声明 `InputModel` / `OutputModel` / `ConfigModel`（Pydantic 模型）。当声明后，taskpipe 会在执行前自动校验输入、在执行后校验输出，并将配置 (`config`) 序列化为结构化数据——这让复杂的分支/并行工作流更安全。
* **节点状态回调**：`ExecutionContext.notify_status`（默认由 `InMemoryExecutionContext` 提供）会收到 “start / success / failed” 事件，便于 UI 或监控订阅节点状态。
* **AgentLoop / 动态 Agent**: 在 `AsyncRunnable` 家族中新增 `AgentLoop`，可封装经典的 ReAct/Tool-Calling 循环：生成器 `Runnable` 可以动态返回“下一步任务”并在事件循环中继续执行，直到返回最终结果。
* **缓存**: 内置支持对 `Runnable` 的 `invoke`/`invoke_async` 和 `check`/`check_async` 方法的结果进行缓存，以加速重复执行和条件判断。
* **错误处理与重试**:
    * 可以为每个 `Runnable` 配置错误处理器和重试机制。
    * `on_error` 方法现在更加灵活，不仅可以接受一个 `Runnable`作为错误处理器，还可以接受一个普通的可调用函数（例如 lambda 或自定义函数），使得轻量级的错误处理更为便捷。
* **生命周期钩子**:
    * 新增 `on_start` 和 `on_complete` 生命周期钩子，允许用户在任务执行的关键阶段（开始前和完成后）注入自定义逻辑。
    * 这些钩子（包括增强后的 `on_error`）都可以接受 `Runnable` 实例或普通可调用函数作为处理器，为日志记录、资源管理、监控和通知等场景提供了极大的灵活性。
* **基于图的定义 (可选)**: 对于更复杂或需要集中管理的工作流，可以使用 `WorkflowGraph` 以声明方式定义节点和边，然后将其编译为可执行的 `CompiledGraph`。`CompiledGraph` 同样支持同步和异步执行，并且在异步执行时会按照自动计算的“执行阶段”并发运行同一阶段中互不依赖的节点。
* **RunnableRegistry**： taskpipe 内置轻量级注册表，应用层可以集中注册可用的 `Runnable`，`WorkflowGraph.from_json` 可以直接使用该注册表来反序列化节点。
* **图 ↔ 代码 双向转换**: 任意 `Runnable`（例如通过 `|` 操作符构建的“PyTorch 风格”流水线）都可以调用 `.to_graph()` 导出为 `WorkflowGraph`，而 `WorkflowGraph.from_json()` / `WorkflowGraph.to_json()` 允许平台化场景进行持久化、可视化与低代码集成。示例脚本会将导出的 JSON 保存到 `examples/registry_workflow.json`，便于直接加载或上传。
* **可扩展性**: 用户可以轻松创建自己的、继承自 `Runnable` 或 `AsyncRunnable` 的自定义任务类型，以封装特定的业务逻辑。
* **明确的调用约定**:
    * 当从**同步代码**中调用工作流或 `Runnable` 时，使用其 `.invoke()` 方法。如果 `Runnable` 是异步的，其 `.invoke()` 方法内部通常会使用 `asyncio.run()` 来执行异步逻辑（注意：这不能在已运行的事件循环中调用）。
    * 当从**异步代码** (例如，在 `async def` 函数内部) 调用工作流或 `Runnable` 时，**强烈推荐**使用 `await .invoke_async()` 方法。这能确保异步操作在当前事件循环中正确执行，并对同步 `Runnable` 进行适当的非阻塞封装。在异步代码中对异步组件使用同步的 `.invoke()` 会导致 `RuntimeError`。

## 目录结构

```

.
├── taskpipe/
│   ├── **init**.py         \# 包初始化，提升常用类到包级别
│   ├── runnables.py      \# Runnable 基类, 同步实现, ExecutionContext
│   ├── async\_runnables.py \# AsyncRunnable 基类, 异步组合器
│   └── graph.py          \# WorkflowGraph 和 CompiledGraph
├── tests/                  \# 单元测试
│   ├── test\_runnables.py
│   ├── test\_async\_runnables.py
│   └── test\_graph.py
├── examples/               \# 使用示例
│   └── test.py             \# (或其他示例文件)
├── setup.py                \# 项目安装配置
└── README.md               \# 本文件

````

## 安装

在你的项目根目录下 (包含 `setup.py` 的目录)，建议使用以下命令以可编辑模式安装 TaskPipe，这对于开发和测试非常方便：

```bash
pip install -e .
````

这将把 `taskpipe` 包链接到你的 Python 环境中，使其可以像其他已安装的库一样被导入，同时你对源代码的任何修改都会立即生效。

## Pydantic Schema 示例

```python
from pydantic import BaseModel
from taskpipe import Runnable

class EmailInput(BaseModel):
    subject: str
    body: str

class EmailOutput(BaseModel):
    status: str

class EmailConfig(BaseModel):
    smtp_server: str

class SendEmail(Runnable):
    InputModel = EmailInput
    OutputModel = EmailOutput
    ConfigModel = EmailConfig

    def _internal_invoke(self, input_data: EmailInput, context):
        # input_data 已经是 Pydantic 对象
        ...  # 实际发送邮件
        return {"status": "sent"}

task = SendEmail(config={"smtp_server": "smtp.example.com"})
result = task.invoke({"subject": "Hi", "body": "..."})
assert result.status == "sent"
```

## 示例 (`examples/` 目录)

- `pipeline_with_schema.py`：类型安全的指标评估流水线，展示 `InputModel`/`OutputModel`/`ConfigModel` 与节点状态事件。
- `parallel_report_workflow.py`：使用 `WorkflowGraph` 构建带并行阶段的异步日报任务，展示 `CompiledGraph.execution_stages`。
- `registry_serialization.py`：演示如何用 `RunnableRegistry` 导出/导入图定义 (`registry_workflow.json`) 并再次执行。

## TaskPipe 的适用场景与边界

TaskPipe 主要设计用于编排具有明确定义的阶段、输入和输出的**数据处理流水线和任务序列**。它非常适合以下场景：

  * ETL (提取、转换、加载) 过程。
  * 复杂的数据分析和报告生成。
  * 机器学习模型的训练和推理流水线。
  * 批处理任务，包括同步和异步I/O密集型操作。
  * 任何可以分解为一系列可组合步骤的操作，这些步骤可能是同步的、异步的，或者是两者混合的。

**边界考虑**：

对于需要**复杂实时外部事件监听和长时状态管理**的场景（例如，持续的UI交互、由键盘/鼠标事件驱动的长时间运行状态如实时视频录制控制），TaskPipe 本身不直接提供事件监听的基础设施。这类场景通常需要专门的事件处理循环和状态机逻辑。

在这种情况下，推荐的做法是：

1.  使用专门的库（例如，`pynput` 或 `keyboard` 用于全局键盘事件监听，GUI 框架如 `PyQt` 或 `Tkinter` 用于图形界面事件，或者网络服务器框架如 `FastAPI` 或 `Flask` 处理网络请求）来处理外部事件的监听和初步响应。这些模块通常有自己的事件循环或线程模型。
2.  当这些外部事件指示一个明确的、可以由 TaskPipe 处理的数据处理阶段开始时（例如，视频录制完成并保存了临时文件，或者接收到了一个需要复杂处理的API请求），**由该外部模块触发一个 TaskPipe 工作流**。此时，可以将必要的上下文（如文件路径、用户ID、请求数据等）作为输入传递给 TaskPipe 工作流的 `invoke()` 或 `await invoke_async()` 方法。
3.  TaskPipe 工作流随后负责后续的所有结构化处理步骤（例如，数据校验、格式转换、特征提取、模型推理、结果聚合、数据存储、发送通知等）。

这种关注点分离的方法可以保持 TaskPipe 框架的核心功能（任务编排和数据流管理）的简洁性和高效性，同时使其能够与事件驱动的系统和更广泛的应用场景良好集成。

## 运行测试

项目包含核心组件的单元测试。在项目根目录下，首先确保你已经通过 `pip install -e .` 安装了项目依赖（如果有测试相关的依赖，也需要安装，例如 `pip install -e ".[test]"`，前提是 `setup.py` 中定义了 `extras_require={'test': [...]}`）。

然后运行测试：

```bash
# 运行所有测试 (如果测试文件名符合 discover 模式，如 test_*.py)
python -m unittest discover tests

# 或者单独运行每个测试文件
python -m unittest tests.test_runnables
python -m unittest tests.test_async_runnables
python -m unittest tests.test_graph
```

如果你使用 `pytest`，通常可以直接在项目根目录运行 `pytest`。

## API 文档

有关所有类、方法和高级使用模式的全面指南，请参阅 [API\_DOCUMENTATION.md](API_DOCUMENTATION.md)。

```
