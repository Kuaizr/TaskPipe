# TaskPipe: 一个可组合、生产级的 Python 工作流引擎

TaskPipe 是一个用于构建、组合和执行复杂工作流的 Python 框架。本次重构的重点在于实现**UI 友好、逻辑完备**的架构，完美支持低代码平台（如 Aether）的集成和高级 Agent 模式。

框架的核心是 **“显式契约”** 和 **“配置驱动的控制流”**。

## 核心特性 (v1.0 架构)

* **架构基石：显式原语 (The Primitives)**：
    * **START / END**：用于定义任何工作流（或子图）的输入/输出契约边界。
    * **Switch**：通用的多路分支控制。逻辑通过配置（表达式字符串）驱动，取代了复杂的 `% >>` 运算符。
    * **Loop**：通用的循环容器。替代了旧的 `While` 和 `AgentLoop`，支持配置驱动的迭代停止。
    * **Map**：动态并行容器。基于输入列表的长度（N）动态分裂出 N 个任务并行执行（Plan-and-Execute 模式的关键）。

* **状态与交互 (HITL & Pause/Resume)**：
    * **暂停/恢复 (Pause/Resume)**：原生支持。任何节点（如 `WaitForInput`）可抛出 `SuspendExecution` 异常请求暂停。顶层 `invoke` 返回完整可序列化的状态快照，供应用层持久化和恢复。
    * **`WaitForInput`**：专用的交互节点，用于在流程中创建断点，等待人工确认或输入。
    * **健壮性**：`CompiledGraph` 实现了状态快照和恢复时的上下文回填机制，防止数据丢失。

* **可组合性与数据流 (`|` & `map_inputs`)**：
    * `task_a | task_b`：创建顺序流水线。如果包含异步任务，自动升级为 `AsyncPipeline`。
    * `map_inputs`：支持深层字段访问 (`parent.Output.data.id`) 和跨节点数据传输，实现了代码模式下最高效的数据描述。
    * **强类型契约**：使用 Pydantic 模型确保所有数据流都是类型安全的，且兼容 Pydantic V1/V2。

* **图 ↔ 代码 同构**：
    * 代码中用 `START | Task | Switch | { ... } | END` 编写的 Pipeline，其结构与 `WorkflowGraph` 完全一致。
    * 任何组合好的 `Runnable` 都可以调用 `.to_graph()` 导出，供可视化平台使用。

---

## 目录结构 (精简版)

.├── taskpipe/│   ├── init.py         # 包初始化，导出所有通用原语│   ├── runnables.py      # Runnable 基类, 同步实现, 核心控制流逻辑│   ├── async_runnables.py # AsyncRunnable 基类, 异步组合器│   └── graph.py          # WorkflowGraph, CompiledGraph (执行引擎)├── tests/                  # 单元测试 (已适配新架构)└── setup.py                # 项目安装配置
## 安装与使用约定

### 安装

在你的项目根目录下，使用以下命令安装：

```bash
pip install -e .
关键调用约定 (HITL)为了支持暂停和恢复，外部应用层必须处理 SuspendExecution 异常。动作TaskPipe API行为首次启动workflow.invoke(input)正常执行，直到遇到 WaitForInput。暂停发生invoke() 抛出 SuspendExecution(snapshot=state)应用程序必须捕获此异常，并持久化 state。恢复执行workflow.invoke(resume_state=saved_state)从保存的状态处精准恢复执行，并继续处理 WaitForInput 的输入。