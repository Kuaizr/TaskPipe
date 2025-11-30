# TaskPipe 核心架构与 API 文档 (最终版)

本文档描述 TaskPipe 的最终架构，该架构旨在支持低代码平台所需的 **显式契约** 和 **暂停/恢复** 能力。

## 1. 架构核心原语 (Primitives)

| 类名 | 继承 | 核心作用 | 备注 |
| :--- | :--- | :--- | :--- |
| **START** | `Runnable` | **契约入口**。定义工作流接收的输入 Schema。 | 任何子图的第一个节点。 |
| **END** | `Runnable` | **契约出口**。定义工作流返回的输出 Schema。 | 任何子图的最后一个节点。 |
| **Switch** | `Runnable` | **多路分支控制**。根据 `config` 中的表达式字符串（`"score > 0.5"`）输出 `decision` 字段，供 Graph 路由。 | 替换旧的 `Router` 和 `Conditional`。 |
| **Loop** | `Runnable` | **循环容器**。重复执行 `body`，直到 `config.condition` 不满足。支持 `await` 内部的异步任务。 | 替代 `While` 和 `AgentLoop`。 |
| **Map** | `Runnable` | **动态并行**。根据输入列表的长度，动态分裂并并行执行 `body` 任务。 | 用于 Plan-and-Execute 等动态并发场景。 |
| **WaitForInput** | `Runnable` | **暂停/交互点**。执行时抛出 `SuspendExecution`，等待外部系统通过 `resume_state` 注入数据后恢复。 | 实现 Human-in-the-Loop。 |

---

## 2. 核心类与方法 (Runnable & Context)

### `taskpipe.runnables.Runnable`

所有任务的基类，提供生命周期管理、缓存、重试和数据流处理。

| 方法/属性 | 签名/类型 | 描述 |
| :--- | :--- | :--- |
| `invoke` | `(input, context, resume_state=None)` | **同步**执行。核心入口，处理 InputModel 校验和错误。 |
| `invoke_async` | `(input, context, resume_state=None)` | **异步**执行。对于同步任务，在线程池中执行 `invoke`。 |
| `map_inputs` | `(self, **mappings)` | 声明字段级映射，支持深层访问（`parent.Output.data.id`）。 |
| `Output` | `_OutputAccessor` | 属性访问器，用于 `map_inputs` 的源头引用。 |
| `_resolve_inputs_from_bindings` | `(context)` | **关键**。根据 `map_inputs` 的配置从 Context 中取出数据。 |
| `_apply_input_model` | `(payload)` | Pydantic InputModel 验证（支持 V1/V2 兼容）。 |
| `on_error` | `(handler)` | 错误处理钩子。 |

### `taskpipe.graph.CompiledGraph`

工作流图的可执行对象，继承自 `Runnable`，实现了复杂的调度和状态管理。

| 方法 | 签名 | 描述 |
| :--- | :--- | :--- |
| `invoke` | `(input, context, resume_state=None)` | **图执行入口**。 |
| `resume` | `(self, resume_state: Dict, context: Optional[ExecutionContext] = None)` | **[新增 API]** 恢复被暂停的工作流。等同于 `invoke(resume_state=...)`。 |
| `resume_async` | `(self, resume_state: Dict, context=None)` | **[新增 API]** 异步恢复工作流。 |
| `_run_sync / _run_async` | `(state, ctx)` | **引擎核心**。包含拓扑排序和中断处理逻辑。 |

### `taskpipe.runnables.SuspendExecution` (关键异常)

```python
class SuspendExecution(Exception):
    def __init__(self, snapshot: Any = None):
        self.snapshot = snapshot
        super().__init__("Execution Suspended")
用途：当 WaitForInput 或任何容器节点（如 Loop, Pipeline）请求外部交互或状态持久化时，抛出此异常。

快照 (snapshot)：异常携带一个可序列化的字典，包含恢复现场所需的所有信息（例如 CompiledGraph 的执行队列、节点状态、上下文数据）。

3. 架构设计图 (概念关系)
代码段

graph TD
    subgraph Execution_Flow [核心执行流]
        direction LR
        A[START<br/>(Input Model)] --> B(Wait for Input/Task)
        B --> C{Switch<br/>(Expression Eval)}
        C -->|branch=TRUE| D[Task/Loop Body]
        C -->|branch=FALSE| E[Task/Tool]
        D --> F(END<br/>Output Model)
        E --> F
    end
    
    subgraph Container_Logic [控制流容器]
        Loop_C[Loop Container]
        Map_C[Map Container]
        Loop_C o-- D : executes
        Map_C o-- D : executes N times (Parallel)
    end
    
    subgraph Data_Control [数据与中断]
        CTX[(ExecutionContext)]
        SNAPSHOT[SuspendExecution<br/>(State Snapshot)]
        
        CTX -- "stream" --> UI[UI/Log/DB]
        Wait[WaitForInput] --> SNAPSHOT
        Loop_C --> SNAPSHOT
        
        Main_Exec(invoke/resume) --o CTX
        Main_Exec -->|catches| SNAPSHOT
        SNAPSHOT --> Main_Exec
    end
    
    A --> CTX
    
    classDef container fill:#f9f,stroke:#333;
    classDef contract fill:#add8e6,stroke:#333;
    classDef interrupt fill:#ff7f7f,stroke:#c00;
    class Loop_C,Map_C container;
    class A,F contract;
    class SNAPSHOT interrupt;