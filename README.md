# TaskPipe: A Composable Python Workflow Framework

TaskPipe is a Python framework for building, composing, and executing complex workflows with a focus on clear, runnable components and intuitive pipeline definition. It allows you to define individual tasks as `Runnable` objects and chain them together using operator overloading to create linear pipelines, conditional branches, and parallel execution paths.

## Key Features

*   **Composable `Runnable` Tasks**: Define each step of your workflow as a `Runnable`. The framework provides base classes and several pre-built `Runnable` types like `SimpleTask`, `Pipeline`, `Conditional`, `SourceParallel`, `BranchAndFanIn`, etc.
*   **Intuitive Operator Overloading**:
    *   `task_a | task_b`: Create a sequential pipeline.
    *   `condition_task % true_task >> false_task`: Define conditional logic.
    *   `source_task | {"branch1": task_b, "branch2": task_c}`: Fan-out a task's output to multiple parallel branches.
    *   `SourceParallel({"branch1": task_a, "branch2": task_b})`: Run multiple tasks or pipelines in parallel from a common (or no) input.
*   **Execution Context**: A shared `ExecutionContext` flows through your workflow, allowing tasks to access outputs from previously executed tasks.
*   **Caching**: Built-in support for caching results of `Runnable` invocations to speed up repeated executions.
*   **Error Handling & Retries**: Configure error handlers and retry mechanisms for your `Runnable` tasks.
*   **Graph-Based Definition (Optional)**: For more complex or visually managed workflows, use `WorkflowGraph` to define nodes and edges, then compile it into an executable `CompiledGraph`.
*   **Extensible**: Easily create your own custom `Runnable` types to encapsulate specific logic.

## Directory Structure

```
.
├── core/
│   ├── __init__.py
│   ├── graph.py       # WorkflowGraph and CompiledGraph for graph-based workflows
│   └── runnables.py   # Runnable base class, concrete implementations, ExecutionContext
├── test.py            # Example usage of the Runnable framework (operator-based)
├── test_graph.py      # Unit tests for graph.py
├── test_runnables.py  # Unit tests for runnables.py
├── API_DOCUMENTATION.md # Detailed API documentation and Quick Start examples
├── README.md          # This file
└── .gitignore         # Git ignore file (to be created)
```

## Quick Start

The following example demonstrates a conditional pipeline:

```python
from core.runnables import SimpleTask, ExecutionContext, Runnable, NO_INPUT

# 1. Define task functions
def get_initial_value(start_val: int = 2) -> int:
    print(f"Input: {start_val}")
    return start_val

def process_large(val: int) -> str:
    return f"Processed large: {val * 10}"

def process_small(val: int) -> str:
    return f"Processed small: {val + 5}"

def finalize_result(val_str: str) -> str:
    return f"Final: {int(val_str) + 3}"

# 2. Define a conditional Runnable
class CheckIfGreaterThanTen(Runnable):
    def _internal_invoke(self, input_data: int, context: ExecutionContext) -> int:
        return input_data # Pass through for check method and to branches
    def _default_check(self, data_from_invoke: int) -> bool:
        return data_from_invoke > 10

# 3. Create Runnable instances
task_initial = SimpleTask(get_initial_value, name="Initial")
task_condition = CheckIfGreaterThanTen(name="Condition")
task_large = SimpleTask(process_large, name="LargeBranch")
task_small = SimpleTask(process_small, name="SmallBranch")
task_finalize = SimpleTask(finalize_result, name="Finalize")

# 4. Build workflow using operator overloading
# Initial -> (Condition % LargeBranch >> SmallBranch) -> Finalize
workflow = task_initial | (task_condition % task_large >> task_small) | task_finalize

# 5. Execute
ctx = ExecutionContext()
result_for_2 = workflow.invoke(2, ctx) # Input 2 for the whole workflow
print(f"Workflow result for 2: {result_for_2}") # Expected: Final: 10

ctx_for_12 = ExecutionContext() # Use a new context for a new independent run
result_for_12 = workflow.invoke(12, ctx_for_12) # Input 12 for the whole workflow
print(f"Workflow result for 12: {result_for_12}") # Expected: Final: 123
```

For more detailed examples, including parallel execution (`SourceParallel`), fan-out (`BranchAndFanIn`), and chained complex pipelines, please see the [API_DOCUMENTATION.md](API_DOCUMENTATION.md).

## Running Tests

The project includes unit tests for the core components:
```bash
python test_runnables.py
python test_graph.py
```
You can also run `python test.py` to see various workflow examples in action.

## API Documentation

For a comprehensive guide to all classes, methods, and advanced usage patterns, please refer to [API_DOCUMENTATION.md](API_DOCUMENTATION.md).