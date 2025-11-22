"""
Example: Advanced TaskPipe features.

This example demonstrates:
1. Error handling with on_error
2. Retry mechanism
3. Lifecycle hooks (on_start, on_complete, on_error)
4. Caching
5. While loop
6. AgentLoop for dynamic agent workflows
7. BranchAndFanIn and SourceParallel
8. Garbage collection in CompiledGraph

Run with: `python examples/advanced_features.py`
"""

from __future__ import annotations

import asyncio
import random
from pydantic import BaseModel

from taskpipe import (
    AsyncRunnable,
    BranchAndFanIn,
    InMemoryExecutionContext,
    SourceParallel,
    While,
    task,
)


class NumberInput(BaseModel):
    value: int


class NumberOutput(BaseModel):
    result: int


class StatusOutput(BaseModel):
    status: str
    attempts: int


@task
def unreliable_task(value: int) -> NumberOutput:
    """A task that randomly fails to demonstrate error handling."""
    if random.random() < 0.5:  # 50% chance of failure
        raise ValueError(f"Random failure for value {value}")
    return NumberOutput(result=value * 2)


def error_handler(context, input_data, exception) -> StatusOutput:
    """Error handler that returns a status instead of raising."""
    # Note: error_handler should be a plain function, not a @task decorated function
    # The signature is: (context: ExecutionContext, input_data: Any, exception: Exception) -> Any
    return StatusOutput(
        status=f"Error handled: {type(exception).__name__}",
        attempts=1
    )


def example_error_handling() -> None:
    """Example 1: Error handling with on_error."""
    print("=" * 60)
    print("Example 1: Error Handling with on_error")
    print("=" * 60)
    
    # Create a task with error handler
    task_with_handler = unreliable_task().on_error(error_handler)
    
    ctx = InMemoryExecutionContext()
    
    # Try multiple times to see error handling in action
    for i in range(3):
        try:
            result = task_with_handler.invoke({"value": 10}, InMemoryExecutionContext())
            if hasattr(result, 'result'):
                print(f"Attempt {i+1}: Success! Result = {result.result}")
            else:
                print(f"Attempt {i+1}: Error handled! Status = {result.status}")
        except Exception as e:
            print(f"Attempt {i+1}: Unhandled error: {e}")


def example_retry() -> None:
    """Example 2: Retry mechanism."""
    print("\n" + "=" * 60)
    print("Example 2: Retry Mechanism")
    print("=" * 60)
    
    # Create a task with retry configuration
    retry_task = unreliable_task().retry(
        max_attempts=3,
        delay_seconds=0.1,
        retry_on_exceptions=(ValueError,)
    )
    
    ctx = InMemoryExecutionContext()
    
    # This should eventually succeed or fail after 3 attempts
    try:
        result = retry_task.invoke({"value": 5}, ctx)
        print(f"Success after retries! Result = {result.result}")
    except Exception as e:
        print(f"Failed after all retries: {e}")
    
    print(f"Total attempts logged: {len([e for e in ctx.event_log if 'attempt' in e.lower()])}")


def example_lifecycle_hooks() -> None:
    """Example 3: Lifecycle hooks (on_start, on_complete)."""
    print("\n" + "=" * 60)
    print("Example 3: Lifecycle Hooks")
    print("=" * 60)
    
    @task
    def simple_task(value: int) -> NumberOutput:
        return NumberOutput(result=value * 2)
    
    def on_start_hook(context, input_data):
        print(f"  [on_start] Task starting with input: {input_data}")
        context.log_event("Custom on_start hook executed")
    
    def on_complete_hook(context, result, exception):
        if exception:
            print(f"  [on_complete] Task failed with: {exception}")
        else:
            print(f"  [on_complete] Task completed with result: {result.result}")
        context.log_event("Custom on_complete hook executed")
    
    task_with_hooks = simple_task().set_on_start(on_start_hook).set_on_complete(on_complete_hook)
    
    ctx = InMemoryExecutionContext()
    result = task_with_hooks.invoke({"value": 7}, ctx)
    print(f"Final result: {result.result}")


def example_caching() -> None:
    """Example 4: Caching."""
    print("\n" + "=" * 60)
    print("Example 4: Caching")
    print("=" * 60)
    
    call_count = {"count": 0}
    
    @task
    def expensive_task(value: int) -> NumberOutput:
        call_count["count"] += 1
        print(f"  [expensive_task] Called {call_count['count']} time(s)")
        return NumberOutput(result=value * 10)
    
    # Enable caching
    cached_task = expensive_task(use_cache=True)
    
    ctx = InMemoryExecutionContext()
    
    # First call - should execute
    result1 = cached_task.invoke({"value": 5}, ctx)
    print(f"First call result: {result1.result}")
    
    # Second call with same input - should use cache
    result2 = cached_task.invoke({"value": 5}, ctx)
    print(f"Second call result: {result2.result} (should be from cache)")
    
    # Third call with different input - should execute again
    result3 = cached_task.invoke({"value": 6}, ctx)
    print(f"Third call result: {result3.result}")


def example_while_loop() -> None:
    """Example 5: While loop."""
    print("\n" + "=" * 60)
    print("Example 5: While Loop")
    print("=" * 60)
    
    @task
    def check_condition(last: int) -> NumberOutput:
        """Check if we should continue looping. Returns the value for checking."""
        return NumberOutput(result=last)
    
    # Set check function to determine if we should continue
    check_task = check_condition()
    check_task.set_check(lambda x: (x.result if hasattr(x, 'result') else x) < 10)
    
    @task
    def increment(last: int) -> NumberOutput:
        """Increment the value."""
        return NumberOutput(result=last + 1)
    
    # Create while loop: continue while condition is True
    loop = While(
        condition_check_runnable=check_task,
        body_runnable=increment(),
        max_loops=20,
        name="IncrementLoop"
    )
    
    ctx = InMemoryExecutionContext()
    result = loop.invoke({"last": 0}, ctx)
    # While loop returns a model with 'history' field containing all loop iterations
    if hasattr(result, 'history') and result.history:
        # Get the last iteration result
        last_result = result.history[-1]
        final_value = last_result.result if hasattr(last_result, 'result') else last_result
        print(f"Final value after loop: {final_value}")
        print(f"Expected: 10 (incremented from 0 to 10)")
        print(f"Total iterations: {len(result.history)}")
    else:
        print(f"Loop result: {result}")


def example_branch_and_fan_in() -> None:
    """Example 6: BranchAndFanIn for parallel execution."""
    print("\n" + "=" * 60)
    print("Example 6: BranchAndFanIn (Parallel Execution)")
    print("=" * 60)
    
    @task
    def process_a(value: int) -> NumberOutput:
        return NumberOutput(result=value * 2)
    
    @task
    def process_b(value: int) -> NumberOutput:
        return NumberOutput(result=value * 3)
    
    @task
    def process_c(value: int) -> NumberOutput:
        return NumberOutput(result=value * 4)
    
    # Fan out to multiple branches, then collect results
    branch_workflow = BranchAndFanIn({
        "branch_a": process_a(),
        "branch_b": process_b(),
        "branch_c": process_c(),
    }, name="ParallelProcessing")
    
    ctx = InMemoryExecutionContext()
    results = branch_workflow.invoke({"value": 5}, ctx)
    
    print("Results from parallel branches:")
    # BranchAndFanIn returns a Pydantic model with fields for each branch
    if hasattr(type(results), 'model_fields'):
        # It's a Pydantic model, access fields directly
        for branch_name in ["branch_a", "branch_b", "branch_c"]:
            if hasattr(results, branch_name):
                branch_result = getattr(results, branch_name)
                result_val = branch_result.result if hasattr(branch_result, 'result') else branch_result
                print(f"  {branch_name}: {result_val}")
    elif isinstance(results, dict):
        # It's a dict
        for branch, result in results.items():
            result_val = result.result if hasattr(result, 'result') else result
            print(f"  {branch}: {result_val}")
    else:
        print(f"  Unexpected result type: {type(results)}")


def example_source_parallel() -> None:
    """Example 7: SourceParallel for independent parallel tasks."""
    print("\n" + "=" * 60)
    print("Example 7: SourceParallel (Independent Parallel Tasks)")
    print("=" * 60)
    
    @task
    def task_a() -> NumberOutput:
        return NumberOutput(result=10)
    
    @task
    def task_b() -> NumberOutput:
        return NumberOutput(result=20)
    
    @task
    def task_c() -> NumberOutput:
        return NumberOutput(result=30)
    
    # Execute multiple independent tasks in parallel
    parallel_workflow = SourceParallel({
        "task_a": task_a(),
        "task_b": task_b(),
        "task_c": task_c(),
    }, name="IndependentParallel")
    
    ctx = InMemoryExecutionContext()
    results = parallel_workflow.invoke({}, ctx)
    
    print("Results from independent parallel tasks:")
    # SourceParallel returns a Pydantic model with fields for each task
    if hasattr(type(results), 'model_fields'):
        # It's a Pydantic model, access fields directly
        for task_name in ["task_a", "task_b", "task_c"]:
            if hasattr(results, task_name):
                task_result = getattr(results, task_name)
                result_val = task_result.result if hasattr(task_result, 'result') else task_result
                print(f"  {task_name}: {result_val}")
    elif isinstance(results, dict):
        # It's a dict
        for task_name, result in results.items():
            result_val = result.result if hasattr(result, 'result') else result
            print(f"  {task_name}: {result_val}")
    else:
        print(f"  Unexpected result type: {type(results)}")


async def example_agent_loop() -> None:
    """Example 8: AgentLoop for dynamic agent workflows."""
    print("\n" + "=" * 60)
    print("Example 8: AgentLoop (Dynamic Agent Workflow)")
    print("=" * 60)
    
    from taskpipe import AgentLoop
    
    iteration_count = {"count": 0}
    
    @task
    async def agent_generator(v: int):
        """Generator that decides next action dynamically."""
        iteration_count["count"] += 1
        current_iteration = iteration_count["count"]
        
        print(f"  [Generator] Iteration {current_iteration}, current value: {v}")
        
        if v >= 10:
            # Terminal result - return final value
            return {"final": v}
        
        # Return a Runnable to execute next
        @task
        async def increment_task(x: int) -> int:
            await asyncio.sleep(0.01)
            return x + 2
        
        return increment_task()
    
    agent = AgentLoop(
        generator=agent_generator(),
        max_iterations=10,
        name="DynamicAgent"
    )
    
    ctx = InMemoryExecutionContext()
    result = await agent.invoke_async({"v": 0}, ctx)
    
    print(f"Agent loop result: {result}")
    print(f"Total iterations: {iteration_count['count']}")


def example_garbage_collection() -> None:
    """Example 9: Garbage collection in CompiledGraph."""
    print("\n" + "=" * 60)
    print("Example 9: Garbage Collection")
    print("=" * 60)
    
    from taskpipe import WorkflowGraph
    
    @task
    def source() -> NumberOutput:
        return NumberOutput(result=100)
    
    @task
    def intermediate(value: int) -> NumberOutput:
        return NumberOutput(result=value * 2)
    
    @task
    def final(value: int) -> NumberOutput:
        return NumberOutput(result=value + 10)
    
    graph = WorkflowGraph(name="GCExample")
    graph.add_node(source(), "Source")
    graph.add_node(intermediate(), "Intermediate")
    graph.add_node(final(), "Final")
    
    graph.add_edge("Source", "Intermediate", data_mapping={"value": "result"})
    graph.add_edge("Intermediate", "Final", data_mapping={"value": "result"})
    
    graph.set_entry_point("Source")
    graph.set_output_nodes(["Final"])
    
    # Compile with GC enabled
    compiled_with_gc = graph.compile(enable_gc=True)
    ctx_gc = InMemoryExecutionContext()
    result_gc = compiled_with_gc.invoke({}, ctx_gc)
    
    result_gc_val = result_gc.result if hasattr(result_gc, 'result') else result_gc
    print(f"Result with GC enabled: {result_gc_val}")
    print(f"Context outputs (should only have Final): {list(ctx_gc.node_outputs.keys())}")
    
    # Compile without GC
    compiled_no_gc = graph.compile(enable_gc=False)
    ctx_no_gc = InMemoryExecutionContext()
    result_no_gc = compiled_no_gc.invoke({}, ctx_no_gc)
    
    result_no_gc_val = result_no_gc.result if hasattr(result_no_gc, 'result') else result_no_gc
    print(f"\nResult without GC: {result_no_gc_val}")
    print(f"Context outputs (should have all nodes): {list(ctx_no_gc.node_outputs.keys())}")


def main() -> None:
    example_error_handling()
    example_retry()
    example_lifecycle_hooks()
    example_caching()
    example_while_loop()
    example_branch_and_fan_in()
    example_source_parallel()
    
    # Run async example
    print()
    asyncio.run(example_agent_loop())
    
    example_garbage_collection()


if __name__ == "__main__":
    main()

