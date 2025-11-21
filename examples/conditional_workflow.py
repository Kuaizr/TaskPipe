"""
Example: Conditional workflows with Router and CheckAdapter.

This example demonstrates:
1. Conditional composition using % and >> operators
2. Router node for branch control
3. CheckAdapter for explicit condition checking
4. Conditional expansion to WorkflowGraph

Run with: `python examples/conditional_workflow.py`
"""

from __future__ import annotations

from pydantic import BaseModel

from taskpipe import InMemoryExecutionContext, WorkflowGraph, Router, task, NO_INPUT


class NumberOutput(BaseModel):
    result: int


class StringOutput(BaseModel):
    message: str


@task
def get_number() -> NumberOutput:
    """Generate a number for testing."""
    return NumberOutput(result=10)


@task
def get_odd_number() -> NumberOutput:
    """Generate an odd number for testing."""
    return NumberOutput(result=11)


@task
def process_number(val: int) -> NumberOutput:
    """Process a number (used as condition task)."""
    return NumberOutput(result=val)


@task
def double_number(val: int) -> NumberOutput:
    """Double the input number."""
    return NumberOutput(result=val * 2)


@task
def triple_number(val: int) -> NumberOutput:
    """Triple the input number."""
    return NumberOutput(result=val * 3)


def example_conditional_operator() -> None:
    """Example 1: Using % and >> operators for conditional logic."""
    print("=" * 60)
    print("Example 1: Conditional with % and >> operators")
    print("=" * 60)
    
    # Create a conditional workflow
    # The condition task processes the number and checks if it's even
    process_task = process_number()
    # Set check function: extract value from Pydantic model and check if even
    process_task.set_check(lambda x: (x.result if hasattr(x, 'result') else x) % 2 == 0)
    
    # Branches receive the output from process_task
    true_branch = double_number()
    false_branch = triple_number()
    
    # Build conditional: condition % true_branch >> false_branch
    conditional = process_task % true_branch >> false_branch
    
    # Connect source to conditional
    source = get_number()
    workflow = source | conditional
    
    ctx = InMemoryExecutionContext()
    result = workflow.invoke({}, ctx)
    
    print(f"Input number: 10 (even)")
    result_val = result.result if hasattr(result, 'result') else result
    print(f"Result: {result_val}")
    print(f"Expected: 20 (doubled)")
    
    # Test with odd number
    source_odd = get_odd_number()
    workflow2 = source_odd | conditional
    result2 = workflow2.invoke({}, InMemoryExecutionContext())
    result2_val = result2.result if hasattr(result2, 'result') else result2
    print(f"\nInput number: 11 (odd)")
    print(f"Result: {result2_val}")
    print(f"Expected: 33 (tripled)")


def example_conditional_graph_expansion() -> None:
    """Example 2: Conditional expansion to WorkflowGraph with Router."""
    print("\n" + "=" * 60)
    print("Example 2: Conditional expansion to WorkflowGraph")
    print("=" * 60)
    
    # Create conditional workflow
    process_task = process_number()
    process_task.set_check(lambda x: (x.result if hasattr(x, 'result') else x) > 5)
    
    @task
    def true_path(v: int) -> str:
        return "Big"
    
    @task
    def false_path(v: int) -> str:
        return "Small"
    
    conditional = process_task % true_path() >> false_path()
    source = get_number()
    workflow = source | conditional
    
    # Convert to graph to see the expansion
    graph = workflow.to_graph("ConditionalExample")
    
    print("Graph nodes:")
    for node_name, node in graph.nodes.items():
        node_type = type(node).__name__
        print(f"  {node_name}: {node_type}")
    
    print("\nGraph edges (showing Router and CheckAdapter):")
    for source_node, edges in graph.edges.items():
        for edge in edges:
            branch_info = f" (branch: {edge.branch})" if edge.branch else ""
            print(f"  {source_node} -> {edge.dest}{branch_info}")
    
    # Execute the compiled graph
    compiled = graph.compile()
    result = compiled.invoke(NO_INPUT)
    result_val = result if isinstance(result, str) else (result.result if hasattr(result, 'result') else result)
    print(f"\nCompiled graph result: {result_val} (10 > 5, should be 'Big')")


def example_workflow_graph_conditional() -> None:
    """Example 3: Building conditional workflow directly with WorkflowGraph."""
    print("\n" + "=" * 60)
    print("Example 3: Conditional workflow with WorkflowGraph")
    print("=" * 60)
    
    graph = WorkflowGraph(name="ManualConditional")
    
    source = get_number()
    
    @task
    def check_condition(value: int) -> bool:
        return value % 2 == 0
    
    @task
    def handle_true(value: int) -> NumberOutput:
        return NumberOutput(result=value * 2)
    
    @task
    def handle_false(value: int) -> NumberOutput:
        return NumberOutput(result=value * 3)
    
    condition_check = check_condition()
    true_branch = handle_true()
    false_branch = handle_false()
    
    router = Router()
    
    graph.add_node(source, "Source")
    graph.add_node(condition_check, "CheckEven")
    graph.add_node(router, "Router")
    graph.add_node(true_branch, "Double")
    graph.add_node(false_branch, "Triple")
    
    # Connect: Source -> CheckEven -> Router
    graph.add_edge("Source", "CheckEven", data_mapping={"value": "result"})
    graph.add_edge("CheckEven", "Router", data_mapping={"condition": "result"})
    
    # Connect: Source -> branches (data bypass)
    graph.add_edge("Source", "Double", data_mapping={"value": "result"})
    graph.add_edge("Source", "Triple", data_mapping={"value": "result"})
    
    # Connect: Router -> branches (control flow)
    graph.add_edge("Router", "Double", branch="true")
    graph.add_edge("Router", "Triple", branch="false")
    
    graph.set_entry_point("Source")
    graph.set_output_nodes(["Double", "Triple"])
    
    compiled = graph.compile()
    result = compiled.invoke(NO_INPUT)
    
    # Result will be a dict with output nodes
    if isinstance(result, dict):
        print(f"Result keys: {list(result.keys())}")
        if "Double" in result:
            result_val = result['Double'].result if hasattr(result['Double'], 'result') else result['Double']
            print(f"Double branch result: {result_val}")
        if "Triple" in result:
            result_val = result['Triple'].result if hasattr(result['Triple'], 'result') else result['Triple']
            print(f"Triple branch result: {result_val}")
    else:
        result_val = result.result if hasattr(result, 'result') else result
        print(f"Result: {result_val}")


def main() -> None:
    example_conditional_operator()
    example_conditional_graph_expansion()
    example_workflow_graph_conditional()


if __name__ == "__main__":
    main()
