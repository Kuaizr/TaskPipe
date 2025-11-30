"""
Example: Logic branching using Switch node.
Run with: `python examples/conditional_workflow.py`
"""

from __future__ import annotations
from pydantic import BaseModel
from taskpipe import WorkflowGraph, Switch, task, START, END

class NumberData(BaseModel):
    val: int

@task
def double_number(val: int) -> int:
    print(f"  -> Doubling {val}")
    return val * 2

@task
def triple_number(val: int) -> int:
    print(f"  -> Tripling {val}")
    return val * 3

def main() -> None:
    print("=" * 60)
    print("Example: Switch Node Logic")
    print("=" * 60)
    
    graph = WorkflowGraph(name="SwitchExample")
    start = START(NumberData)
    
    switch = Switch(config={
        "rules": [("val % 2 == 0", "EVEN")],
        "default_branch": "ODD"
    })
    
    branch_even = double_number()
    branch_odd = triple_number()
    end = END(NumberData)
    
    graph.add_node(start, "Start")
    graph.add_node(switch, "CheckParity")
    graph.add_node(branch_even, "EvenBranch")
    graph.add_node(branch_odd, "OddBranch")
    graph.add_node(end, "End")
    
    # 1. Start -> Switch (判断数据)
    graph.add_edge("Start", "CheckParity", data_mapping={"val": "val"})
    
    # 2. Switch -> Branches (仅控制流，无数据映射!)
    graph.add_edge("CheckParity", "EvenBranch", branch="EVEN")
    graph.add_edge("CheckParity", "OddBranch", branch="ODD")
    
    # 3. Start -> Branches (真实数据流)
    graph.add_edge("Start", "EvenBranch", data_mapping={"val": "val"})
    graph.add_edge("Start", "OddBranch", data_mapping={"val": "val"})
    
    # 4. Branches -> End
    graph.add_edge("EvenBranch", "End", data_mapping={"val": "result"})
    graph.add_edge("OddBranch", "End", data_mapping={"val": "result"})
    
    compiled = graph.compile()
    
    print("\nRunning with 10 (Even):")
    res1 = compiled.invoke({"val": 10})
    val1 = res1.val if hasattr(res1, "val") else res1
    print(f"Result: {val1} (Expected 20)")
    
    print("\nRunning with 11 (Odd):")
    res2 = compiled.invoke({"val": 11})
    val2 = res2.val if hasattr(res2, "val") else res2
    print(f"Result: {val2} (Expected 33)")

if __name__ == "__main__":
    main()