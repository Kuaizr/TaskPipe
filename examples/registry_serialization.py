"""
Example: Serialization with START/END nodes.
"""
import json
from pathlib import Path
from pydantic import BaseModel
from taskpipe import RunnableRegistry, WorkflowGraph, task, START, END, NO_INPUT

EXAMPLE_JSON = Path(__file__).with_name("registry_workflow.json")

class MetricInput(BaseModel):
    cpu: float; ram: float
class ResultPayload(BaseModel):
    text: str

@task
def normalize(cpu: float, ram: float) -> MetricInput:
    return MetricInput(cpu=round(cpu, 2), ram=round(ram, 2))

@task
def persist(cpu: float, ram: float) -> ResultPayload:
    return ResultPayload(text=f"Saved: CPU={cpu}, RAM={ram}")

def build_and_export():
    graph = WorkflowGraph(name="MetricFlow")
    
    # Explicit Contract
    graph.add_node(START(MetricInput), "Start")
    graph.add_node(normalize(), "Normalize")
    graph.add_node(persist(), "Persist")
    graph.add_node(END(ResultPayload), "End")
    
    graph.add_edge("Start", "Normalize", data_mapping={"cpu": "cpu", "ram": "ram"})
    graph.add_edge("Normalize", "Persist", data_mapping={"cpu": "cpu", "ram": "ram"})
    graph.add_edge("Persist", "End", data_mapping={"text": "text"})
    
    data = graph.to_json()
    with EXAMPLE_JSON.open("w") as f:
        json.dump(data, f, indent=2)
    print(f"Exported to {EXAMPLE_JSON}")

def load_and_run():
    registry = RunnableRegistry()
    registry.register("Normalize", normalize)
    registry.register("Persist", persist)
    # Need to register START/END logic or handle them?
    # In practice, START/END are generic. 
    # For this example, we assume we reconstruct them or the Registry handles generic types.
    # To make the example simple, we'll just show the graph structure.
    
    # Note: Loading START/END via registry requires registering factories for them
    # that accept the specific Schema. This is advanced usage.
    # For now, we demonstrate export structure.
    pass

if __name__ == "__main__":
    build_and_export()