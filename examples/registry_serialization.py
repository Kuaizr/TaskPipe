"""
Example: WorkflowGraph serialization + RunnableRegistry.
Run with: `python examples/registry_serialization.py`
"""

from __future__ import annotations

import json
from pathlib import Path

from taskpipe import (
    InMemoryExecutionContext,
    NO_INPUT,
    RunnableRegistry,
    SimpleTask,
    WorkflowGraph,
)

EXAMPLE_JSON = Path(__file__).with_name("registry_workflow.json")


def build_graph() -> WorkflowGraph:
    graph = WorkflowGraph(name="MetricIngestion")
    collect = SimpleTask(lambda: {"cpu": 0.63, "ram": 0.41}, name="CollectMetrics")
    normalize = SimpleTask(lambda cpu, ram: {"cpu": round(cpu * 100, 1), "ram": round(ram * 100, 1)}, name="NormalizeMetrics")
    persist = SimpleTask(lambda cpu, ram: f"Recorded CPU={cpu}%, RAM={ram}%", name="PersistResult")

    graph.add_node(collect, "CollectMetrics")
    graph.add_node(normalize, "NormalizeMetrics")
    graph.add_node(persist, "PersistResult")

    graph.add_edge("CollectMetrics", "NormalizeMetrics")
    graph.add_edge("NormalizeMetrics", "PersistResult")
    graph.set_entry_point("CollectMetrics")
    graph.set_output_nodes(["PersistResult"])
    return graph


def export_graph(graph: WorkflowGraph) -> None:
    data = graph.to_json()
    with EXAMPLE_JSON.open("w", encoding="utf-8") as fp:
        json.dump(data, fp, indent=2, ensure_ascii=False)
    print(f"Graph definition exported to {EXAMPLE_JSON}")


def rebuild_via_registry() -> None:
    registry = RunnableRegistry()
    registry.register("CollectMetrics", lambda: SimpleTask(lambda: {"cpu": 0.5, "ram": 0.35}, name="CollectMetrics"))
    registry.register(
        "NormalizeMetrics",
        lambda: SimpleTask(lambda cpu, ram: {"cpu": round(cpu * 100, 1), "ram": round(ram * 100, 1)}, name="NormalizeMetrics"),
    )
    registry.register(
        "PersistResult",
        lambda: SimpleTask(lambda cpu, ram: f"PERSISTED cpu={cpu}%, ram={ram}%", name="PersistResult"),
    )

    with EXAMPLE_JSON.open("r", encoding="utf-8") as fp:
        payload = json.load(fp)

    rebuilt = WorkflowGraph.from_json(payload, registry)
    ctx = InMemoryExecutionContext()
    result = rebuilt.compile().invoke(NO_INPUT, ctx)
    print("Rebuilt graph output:", result)


def main() -> None:
    graph = build_graph()
    export_graph(graph)
    rebuild_via_registry()


if __name__ == "__main__":
    main()

