"""
Example: WorkflowGraph serialization + RunnableRegistry with explicit mappings.
Run with: `python examples/registry_serialization.py`
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel

from taskpipe import (
    InMemoryExecutionContext,
    NO_INPUT,
    RunnableRegistry,
    WorkflowGraph,
    task,
)

EXAMPLE_JSON = Path(__file__).with_name("registry_workflow.json")


class MetricPayload(BaseModel):
    cpu: float
    ram: float


class ResultPayload(BaseModel):
    text: str


@task
def collect_metrics() -> MetricPayload:
    return MetricPayload(cpu=0.63, ram=0.41)


@task
def normalize_metrics(cpu: float, ram: float) -> MetricPayload:
    return MetricPayload(cpu=round(cpu * 100, 1), ram=round(ram * 100, 1))


@task
def persist_result(cpu: float, ram: float) -> ResultPayload:
    return ResultPayload(text=f"Recorded CPU={cpu}%, RAM={ram}%")


def build_graph() -> WorkflowGraph:
    graph = WorkflowGraph(name="MetricIngestion")

    collect = collect_metrics()
    normalize = normalize_metrics()
    persist = persist_result()

    graph.add_node(collect, "CollectMetrics")
    graph.add_node(normalize, "NormalizeMetrics")
    graph.add_node(persist, "PersistResult")

    graph.add_edge("CollectMetrics", "NormalizeMetrics", data_mapping={"cpu": "cpu", "ram": "ram"})
    graph.add_edge("NormalizeMetrics", "PersistResult", data_mapping={"cpu": "cpu", "ram": "ram"})
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
    registry.register("CollectMetrics", collect_metrics)
    registry.register("NormalizeMetrics", normalize_metrics)
    registry.register("PersistResult", persist_result)

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
