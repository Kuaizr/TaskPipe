import unittest

from pydantic import BaseModel

from taskpipe import InMemoryExecutionContext, NO_INPUT, Router, SimpleTask, WorkflowGraph, task


class MessagePayload(BaseModel):
    message: str


class PromptPayload(BaseModel):
    prompt: str
    temperature: float


class ResultPayload(BaseModel):
    result: str


class StartPayload(BaseModel):
    condition: bool
    value: str


class TestWorkflowGraphSerialization(unittest.TestCase):
    def test_graph_to_json_contains_schema_and_mappings(self):
        @task
        def producer() -> MessagePayload:
            return MessagePayload(message="hello")

        @task
        def sink(prompt: str, temperature: float) -> ResultPayload:
            return ResultPayload(result=f"{prompt}:{temperature}")

        source = producer()
        consumer = sink().map_inputs(prompt=source.Output.message, temperature=0.5)
        graph = (source | consumer).to_graph("Example")
        data = graph.to_json()

        self.assertEqual(data["name"], "Example")
        self.assertEqual(len(data["nodes"]), 2)
        edge = data["edges"][0]
        self.assertIn("data_mapping", edge)
        self.assertEqual(edge["static_inputs"].get("temperature"), 0.5)

    def test_graph_round_trip_with_registry(self):
        def start_fn() -> MessagePayload:
            return MessagePayload(message="hi")

        graph = WorkflowGraph(name="Registry")
        graph.add_node(SimpleTask(start_fn, name="Start"), "Start")
        graph.set_entry_point("Start").set_output_nodes(["Start"])

        payload = graph.to_json()
        registry = {"Start": lambda: SimpleTask(start_fn, name="Start")}
        rebuilt = WorkflowGraph.from_json(payload, registry)
        result = rebuilt.compile().invoke(NO_INPUT)
        self.assertEqual(result.message, "hi")


class TestCompiledGraphExecution(unittest.TestCase):
    def test_branching_router_skips_inactive_branch(self):
        def start_fn() -> StartPayload:
            return StartPayload(condition=False, value="payload")

        start = SimpleTask(start_fn, name="Start")
        router = Router(name="Switch")

        def true_fn(value: str) -> ResultPayload:
            return ResultPayload(result=f"T:{value}")

        def false_fn(value: str) -> ResultPayload:
            return ResultPayload(result=f"F:{value}")

        true_branch = SimpleTask(true_fn, name="TrueBranch")
        false_branch = SimpleTask(false_fn, name="FalseBranch")

        graph = WorkflowGraph(name="RouterGraph")
        graph.add_node(start, "Start")
        graph.add_node(router, "Router")
        graph.add_node(true_branch, "True")
        graph.add_node(false_branch, "False")

        graph.add_edge("Start", "Router", data_mapping={"condition": "condition"})
        graph.add_edge("Start", "True", data_mapping={"value": "value"})
        graph.add_edge("Start", "False", data_mapping={"value": "value"})
        graph.add_edge("Router", "True", branch="true")
        graph.add_edge("Router", "False", branch="false")

        graph.set_entry_point("Start").set_output_nodes(["False"])
        compiled = graph.compile()
        ctx = InMemoryExecutionContext()
        result = compiled.invoke(NO_INPUT, ctx)

        self.assertEqual(result.result, "F:payload")
        skipped_nodes = [event["node"] for event in ctx.node_status_events if event["status"] == "skipped"]
        self.assertIn("True", skipped_nodes)


if __name__ == "__main__":
    unittest.main()
