import asyncio
import unittest

from pydantic import BaseModel

from taskpipe import InMemoryExecutionContext, NO_INPUT, SimpleTask
from taskpipe.async_runnables import (
    AgentLoop,
    AsyncBranchAndFanIn,
    AsyncPipeline,
    AsyncRunnable,
    AsyncSourceParallel,
)


class ValuePayload(BaseModel):
    value: int


class AsyncDouble(AsyncRunnable):
    class InputModel(BaseModel):
        value: int

    class OutputModel(BaseModel):
        doubled: int

    async def _internal_invoke_async(self, input_data, context):
        await asyncio.sleep(0.01)
        return {"doubled": input_data.value * 2}


class TestAsyncPipeline(unittest.TestCase):
    def test_async_pipeline_with_simple_task(self):
        def starter() -> ValuePayload:
            return ValuePayload(value=3)

        starter_task = SimpleTask(starter, name="Starter")
        doubler = AsyncDouble(name="Doubler")
        pipeline = starter_task | doubler

        ctx = InMemoryExecutionContext()
        result = asyncio.run(pipeline.invoke_async(NO_INPUT, ctx))
        self.assertEqual(result.doubled, 6)
        self.assertIn("Doubler", ctx.node_outputs)


class TestAsyncParallel(unittest.TestCase):
    def test_async_branch_and_fan_in(self):
        branch = AsyncBranchAndFanIn({
            "left": AsyncDouble(name="Left"),
            "right": AsyncDouble(name="Right"),
        }, name="Fan")
        ctx = InMemoryExecutionContext()
        results = asyncio.run(branch.invoke_async({"value": 5}, ctx))
        self.assertEqual(results.left.doubled, 10)
        self.assertEqual(results.right.doubled, 10)

    def test_async_source_parallel(self):
        nodes = AsyncSourceParallel({
            "alpha": AsyncDouble(name="Alpha"),
            "beta": AsyncDouble(name="Beta"),
        }, name="SourcePar")
        ctx = InMemoryExecutionContext()
        results = asyncio.run(nodes.invoke_async({"value": 2}, ctx))
        self.assertEqual(results.alpha.doubled, 4)
        self.assertEqual(results.beta.doubled, 4)


class TestAgentLoop(unittest.TestCase):
    def test_agent_loop_finishes(self):
        class CounterPayload(BaseModel):
            counter: int

        def increment(counter: int) -> CounterPayload:
            return CounterPayload(counter=counter + 1)

        increment_task = SimpleTask(increment)

        class Sequence(AsyncRunnable):
            class InputModel(BaseModel):
                counter: int

            class OutputModel(BaseModel):
                decision: bool

            def __init__(self):
                super().__init__()
                self._steps = 0

            async def _internal_invoke_async(self, input_data, context):
                self._steps += 1
                if self._steps > 1:
                    return {"decision": False}
                return increment_task

        loop = AgentLoop(Sequence(), max_iterations=3, name="Agent")
        ctx = InMemoryExecutionContext()
        result = asyncio.run(loop.invoke_async({"counter": 0}, ctx))
        self.assertFalse(result.result.decision)


if __name__ == "__main__":
    unittest.main()
