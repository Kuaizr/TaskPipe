import unittest

from pydantic import BaseModel

from taskpipe import (
    InMemoryExecutionContext,
    NO_INPUT,
    Router,
    ScriptRunnable,
    SimpleTask,
    task,
)


class MultiplyOutput(BaseModel):
    result: int


class CombineOutput(BaseModel):
    text: str


class IncrementOutput(BaseModel):
    result: int


class TestTaskDecoratorAndMapping(unittest.TestCase):
    def test_task_decorator_enforces_pydantic_contracts(self):
        @task
        def multiply(value: int) -> MultiplyOutput:
            return MultiplyOutput(result=value * 3)

        worker = multiply()
        result = worker.invoke({"value": 5})
        self.assertEqual(result.result, 15)

        with self.assertRaises(ValueError):
            worker.invoke({"value": "bad"})

    def test_map_inputs_supports_dynamic_and_static_fields(self):
        @task
        def producer() -> CombineOutput:
            return CombineOutput(text="hi")

        @task
        def combine(text: str, suffix: str) -> CombineOutput:
            return CombineOutput(text=f"{text}{suffix}")

        source = producer()
        combo = combine().map_inputs(text=source.Output.text, suffix="!")
        ctx = InMemoryExecutionContext()
        source.invoke(NO_INPUT, ctx)

        result = combo.invoke(NO_INPUT, ctx)
        self.assertEqual(result.text, "hi!")


class TestSimpleTaskAndUtilities(unittest.TestCase):
    def test_simple_task_callable(self):
        def increment(value: int) -> IncrementOutput:
            return IncrementOutput(result=value + 1)

        task_inst = SimpleTask(increment, name="Increment")
        result = task_inst.invoke({"value": 2})
        self.assertEqual(result.result, 3)

    def test_router_outputs_decision(self):
        router = Router()
        ctx = InMemoryExecutionContext()
        result = router.invoke({"condition": True}, ctx)
        self.assertTrue(result.decision)


class TestScriptRunnable(unittest.TestCase):
    def test_script_runnable_executes_user_code(self):
        config = {
            "code": "output_value = name.upper()\nresult = output_value",
            "input_keys": ["name"],
            "output_keys": ["result"],
        }
        script = ScriptRunnable(config=config)
        result = script.invoke({"name": "aether"})
        self.assertEqual(result.result, "AETHER")


if __name__ == "__main__":
    unittest.main()
