import unittest
import time
from typing import Any, List, Dict
from pydantic import BaseModel

from taskpipe import (
    InMemoryExecutionContext, Runnable, task, NO_INPUT, 
    START, END, Switch, Loop, Map, WaitForInput, SuspendExecution, 
    Pipeline, BranchAndFanIn, SourceParallel
)

# --- 模型定义 ---
class SimpleInput(BaseModel):
    value: int

class SimpleOutput(BaseModel):
    result: int

class NestedOutput(BaseModel):
    data: SimpleOutput
    info: str

# --- 任务定义 ---
@task
def add_one(value: int) -> SimpleOutput:
    return SimpleOutput(result=value + 1)

@task
def double_val(value: int) -> SimpleOutput:
    return SimpleOutput(result=value * 2)

@task
def make_nested(result: int) -> NestedOutput:
    return NestedOutput(data=SimpleOutput(result=result), info="nested")

class TestCoreMechanics(unittest.TestCase):
    def test_basic_invoke_and_schema(self):
        """测试基础的 invoke 和 Pydantic 校验"""
        worker = add_one()
        res = worker.invoke({"value": 10})
        self.assertEqual(res.result, 11)
        
        # 测试输入校验失败
        with self.assertRaises(ValueError):
            worker.invoke({"value": "not_an_int"})

    def test_deep_mapping(self):
        """测试 map_inputs 的深层字段映射能力 (task.Output.data.result)"""
        nested_task = make_nested()
        consumer = add_one(name="Consumer").map_inputs(
            value=nested_task.Output.data.result
        )
        
        ctx = InMemoryExecutionContext()
        # 模拟上游产出
        ctx.add_output(nested_task.name, NestedOutput(data=SimpleOutput(result=10), info="test"))
        
        res = consumer.invoke(NO_INPUT, ctx)
        self.assertEqual(res.result, 11)

    def test_start_end_contract(self):
        """测试 START 和 END 节点的透传特性"""
        start = START(SimpleInput)
        end = END(SimpleOutput)
        
        # START 应该原样返回输入，且具备 InputModel 供检查
        res_start = start.invoke({"value": 99})
        # 注意：START 内部返回的是 dict 或 model，取决于 _internal_invoke 实现，通常是透传
        # 在新版实现中 START _internal_invoke 直接返回 input_data (可能已归一化)
        val = res_start.value if isinstance(res_start, SimpleInput) else res_start["value"]
        self.assertEqual(val, 99)
        self.assertEqual(start.InputModel, SimpleInput)
        self.assertEqual(end.OutputModel, SimpleOutput)

class TestControlFlow(unittest.TestCase):
    def test_switch_logic(self):
        """测试 Switch 节点基于配置的规则跳转"""
        # 配置规则：如果 val > 10 -> BIG，否则默认 -> SMALL
        switch = Switch(config={
            "rules": [("val > 10", "BIG"), ("val == 10", "EQUAL")], 
            "default_branch": "SMALL"
        })
        
        res1 = switch.invoke({"val": 20})
        self.assertEqual(res1.decision, "BIG")
        
        res2 = switch.invoke({"val": 5})
        self.assertEqual(res2.decision, "SMALL")
        
        res3 = switch.invoke({"val": 10})
        self.assertEqual(res3.decision, "EQUAL")

    def test_loop_logic(self):
        """测试 Loop 节点的循环与状态累加"""
        @task
        def increment(current: int) -> int:
            return current + 1
            
        # 循环条件：loop_count < 5 (内置变量)
        loop = Loop(body=increment(), config={"condition": "loop_count < 5"})
        
        # 初始输入 0 -> 1 -> 2 -> 3 -> 4 -> 5 (loop_count=0..4 执行 5 次)
        res = loop.invoke(0)
        
        val = res.result if hasattr(res, "result") else res
        self.assertEqual(val, 5)

    def test_map_parallel_sync(self):
        """测试 Map 节点处理列表数据的能力"""
        @task
        def square(x: int) -> int:
            time.sleep(0.01) # 模拟一点耗时
            return x * x
        
        mapper = Map(body=square(), config={"max_concurrency": 2})
        
        inputs = [{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}]
        results = mapper.invoke(inputs)
        expected = [1, 4, 9, 16]
        
        clean_results = []
        for r in results:
            val = r.result if hasattr(r, "result") else r
            clean_results.append(val)
                
        self.assertEqual(clean_results, expected)

class TestLifecycleAndHooks(unittest.TestCase):
    def test_retry_and_hooks(self):
        """测试重试机制和生命周期钩子"""
        self.attempts = 0
        events = []

        @task
        def flaky_task() -> int:
            self.attempts += 1
            if self.attempts < 3:
                raise ValueError("Fail")
            return 100

        def on_error_handler(ctx, inp, exc):
            events.append(f"error:{type(exc).__name__}")
            # [修复] 错误处理器必须返回符合 OutputModel 的数据 (int)
            return 0 

        worker = flaky_task().retry(
            max_attempts=3, delay_seconds=0, retry_on_exceptions=(ValueError,)
        ).on_error(on_error_handler)
        
        res = worker.invoke(NO_INPUT)
        
        # 结果应为 100 (第3次成功)
        val = res.result if hasattr(res, "result") else res
        self.assertEqual(val, 100)
        self.assertEqual(self.attempts, 3)
        
        # 测试最终失败并被 Handler 兜底的情况
        self.attempts = 0
        worker_fail = flaky_task().retry(max_attempts=2, delay_seconds=0).on_error(
            lambda ctx, i, e: {"result": 999} # 兜底返回值
        )
        res_fail = worker_fail.invoke(NO_INPUT)
        
        val_fail = res_fail.result if hasattr(res_fail, "result") else res_fail["result"]
        self.assertEqual(val_fail, 999) # 兜底生效

class TestLegacyComposers(unittest.TestCase):
    def test_pipeline_operator(self):
        """测试 | 操作符生成的 Pipeline"""
        pipe = add_one() | double_val()
        # 10 + 1 = 11 -> 11 * 2 = 22
        res = pipe.invoke({"value": 10})
        self.assertEqual(res.result, 22)

    def test_branch_fan_in(self):
        """测试 BranchAndFanIn"""
        branches = BranchAndFanIn({
            "a": add_one(),
            "b": double_val()
        })
        res = branches.invoke({"value": 10})
        # [修复] 结果是 Model，使用属性访问而不是字典访问
        self.assertEqual(res.a.result, 11)
        self.assertEqual(res.b.result, 20)

if __name__ == "__main__":
    unittest.main()