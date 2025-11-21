import asyncio
import unittest
from pydantic import BaseModel

from taskpipe import (
    InMemoryExecutionContext,
    NO_INPUT,
    task,
    AsyncRunnable,
    AsyncPipeline,
    AsyncBranchAndFanIn,
    AsyncSourceParallel,
    AgentLoop,
    AsyncConditional,
    AsyncWhile
)

class Data(BaseModel):
    v: int

class TestAsyncBasic(unittest.TestCase):
    def test_async_task_generation(self):
        """测试 @task 自动识别 async def 并生成 AsyncRunnable"""
        @task
        async def slow_double(v: int) -> int:
            await asyncio.sleep(0.01)
            return v * 2

        worker = slow_double()
        self.assertIsInstance(worker, AsyncRunnable)

        # 必须使用 invoke_async (或 asyncio.run)
        res = asyncio.run(worker.invoke_async({"v": 10}))
        # @task 返回的是 Pydantic 模型
        res_val = res.result if hasattr(res, 'result') else res
        self.assertEqual(res_val, 20)

    def test_sync_async_mix_pipeline(self):
        """测试同步与异步任务混合在 Pipeline 中"""
        @task
        def sync_step(v: int) -> int:
            return v + 1

        @task
        async def async_step(v: int) -> int:
            await asyncio.sleep(0.01)
            return v * 2

        # Sync | Async | Sync
        pipeline = sync_step() | async_step() | sync_step()
        self.assertIsInstance(pipeline, AsyncPipeline)

        # Input: 10 -> +1=11 -> *2=22 -> +1=23
        res = asyncio.run(pipeline.invoke_async({"v": 10}))
        # @task 返回的是 Pydantic 模型
        res_val = res.result if hasattr(res, 'result') else res
        self.assertEqual(res_val, 23)

class TestAsyncComposers(unittest.TestCase):
    def test_async_branch_fan_in(self):
        """测试异步并行扇出 (AsyncBranchAndFanIn)"""
        @task
        async def worker(v: int) -> int:
            await asyncio.sleep(0.01)
            return v

        # 手动构建 Branch
        branches = AsyncBranchAndFanIn({
            "a": worker(),
            "b": worker()
        })

        res = asyncio.run(branches.invoke_async({"v": 99}))
        # AsyncBranchAndFanIn 返回的是 Pydantic 模型
        self.assertEqual(res.a.result, 99)
        self.assertEqual(res.b.result, 99)

    def test_agent_loop(self):
        """测试 AgentLoop: 动态决定下一步"""
        
        @task
        def tool_add(v: int) -> int:
            return v + 10

        @task
        async def agent_brain(v: int):
            # 模拟 Agent 思考
            if v < 20:
                # 返回一个 Runnable 实例，AgentLoop 会执行它
                return tool_add()
            else:
                # 返回数据，AgentLoop 结束
                return {"final": v}

        # Loop: Brain -> (tool) -> Brain ... -> Result
        agent = AgentLoop(generator=agent_brain(), max_iterations=5)
        
        # Start with 5 -> add(5)=15 -> Brain(15) -> add(15)=25 -> Brain(25) -> Done
        res = asyncio.run(agent.invoke_async({"v": 5}))
        # AgentLoop 返回的是 Pydantic 模型
        res_val = res.result if hasattr(res, 'result') else res
        if isinstance(res_val, dict):
            self.assertEqual(res_val["final"], 25)
        else:
            # 如果是模型，尝试访问 final 字段
            self.assertEqual(getattr(res_val, "final", None), 25)

class TestAsyncErrorHandling(unittest.TestCase):
    def test_async_retry(self):
        """测试异步任务的重试"""
        self.attempts = 0

        @task
        async def flaky() -> str:
            self.attempts += 1
            if self.attempts < 3:
                raise ValueError("Net Error")
            return "OK"

        worker = flaky().retry(max_attempts=3, delay_seconds=0.01)
        res = asyncio.run(worker.invoke_async(NO_INPUT))
        # @task 返回的是 Pydantic 模型
        self.assertEqual(res.result, "OK")
        self.assertEqual(self.attempts, 3)

    def test_async_conditional(self):
        """测试 AsyncConditional"""
        @task
        async def check_condition(v: int) -> int:
            await asyncio.sleep(0.01)
            return v

        @task
        async def true_branch(v: int) -> str:
            return "Big"

        @task
        async def false_branch(v: int) -> str:
            return "Small"

        from taskpipe import AsyncConditional
        check_task = check_condition()
        check_task.set_check(lambda x: (x.result if hasattr(x, 'result') else x) > 5)
        
        cond = AsyncConditional(
            check_task,
            true_branch(),
            false_branch()
        )

        result1 = asyncio.run(cond.invoke_async({"v": 10}))
        # @task 返回的是 Pydantic 模型
        result1_val = result1.result if hasattr(result1, 'result') else result1
        self.assertEqual(result1_val, "Big")

        result2 = asyncio.run(cond.invoke_async({"v": 3}))
        result2_val = result2.result if hasattr(result2, 'result') else result2
        self.assertEqual(result2_val, "Small")

    def test_async_source_parallel(self):
        """测试 AsyncSourceParallel"""
        @task
        async def task_a(v: int) -> int:
            await asyncio.sleep(0.01)
            return v + 1

        @task
        async def task_b(v: int) -> int:
            await asyncio.sleep(0.01)
            return v + 2

        parallel = AsyncSourceParallel({
            "a": task_a(),
            "b": task_b()
        })

        result = asyncio.run(parallel.invoke_async({"v": 10}))
        # AsyncSourceParallel 返回的是 Pydantic 模型
        self.assertEqual(result.a.result, 11)
        self.assertEqual(result.b.result, 12)

    def test_async_while(self):
        """测试 AsyncWhile 循环"""
        @task
        async def check_condition(v: int) -> int:
            await asyncio.sleep(0.001)
            return v

        @task
        async def increment(last: int) -> int:
            await asyncio.sleep(0.001)
            return last + 1

        from taskpipe import AsyncWhile
        check_task = check_condition()
        # check 函数接收的是 Pydantic 模型，需要提取值
        check_task.set_check(lambda x: (x.result if hasattr(x, 'result') else x) < 3)

        loop = AsyncWhile(check_task, increment(), max_loops=10)
        result = asyncio.run(loop.invoke_async({"v": 0}))
        
        self.assertEqual(len(result.history), 3)
        # history 中的元素可能是 Pydantic 模型
        last_value = result.history[-1]
        if hasattr(last_value, 'result'):
            self.assertEqual(last_value.result, 3)
        else:
            self.assertEqual(last_value, 3)

    def test_async_pipeline_with_error(self):
        """测试异步 Pipeline 中的错误处理"""
        @task
        async def step1(v: int) -> int:
            return v + 1

        @task
        async def step2(v: int) -> int:
            raise ValueError("Error in step2")

        @task
        async def error_handler() -> int:
            return -1

        pipeline = step1() | step2()
        pipeline.on_error(error_handler())

        result = asyncio.run(pipeline.invoke_async({"v": 5}))
        # @task 返回的是 Pydantic 模型
        result_val = result.result if hasattr(result, 'result') else result
        self.assertEqual(result_val, -1)

    def test_async_check_function(self):
        """测试异步 check 函数"""
        @task
        async def async_task(v: int) -> int:
            return v

        async def async_check(v) -> bool:
            await asyncio.sleep(0.001)
            # v 可能是 Pydantic 模型，需要提取值
            val = v.result if hasattr(v, 'result') else v
            return val > 5

        worker = async_task()
        worker.set_check(async_check)

        result1 = asyncio.run(worker.check_async(10))
        self.assertTrue(result1)

        result2 = asyncio.run(worker.check_async(3))
        self.assertFalse(result2)

if __name__ == "__main__":
    import typing
    from typing import Any # 补充 Any 导入
    unittest.main()