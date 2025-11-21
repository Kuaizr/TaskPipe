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
    AgentLoop
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
        self.assertEqual(res, 20)

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
        self.assertEqual(res, 23)

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
        self.assertEqual(res["a"], 99)
        self.assertEqual(res["b"], 99)

    def test_agent_loop(self):
        """测试 AgentLoop: 动态决定下一步"""
        
        @task
        def tool_add(v: int) -> int:
            return v + 10

        @task
        async def agent_brain(v: int) -> Any:
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
        self.assertEqual(res["final"], 25)

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
        self.assertEqual(res, "OK")
        self.assertEqual(self.attempts, 3)

if __name__ == "__main__":
    import typing
    from typing import Any # 补充 Any 导入
    unittest.main()