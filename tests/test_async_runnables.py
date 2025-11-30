import asyncio
import unittest
from pydantic import BaseModel
from taskpipe import (
    task, Loop, Map, WaitForInput, SuspendExecution, AsyncRunnable, 
    InMemoryExecutionContext, NO_INPUT
)

class TestAsyncControl(unittest.IsolatedAsyncioTestCase):
    async def test_async_loop(self):
        """测试异步 Loop"""
        @task
        async def async_inc(v: int) -> int:
            await asyncio.sleep(0.01)
            return v + 1
            
        loop = Loop(body=async_inc(), config={"condition": "loop_count < 3"})
        res = await loop.invoke_async(0)
        
        val = res.result if hasattr(res, "result") else res
        self.assertEqual(val, 3)

    async def test_async_map_concurrency(self):
        """测试异步 Map 的并发执行"""
        @task
        async def heavy_task(x: int) -> int:
            await asyncio.sleep(0.05) # 模拟耗时
            return x * 10
            
        mapper = Map(body=heavy_task(), config={"max_concurrency": 5})
        inputs = [{"x": i} for i in range(5)]
        
        start_t = asyncio.get_event_loop().time()
        res = await mapper.invoke_async(inputs)
        end_t = asyncio.get_event_loop().time()
        
        # 验证结果正确性
        clean_res = [r.result if hasattr(r, "result") else r for r in res]
        self.assertEqual(clean_res, [0, 10, 20, 30, 40])
        
        # 验证并发性：5个任务并行，总耗时应接近单次耗时 (0.05s)，肯定远小于串行 (0.25s)
        self.assertLess(end_t - start_t, 0.20)

class TestAsyncResume(unittest.IsolatedAsyncioTestCase):
    async def test_wait_for_input_async(self):
        """测试异步环境下的 WaitForInput 挂起与恢复"""
        wait_node = WaitForInput(name="Human")
        ctx = InMemoryExecutionContext()
        
        # 1. 第一次调用，预期抛出 SuspendExecution
        with self.assertRaises(SuspendExecution): 
            await wait_node.invoke_async(NO_INPUT, ctx)
            
        # 2. 恢复调用，传入 resume_state
        resume_val = "User Accepted"
        res = await wait_node.invoke_async(NO_INPUT, ctx, resume_state=resume_val)
        
        # 结果应该是 Model 或 dict，包含 result 字段
        val = res.result if hasattr(res, "result") else res["result"]
        self.assertEqual(val, resume_val)

    async def test_async_pipeline_suspend_resume(self):
        """测试整个异步 Pipeline 的挂起与状态快照恢复"""
        @task
        async def step_a(v: int) -> int:
            return v + 1
            
        wait = WaitForInput()
        
        @task
        async def step_b(v: int) -> int:
            return v * 2
            
        # Pipeline: A -> Wait -> B
        pipe = step_a() | wait | step_b()
        
        # 1. 执行到 Wait 挂起
        # 简单的 A | Wait 测试
        simple_pipe = step_a() | wait
        try:
            await simple_pipe.invoke_async({"v": 10})
        except SuspendExecution as e:
            simple_snapshot = e.snapshot
            # step 1 是 Wait 节点
            self.assertEqual(simple_snapshot["step"], 1)
            # data 是 A 的输出 11
            data_val = simple_snapshot["data"].result if hasattr(simple_snapshot["data"], "result") else simple_snapshot["data"]
            self.assertEqual(data_val, 11)
            
            # 构造恢复 Payload
            resume_payload = {
                "step": 1, 
                "data": simple_snapshot["data"], 
                "child": "GO" # 直接给 Wait 节点传值
            }
            
            res = await simple_pipe.invoke_async(NO_INPUT, resume_state=resume_payload)
            val = res.result if hasattr(res, "result") else res["result"]
            self.assertEqual(val, "GO")

if __name__ == "__main__":
    unittest.main()