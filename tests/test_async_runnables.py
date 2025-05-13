import unittest
import asyncio
import time
import logging
from unittest.mock import MagicMock, call

from taskpipe.runnables import Runnable, SimpleTask, ExecutionContext, NO_INPUT, Pipeline as SyncPipeline
from taskpipe import (
    AsyncRunnable, 
    AsyncPipeline, 
    AsyncConditional, 
    AsyncWhile,
    AsyncBranchAndFanIn,
    AsyncSourceParallel,
    _AsyncPendingConditional # For testing operator if needed
)



# Configure logger for less verbose test output
logging.getLogger('taskpipe.runnables').setLevel(logging.CRITICAL)
logging.getLogger('taskpipe.async_runnables').setLevel(logging.CRITICAL)


# --- Helper Test Runnables ---

class SyncTestTask(Runnable):
    def __init__(self, name="SyncTestTask", side_effect_func=None, delay=0):
        super().__init__(name=name)
        self.call_count = 0
        self.inputs_received = []
        self._side_effect_func = side_effect_func
        self.delay = delay

    def _internal_invoke(self, input_data, context):
        self.call_count += 1
        self.inputs_received.append(input_data)
        if self.delay > 0:
            time.sleep(self.delay) # Simulate blocking work
        if self._side_effect_func:
            return self._side_effect_func(input_data, context)
        return f"sync_processed_{self.name}_{input_data}"

    def _default_check(self, data_from_invoke) -> bool:
        # Simple check for testing
        if isinstance(data_from_invoke, str) and "block" in data_from_invoke:
            return False
        return True


class AsyncTestTask(AsyncRunnable):
    def __init__(self, name="AsyncTestTask", side_effect_coro=None, delay=0):
        super().__init__(name=name)
        self.call_count = 0
        self.inputs_received = []
        self._side_effect_coro = side_effect_coro
        self.delay = delay

    async def _internal_invoke_async(self, input_data, context):
        self.call_count += 1
        self.inputs_received.append(input_data)
        if self.delay > 0:
            await asyncio.sleep(self.delay) # Simulate non-blocking work
        if self._side_effect_coro:
            return await self._side_effect_coro(input_data, context)
        return f"async_processed_{self.name}_{input_data}"

    async def _default_check_async(self, data_from_invoke) -> bool:
        # Simple async check for testing
        if isinstance(data_from_invoke, str) and "block_async" in data_from_invoke:
            return False
        await asyncio.sleep(0.001) # simulate async check
        return True

# --- Test Cases ---

class TestAsyncRunnableBase(unittest.TestCase):
    def test_async_runnable_invoke_async(self):
        async def run_test():
            task = AsyncTestTask("AR1", delay=0.01)
            result = await task.invoke_async("test_input")
            self.assertEqual(result, "async_processed_AR1_test_input")
            self.assertEqual(task.call_count, 1)
            self.assertEqual(task.inputs_received[0], "test_input")
        asyncio.run(run_test())

    def test_async_runnable_check_async(self):
        async def run_test():
            task = AsyncTestTask("AR_Check")
            # Test default check
            self.assertTrue(await task.check_async("some_data"))
            self.assertFalse(await task.check_async("block_async_data"))

            # Test custom async check
            custom_check_coro = MagicMock(return_value=None) # Needs to be awaitable
            async def mock_async_check(data):
                custom_check_coro(data)
                return False
            
            task.set_check(mock_async_check) # set_check handles both sync and async
            self.assertFalse(await task.check_async("custom_check_data"))
            custom_check_coro.assert_called_once_with("custom_check_data")
        asyncio.run(run_test())


class TestAsyncPipeline(unittest.TestCase):
    def test_pipeline_all_async(self):
        async def run_test():
            ar1 = AsyncTestTask("AR1", delay=0.01)
            async def ar2_side_effect(d,c): return f"final_{d}"
            ar2 = AsyncTestTask("AR2", side_effect_coro=ar2_side_effect)
            
            pipeline = AsyncPipeline(ar1, ar2)
            context = ExecutionContext()
            result = await pipeline.invoke_async("start", context)
            
            self.assertEqual(ar1.call_count, 1)
            self.assertEqual(ar2.call_count, 1)
            self.assertEqual(result, "final_async_processed_AR1_start")
            self.assertEqual(context.get_output("AR1"), "async_processed_AR1_start")
            self.assertEqual(context.get_output(pipeline.name), result)
        asyncio.run(run_test())

    def test_pipeline_mixed_async_sync(self):
        async def run_test():
            ar1 = AsyncTestTask("AR1_mixed", delay=0.01)
            sr2 = SyncTestTask("SR2_mixed", delay=0.01, side_effect_func=lambda d,c: f"sync_final_{d}")
            async def ar3_side_effect(d,c): return f"async_ultra_{d}"
            ar3 = AsyncTestTask("AR3_mixed", side_effect_coro=ar3_side_effect)

            # Explicitly create AsyncPipeline
            pipeline1 = AsyncPipeline(ar1, sr2) # AR | SR
            pipeline_full = AsyncPipeline(pipeline1, ar3) # (AR | SR) | AR
            
            context = ExecutionContext()
            start_time = time.time()
            result = await pipeline_full.invoke_async("mix_start", context)
            duration = time.time() - start_time

            self.assertEqual(ar1.call_count, 1)
            self.assertEqual(sr2.call_count, 1) # Sync task called via invoke_async -> run_in_executor
            self.assertEqual(ar3.call_count, 1)
            self.assertEqual(result, "async_ultra_sync_final_async_processed_AR1_mixed_mix_start")
            # Check if total time is roughly sum of delays due to await and executor for sync task
            self.assertTrue(duration >= 0.02, f"Duration {duration} was too short.")
        asyncio.run(run_test())

    def test_pipeline_operator_overload_async(self):
        async def run_test():
            ar1 = AsyncTestTask("Op_AR1")
            sr1 = SyncTestTask("Op_SR1")
            ar2 = AsyncTestTask("Op_AR2")

            # Test AsyncRunnable.__or__ and __ror__
            pipeline1 = AsyncPipeline(ar1, sr1)  # Should create AsyncPipeline
            self.assertIsInstance(pipeline1, AsyncPipeline)
            
            pipeline2 = AsyncPipeline(sr1, ar1)  # Should create AsyncPipeline
            self.assertIsInstance(pipeline2, AsyncPipeline)

            pipeline3 = AsyncPipeline(ar1, ar2) # Async | Async -> AsyncPipeline
            self.assertIsInstance(pipeline3, AsyncPipeline)

            result1 = await pipeline1.invoke_async("test1")
            self.assertEqual(result1, "sync_processed_Op_SR1_async_processed_Op_AR1_test1")
            
            result2 = await pipeline2.invoke_async("test2")
            self.assertEqual(result2, "async_processed_Op_AR1_sync_processed_Op_SR1_test2")

        asyncio.run(run_test())


class TestAsyncConditional(unittest.TestCase):
    def test_conditional_async_true_branch(self):
        async def run_test():
            cond_task = AsyncTestTask("CondAR") # Default check is bool(output)
            async def true_task_side_effect(d,c): return f"true_{d}"
            true_task = AsyncTestTask("TrueAR", side_effect_coro=true_task_side_effect)
            async def false_task_side_effect(d,c): return f"false_{d}" # Not called in this path but define for consistency
            false_task = AsyncTestTask("FalseAR", side_effect_coro=false_task_side_effect)

            # Explicit construction
            conditional = AsyncConditional(cond_task, true_task, false_task)
            context = ExecutionContext()
            result = await conditional.invoke_async("condition_input_for_true", context) # Default check on this string is True

            self.assertEqual(cond_task.call_count, 1)
            self.assertEqual(true_task.call_count, 1)
            self.assertEqual(false_task.call_count, 0)
            self.assertEqual(result, "true_async_processed_CondAR_condition_input_for_true")
        asyncio.run(run_test())

    def test_conditional_async_false_branch_with_custom_check(self):
        async def run_test():
            cond_task = AsyncTestTask("CondAR_False")
            # Custom async check for cond_task
            async def custom_checker(data): return "false" not in data # True if "false" is NOT in data
            cond_task.set_check(custom_checker)

            true_task = AsyncTestTask("TrueAR2")
            async def false_task2_side_effect(d,c): return f"false_val_{d}"
            false_task = AsyncTestTask("FalseAR2", side_effect_coro=false_task2_side_effect)
            
            # Test operator: ar_cond % ar_true >> ar_false
            conditional_op = cond_task % true_task >> false_task
            self.assertIsInstance(conditional_op, AsyncConditional)
            
            context = ExecutionContext()
            # "condition_input_makes_false" will make custom_checker return False
            result = await conditional_op.invoke_async("condition_input_makes_false", context)

            self.assertEqual(cond_task.call_count, 1)
            self.assertEqual(true_task.call_count, 0)
            self.assertEqual(false_task.call_count, 1)
            self.assertEqual(result, "false_val_async_processed_CondAR_False_condition_input_makes_false")
        asyncio.run(run_test())


class TestAsyncWhile(unittest.TestCase):
    def test_while_async_loop(self):
        async def run_test():
            # Condition: input < 3 (async check)
            # Body: input + 1 (async processing)
            
            async def cond_passthrough(d, c): return d # Make cond_runnable output the number
            cond_runnable = AsyncTestTask(name="LoopCond", side_effect_coro=cond_passthrough)
            async def cond_check(data): # data is output of LoopCond.invoke_async
                return data < 3
            cond_runnable.set_check(cond_check)

            async def body_coro_while(data, context): # data is previous body output or initial
                return data + 1
            body_runnable = AsyncTestTask(name="LoopBody", side_effect_coro=body_coro_while)
            
            loop = AsyncWhile(cond_runnable, body_runnable, max_loops=5)
            context = ExecutionContext()
            results = await loop.invoke_async(0, context) # Start with 0

            self.assertEqual(results, [1, 2, 3]) # 0->1, 1->2, 2->3 (cond becomes false for 3)
            self.assertEqual(cond_runnable.call_count, 4) # 0, 1, 2 (true), 3 (false)
            self.assertEqual(body_runnable.call_count, 3) # for 0, 1, 2
        asyncio.run(run_test())


class TestAsyncParallelComposers(unittest.TestCase):
    def test_async_branch_and_fan_in_mixed_tasks(self):
        async def run_test():
            ar_branch = AsyncTestTask("AR_Branch", delay=0.02)
            sr_branch = SyncTestTask("SR_Branch", delay=0.02) # Will run in executor

            fan_in = AsyncBranchAndFanIn({
                "async_key": ar_branch,
                "sync_key": sr_branch
            })
            context = ExecutionContext()
            start_time = time.time()
            results = await fan_in.invoke_async("fan_in_input", context)
            duration = time.time() - start_time

            self.assertEqual(ar_branch.call_count, 1)
            self.assertEqual(sr_branch.call_count, 1)
            self.assertIn("async_key", results)
            self.assertIn("sync_key", results)
            self.assertEqual(results["async_key"], "async_processed_AR_Branch_fan_in_input")
            self.assertEqual(results["sync_key"], "sync_processed_SR_Branch_fan_in_input")
            
            # Expect tasks to run concurrently (roughly max of individual delays)
            self.assertTrue(duration < 0.035 and duration > 0.015, f"Duration {duration} suggests non-concurrent execution.")
        asyncio.run(run_test())

    def test_async_source_parallel_all_async(self):
        async def run_test():
            source_ar1 = AsyncTestTask("SourceAR1", delay=0.01)
            source_ar2 = AsyncTestTask("SourceAR2", delay=0.02)

            parallel_runner = AsyncSourceParallel({
                "s1": source_ar1,
                "s2": source_ar2
            })
            context = ExecutionContext()
            start_time = time.time()
            results = await parallel_runner.invoke_async("source_parallel_input", context)
            duration = time.time() - start_time

            self.assertEqual(source_ar1.call_count, 1)
            self.assertEqual(source_ar2.call_count, 1)
            self.assertEqual(results["s1"], "async_processed_SourceAR1_source_parallel_input")
            self.assertEqual(results["s2"], "async_processed_SourceAR2_source_parallel_input")
            
            # Check for concurrency
            # Further relax the lower bound, as sleep(0.01) and sleep(0.02) might finish very quickly.
            # The key is that it's less than the sum of delays (0.03) and more than a very small epsilon.
            self.assertTrue(duration < 0.05 and duration > 0.005, f"Duration {duration} suggests non-concurrent execution.")
            # Check context for individual outputs
            self.assertEqual(context.get_output(f"{parallel_runner.name}_s1"), results["s1"])
            self.assertEqual(context.get_output(f"{parallel_runner.name}_s2"), results["s2"])
        asyncio.run(run_test())

    def test_async_branch_operator(self):
        async def run_test():
            source_task = AsyncTestTask("FanSource")
            ar_branch = AsyncTestTask("AR_OpBranch")
            sr_branch = SyncTestTask("SR_OpBranch")

            # ar_source | {ar_b, sr_b}
            # AsyncRunnable.__or__ should handle the dict to create AsyncPipeline with AsyncBranchAndFanIn (or wrapped SyncBranchAndFanIn)
            workflow = source_task | {
                "async_b": ar_branch,
                "sync_b": sr_branch
            }
            self.assertIsInstance(workflow, AsyncPipeline)
            # The second part of the pipeline should be an AsyncBranchAndFanIn or a SyncBranchAndFanIn
            # based on the AsyncRunnable.__or__ logic.
            # If it's SyncBranchAndFanIn, AsyncPipeline will wrap its invoke_async.

            context = ExecutionContext()
            results = await workflow.invoke_async("fan_op_input", context)

            self.assertEqual(source_task.call_count, 1)
            self.assertEqual(ar_branch.call_count, 1)
            self.assertEqual(sr_branch.call_count, 1)
            
            self.assertIsInstance(results, dict)
            self.assertEqual(results["async_b"], "async_processed_AR_OpBranch_async_processed_FanSource_fan_op_input")
            self.assertEqual(results["sync_b"], "sync_processed_SR_OpBranch_async_processed_FanSource_fan_op_input")

        asyncio.run(run_test())


if __name__ == '__main__':
    unittest.main(verbosity=2)