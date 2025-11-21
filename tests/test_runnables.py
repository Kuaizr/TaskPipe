import unittest
from typing import Any
from pydantic import BaseModel, ValidationError

from taskpipe import ( 
    InMemoryExecutionContext,
    ExecutionContext,
    NO_INPUT,
    Runnable,
    task,
    Pipeline,
    Conditional,
    BranchAndFanIn,
    SourceParallel,
    While,
    Router,
    ScriptRunnable,
    MergeInputs
)

# --- 模型定义 ---
class MathInput(BaseModel):
    val: int

class MathOutput(BaseModel):
    result: int

class StringInput(BaseModel):
    text: str
    suffix: str = "!"

class StringOutput(BaseModel):
    content: str

# --- 测试用例 ---

class TestTaskBasic(unittest.TestCase):
    def test_task_sync_execution_and_schema(self):
        """测试 @task 装饰器生成同步 Runnable 及 Schema 校验"""
        @task
        def double(val: int) -> MathOutput:
            return MathOutput(result=val * 2)

        # 实例化
        worker = double(name="Doubler")
        
        # 1. 正常调用
        res = worker.invoke({"val": 10})
        self.assertEqual(res.result, 20)
        self.assertIsInstance(res, MathOutput)

        # 2. 校验 InputModel
        with self.assertRaises(ValueError):
            worker.invoke({"val": "not_an_int"})

    def test_map_inputs_explicit_binding(self):
        """测试 map_inputs 显式数据流绑定"""
        @task
        def source() -> str:
            return "10"

        @task
        def consumer(text: str, suffix: str) -> StringOutput:
            return StringOutput(content=f"{text}{suffix}")

        t1 = source(name="Source")
        # 映射: text 取自 Source 的 result, suffix 使用静态值
        t2 = consumer(name="Consumer").map_inputs(
            text=t1.Output.result,
            suffix="?"
        )

        # 手动模拟 Context 数据流
        ctx = InMemoryExecutionContext()
        # 先运行 t1，产生数据
        t1_res = t1.invoke(NO_INPUT, ctx)
        
        # 运行 t2，它应该自动从 context 获取 Source 的输出
        t2_res = t2.invoke(NO_INPUT, ctx)
        self.assertEqual(t2_res.content, "10?")

class TestLifecycleAndHooks(unittest.TestCase):
    def test_retry_logic(self):
        """测试重试机制"""
        self.counter = 0

        @task
        def unstable_task() -> int:
            self.counter += 1
            if self.counter < 3:
                raise ValueError("Fail")
            return 100

        # 设置重试：最多3次，间隔0秒
        worker = unstable_task().retry(max_attempts=3, delay_seconds=0, retry_on_exceptions=(ValueError,))
        
        res = worker.invoke(NO_INPUT)
        # @task 返回的是 Pydantic 模型
        res_val = res.result if hasattr(res, 'result') else res
        self.assertEqual(res_val, 100)
        self.assertEqual(self.counter, 3)

    def test_hooks_execution(self):
        """测试 on_start, on_complete, on_error 钩子"""
        events = []

        @task
        def risky_task(fail: bool) -> str:
            if fail:
                raise RuntimeError("Boom")
            return "Success"

        def start_hook(ctx, input_data):
            events.append("start")

        def complete_hook(ctx, result, exception):
            if exception:
                events.append(f"complete_error:{type(exception).__name__}")
            else:
                events.append(f"complete_ok:{result}")

        def error_handler(ctx, input_data, exc):
            events.append("handled")
            return "Recovered"

        # 1. 测试成功路径
        worker = risky_task(name="Worker")
        worker.set_on_start(start_hook).set_on_complete(complete_hook)
        
        res = worker.invoke({"fail": False})
        # @task 返回的是 Pydantic 模型
        res_val = res.result if hasattr(res, 'result') else res
        self.assertEqual(res_val, "Success")
        # 检查事件（可能包含模型对象）
        self.assertIn("start", events)
        self.assertTrue(any("complete_ok" in str(e) for e in events))

        # 2. 测试失败并恢复路径
        events.clear()
        worker.on_error(error_handler) # 添加错误处理
        
        res = worker.invoke({"fail": True})
        # @task 返回的是 Pydantic 模型
        res_val = res.result if hasattr(res, 'result') else res
        self.assertEqual(res_val, "Recovered") # 被 on_error 覆盖
        # 注意: on_error 处理后，on_complete 接收到的 exception 可能为空（如果视为成功恢复），
        # 或者取决于具体实现细节。在当前实现中，如果 handler 成功，视为正常返回。
        self.assertIn("start", events)
        self.assertIn("handled", events)

class TestComposersSync(unittest.TestCase):
    def test_pipeline_operator(self):
        """测试 | 操作符"""
        @task
        def add_one(val: int) -> int:
            return val + 1

        chain = add_one() | add_one() | add_one()
        res = chain.invoke({"val": 0})
        # {val:0} -> 1 -> 2 -> 3. 
        # @task 返回的是 Pydantic 模型，需要提取值
        if hasattr(res, 'result'):
            self.assertEqual(res.result, 3)
        else:
            self.assertEqual(res, 3)

    def test_conditional_operator(self):
        """测试 % >> 操作符 (同步)"""
        @task
        def is_even(val: int) -> int:
            return val

        @task
        def handle_true(val: int) -> str:
            return "Even"

        @task
        def handle_false(val: int) -> str:
            return "Odd"

        check_task = is_even()
        # check 函数接收的是 Pydantic 模型，需要提取值
        check_task.set_check(lambda x: (x.result if hasattr(x, 'result') else x) % 2 == 0)
        logic = check_task % handle_true() >> handle_false()
        
        result1 = logic.invoke({"val": 2})
        result2 = logic.invoke({"val": 3})
        # 提取字符串值
        if hasattr(result1, 'result'):
            self.assertEqual(result1.result, "Even")
            self.assertEqual(result2.result, "Odd")
        else:
            self.assertEqual(result1, "Even")
            self.assertEqual(result2, "Odd")

    def test_conditional_set_check(self):
        """测试 set_check 自定义检查函数"""
        @task
        def process(val: int) -> int:
            return val

        # 自定义 check: val > 5，需要处理 Pydantic 模型
        process_task = process()
        process_task.set_check(lambda x: (x.result if hasattr(x, 'result') else x) > 5)
        
        @task
        def big(val: int) -> str:
            return "Big"
        
        @task
        def small(val: int) -> str:
            return "Small"
        
        logic = process_task % big() >> small()
        
        result1 = logic.invoke({"val": 10})
        result2 = logic.invoke({"val": 1})
        # 提取字符串值
        if hasattr(result1, 'result'):
            self.assertEqual(result1.result, "Big")
            self.assertEqual(result2.result, "Small")
        else:
            self.assertEqual(result1, "Big")
            self.assertEqual(result2, "Small")

    def test_branch_and_fan_in(self):
        """测试 BranchAndFanIn 并行执行"""
        # 使用相同的输入模型
        class BranchInput(BaseModel):
            val: int
        
        class DoubleOutput(BaseModel):
            result: int
        
        class TripleOutput(BaseModel):
            result: int
        
        class DoubleTask(Runnable):
            InputModel = BranchInput
            OutputModel = DoubleOutput
            
            def _internal_invoke(self, input_data, context):
                return {"result": input_data.val * 2}
        
        class TripleTask(Runnable):
            InputModel = BranchInput
            OutputModel = TripleOutput
            
            def _internal_invoke(self, input_data, context):
                return {"result": input_data.val * 3}

        branches = BranchAndFanIn({
            "double": DoubleTask(),
            "triple": TripleTask()
        })

        result = branches.invoke({"val": 5})
        # BranchAndFanIn 返回的是 Pydantic 模型
        self.assertEqual(result.double.result, 10)
        self.assertEqual(result.triple.result, 15)

    def test_source_parallel(self):
        """测试 SourceParallel 从同一输入并行执行"""
        # 使用相同的输入模型
        class ParallelInput(BaseModel):
            val: int
        
        class ParallelOutput(BaseModel):
            result: int
        
        class AddOneTask(Runnable):
            InputModel = ParallelInput
            OutputModel = ParallelOutput
            
            def _internal_invoke(self, input_data, context):
                return {"result": input_data.val + 1}
        
        class AddTwoTask(Runnable):
            InputModel = ParallelInput
            OutputModel = ParallelOutput
            
            def _internal_invoke(self, input_data, context):
                return {"result": input_data.val + 2}

        parallel = SourceParallel({
            "one": AddOneTask(),
            "two": AddTwoTask()
        })

        result = parallel.invoke({"val": 10})
        # SourceParallel 返回的是 Pydantic 模型
        self.assertEqual(result.one.result, 11)
        self.assertEqual(result.two.result, 12)

    def test_while_loop(self):
        """测试 While 循环"""
        @task
        def check_condition(val: int) -> int:
            return val

        @task
        def increment(last: int) -> int:
            return last + 1

        check_task = check_condition()
        # check 函数接收的是 Pydantic 模型，需要提取值
        check_task.set_check(lambda x: (x.result if hasattr(x, 'result') else x) < 5)

        loop = While(check_task, increment(), max_loops=10)
        result = loop.invoke({"val": 0})
        
        # 应该执行 5 次: 0->1->2->3->4->5 (5 不满足条件，停止)
        # While 返回的是 Pydantic 模型
        self.assertEqual(len(result.history), 5)
        # history 中的元素也是 Pydantic 模型，需要提取值
        last_value = result.history[-1]
        if hasattr(last_value, 'result'):
            self.assertEqual(last_value.result, 5)
        else:
            self.assertEqual(last_value, 5)

    def test_router_node(self):
        """测试 Router 节点"""
        router = Router(name="TestRouter")
        
        # Router 接受 condition: bool
        result = router.invoke({"condition": True})
        self.assertTrue(result.decision)
        
        result = router.invoke({"condition": False})
        self.assertFalse(result.decision)

    def test_script_runnable(self):
        """测试 ScriptRunnable 执行用户代码"""
        script = ScriptRunnable(config={
            "code": "result = x * 2 + y",
            "input_keys": ["x", "y"],
            "output_keys": ["result"]
        })

        result = script.invoke({"x": 5, "y": 3})
        self.assertEqual(result.result, 13)

    def test_merge_inputs(self):
        """测试 MergeInputs 合并多个输入"""
        @task
        def merge(a: int, b: str, c: float) -> str:
            return f"{a}-{b}-{c}"

        merge_task = MergeInputs(None, merge)
        
        ctx = InMemoryExecutionContext()
        ctx.add_output("source_a", 10)
        ctx.add_output("source_b", "hello")
        
        # 使用 map_inputs 配置映射
        merge_task = merge_task.map_inputs(
            a=task(lambda: 10)().Output.result,
            b=task(lambda: "hello")().Output.result,
            c=3.14
        )
        
        # 手动设置上下文
        task(lambda: 10)().invoke(NO_INPUT, ctx)
        task(lambda: "hello")().invoke(NO_INPUT, ctx)
        
        result = merge_task.invoke(NO_INPUT, ctx)
        # MergeInputs 返回的可能是字符串或 Pydantic 模型
        result_str = result.result if hasattr(result, 'result') else result
        self.assertIn("10", str(result_str))
        self.assertIn("hello", str(result_str))

    def test_cache_functionality(self):
        """测试缓存功能"""
        call_count = [0]

        @task
        def cached_task(val: int) -> int:
            call_count[0] += 1
            return val * 2

        worker = cached_task(use_cache=True)
        
        # 第一次调用
        result1 = worker.invoke({"val": 5})
        # @task 返回的是 Pydantic 模型
        result1_val = result1.result if hasattr(result1, 'result') else result1
        self.assertEqual(result1_val, 10)
        self.assertEqual(call_count[0], 1)
        
        # 第二次调用相同输入，应该使用缓存
        result2 = worker.invoke({"val": 5})
        result2_val = result2.result if hasattr(result2, 'result') else result2
        self.assertEqual(result2_val, 10)
        self.assertEqual(call_count[0], 1)  # 不应该再次调用
        
        # 不同输入，应该重新计算
        result3 = worker.invoke({"val": 10})
        result3_val = result3.result if hasattr(result3, 'result') else result3
        self.assertEqual(result3_val, 20)
        self.assertEqual(call_count[0], 2)
        
        # 清除缓存
        worker.clear_cache()
        result4 = worker.invoke({"val": 5})
        self.assertEqual(call_count[0], 3)  # 应该重新计算

    def test_map_inputs_complex_scenarios(self):
        """测试 map_inputs 复杂场景"""
        @task
        def source1() -> MathOutput:
            return MathOutput(result=10)

        @task
        def source2() -> MathOutput:
            return MathOutput(result=20)

        @task
        def consumer(a: int, b: int, c: int) -> MathOutput:
            return MathOutput(result=a + b + c)

        s1 = source1()
        s2 = source2()
        
        # 从多个源映射，加上静态值
        c = consumer().map_inputs(
            a=s1.Output.result,
            b=s2.Output.result,
            c=5
        )

        ctx = InMemoryExecutionContext()
        s1.invoke(NO_INPUT, ctx)
        s2.invoke(NO_INPUT, ctx)
        
        result = c.invoke(NO_INPUT, ctx)
        self.assertEqual(result.result, 35)

    def test_pydantic_model_validation(self):
        """测试 Pydantic 模型验证"""
        @task
        def strict_task(val: int, text: str) -> StringOutput:
            return StringOutput(content=f"{val}:{text}")

        worker = strict_task()
        
        # 正常调用
        result = worker.invoke({"val": 10, "text": "hello"})
        self.assertEqual(result.content, "10:hello")
        
        # 类型错误应该被捕获
        with self.assertRaises(ValueError):
            worker.invoke({"val": "not_int", "text": "hello"})
        
        # 缺少字段应该被捕获
        with self.assertRaises(ValueError):
            worker.invoke({"val": 10})

    def test_context_passing(self):
        """测试上下文传递"""
        @task
        def producer() -> str:
            return "data"

        @task
        def consumer(data: str, context: ExecutionContext) -> str:
            # 可以访问上下文
            return f"{data}-{len(context.node_outputs)}"

        p = producer()
        c = consumer().map_inputs(data=p.Output.result)
        
        ctx = InMemoryExecutionContext()
        p.invoke(NO_INPUT, ctx)
        result = c.invoke(NO_INPUT, ctx)
        # @task 返回的是 Pydantic 模型
        result_str = result.result if hasattr(result, 'result') else result
        self.assertIn("data", str(result_str))

    def test_error_handler_runnable(self):
        """测试错误处理器（Runnable 类型）"""
        @task
        def failing_task() -> str:
            raise ValueError("Error")

        @task
        def handler() -> str:
            return "Recovered"

        worker = failing_task()
        worker.on_error(handler())
        
        result = worker.invoke(NO_INPUT)
        # @task 返回的是 Pydantic 模型
        result_val = result.result if hasattr(result, 'result') else result
        self.assertEqual(result_val, "Recovered")

    def test_copy_runnable(self):
        """测试 Runnable 复制"""
        @task
        def simple_task(val: int) -> int:
            return val * 2

        original = simple_task(use_cache=True)
        original.invoke({"val": 5})  # 填充缓存
        
        copied = original.copy()
        
        # 复制的应该清除缓存
        self.assertEqual(len(copied._invoke_cache), 0)
        self.assertNotEqual(id(original), id(copied))

if __name__ == "__main__":
    unittest.main()