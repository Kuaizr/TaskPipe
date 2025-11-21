import unittest
from typing import Any
from pydantic import BaseModel, ValidationError

from taskpipe import (
    InMemoryExecutionContext,
    NO_INPUT,
    Runnable,
    task,
    Pipeline,
    Conditional
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
        def source() -> MathOutput:
            return MathOutput(result=10)

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
        self.assertEqual(res, 100)
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
        self.assertEqual(res, "Success")
        self.assertEqual(events, ["start", "complete_ok:Success"])

        # 2. 测试失败并恢复路径
        events.clear()
        worker.on_error(error_handler) # 添加错误处理
        
        res = worker.invoke({"fail": True})
        self.assertEqual(res, "Recovered") # 被 on_error 覆盖
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
        # 注意：中间步骤如果是 dict/int 传递，add_one 需能接收。
        # 当前 @task 会自动处理单一参数的解包。
        self.assertEqual(res, 3)

    def test_conditional_operator(self):
        """测试 % >> 操作符 (同步)"""
        @task
        def is_even(val: int) -> bool:
            return val % 2 == 0

        @task
        def handle_true(val: int) -> str:
            return "Even"

        @task
        def handle_false(val: int) -> str:
            return "Odd"

        logic = is_even() % handle_true() >> handle_false()
        
        self.assertEqual(logic.invoke({"val": 2}), "Even")
        self.assertEqual(logic.invoke({"val": 3}), "Odd")

    def test_conditional_set_check(self):
        """测试 set_check 自定义检查函数"""
        @task
        def process(val: int) -> int:
            return val

        # 自定义 check: val > 5
        logic = process().set_check(lambda x: x > 5) % task(lambda x: "Big")() >> task(lambda x: "Small")()
        
        self.assertEqual(logic.invoke({"val": 10}), "Big")
        self.assertEqual(logic.invoke({"val": 1}), "Small")

if __name__ == "__main__":
    unittest.main()