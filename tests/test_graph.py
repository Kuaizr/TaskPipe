import unittest
from pydantic import BaseModel
from taskpipe import (
    task,
    WorkflowGraph,
    START,
    END,
    WaitForInput,
    SuspendExecution,
    InMemoryExecutionContext,
    NO_INPUT,
    RunnableRegistry
)

class InputModel(BaseModel):
    user: str

class OutputModel(BaseModel):
    msg: str

@task
def task_a() -> str: return "DataA"

@task
def greet(name: str) -> str:
    return f"Hi {name}"

class TestGraphFeatures(unittest.TestCase):
    def test_auto_contract_injection(self):
        """测试 Graph 编译时自动继承 START/END 的契约"""
        g = WorkflowGraph(name="ContractGraph")
        g.add_node(START(InputModel), "S")
        g.add_node(END(OutputModel), "E")
        g.add_edge("S", "E", data_mapping={"msg": "user"}) 
        
        compiled = g.compile()
        self.assertEqual(compiled.InputModel, InputModel)
        self.assertEqual(compiled.OutputModel, OutputModel)
        
        res = compiled.invoke({"user": "Alice"})
        self.assertEqual(res.msg, "Alice")

    def test_graph_deep_mapping(self):
        """测试图执行引擎支持深层字段映射 (Deep Mapping)"""
        @task
        def get_nested() -> dict:
            return {"user": {"profile": {"name": "Bob"}}}
            
        g = WorkflowGraph(name="DeepMapGraph")
        g.add_node(get_nested(), "Source")
        g.add_node(greet(), "Target")
        
        # 关键：映射 deep path
        g.add_edge("Source", "Target", data_mapping={"name": "result.user.profile.name"})
        
        g.set_entry_point("Source")
        g.set_output_nodes(["Target"])
        
        compiled = g.compile()
        res = compiled.invoke()
        
        val = res.result if hasattr(res, "result") else res
        self.assertEqual(val, "Hi Bob")

    def test_bypass_input_nodes(self):
        """测试 '旁路输入节点'：没有输入依赖的独立配置节点"""
        @task
        def config_provider() -> str:
            return "ConfigVal"
            
        g = WorkflowGraph(name="BypassGraph")
        g.add_node(START(InputModel), "Start") # 标准入口
        g.add_node(config_provider(), "Config") # 旁路入口 (In-degree 0)
        
        @task
        def consumer(main: str, cfg: str) -> str:
            return f"{main}-{cfg}"
            
        g.add_node(consumer(), "Consumer")
        
        g.add_edge("Start", "Consumer", data_mapping={"main": "user"})
        g.add_edge("Config", "Consumer", data_mapping={"cfg": "result"})
        
        # [修复] 显式将 Config 设为入口，因为它是该图的必要数据源之一
        g.set_entry_point("Start").set_entry_point("Config")
        g.set_output_nodes(["Consumer"])
        
        compiled = g.compile()
        res = compiled.invoke({"user": "User"})
        
        val = res.result if hasattr(res, "result") else res
        self.assertEqual(val, "User-ConfigVal")

    def test_graph_resume_with_context(self):
        """测试图恢复时的上下文一致性与状态恢复"""
        wait = WaitForInput()
        
        @task
        def task_b(a_data: str, user_input: str) -> str:
            return f"{a_data}+{user_input}"
            
        g = WorkflowGraph(name="ResumeGraph")
        g.add_node(task_a(), "A")
        g.add_node(wait, "Wait")
        g.add_node(task_b(), "B")
        
        g.add_edge("A", "Wait") # A -> Wait
        g.add_edge("Wait", "B", data_mapping={"user_input": "result"})
        g.add_edge("A", "B", data_mapping={"a_data": "result"}) # A -> B (跨节点传递)
        
        g.set_entry_point("A")
        g.set_output_nodes(["B"])
        
        compiled = g.compile()
        
        # 1. 第一次运行，在 Wait 处挂起
        try:
            compiled.invoke()
        except SuspendExecution as e:
            snapshot = e.snapshot
            # 验证快照包含了上下文数据 (A 的输出)
            self.assertIn("ctx_data", snapshot)
            self.assertIn("A", snapshot["ctx_data"])
            
            # 验证节点状态：A 完成，Wait 挂起
            self.assertEqual(snapshot["node_states"]["A"], "completed")
            self.assertEqual(snapshot["node_states"]["Wait"], "pending")
            
            # 2. 构造恢复状态
            snapshot["child_states"] = {"Wait": "Input"} 
            
            # 3. 恢复运行
            res = compiled.invoke(NO_INPUT, resume_state=snapshot)
            
            final = res.result if hasattr(res, "result") else res
            self.assertEqual(final, "DataA+Input")

    def test_serialization_roundtrip(self):
        """测试图的 JSON 序列化与反序列化 (简单冒烟测试)"""
        g = WorkflowGraph(name="SerializeMe")
        g.add_node(task_a(), "A")
        g.add_node(greet(), "B")
        g.add_edge("A", "B", data_mapping={"name": "result"})
        
        json_data = g.to_json()
        
        registry = RunnableRegistry()
        registry.register("A", task_a)
        registry.register("B", greet)
        # Mock ref in json to match registry keys
        json_data["nodes"][0]["ref"] = "A"
        json_data["nodes"][1]["ref"] = "B"
        
        g2 = WorkflowGraph.from_json(json_data, registry)
        self.assertEqual(len(g2.nodes), 2)
        self.assertEqual(len(g2.edges), 1)

if __name__ == "__main__":
    unittest.main()