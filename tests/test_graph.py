import unittest
import asyncio
from pydantic import BaseModel

from taskpipe import (
    task, 
    WorkflowGraph, 
    InMemoryExecutionContext, 
    NO_INPUT,
    RunnableRegistry
)

class TestGraphBasics(unittest.TestCase):
    def test_graph_serialization(self):
        """测试图的 JSON 导出与注册表恢复"""
        @task
        def step1() -> str: return "A"
        @task
        def step2(s: str) -> str: return s + "B"

        graph = WorkflowGraph(name="TestGraph")
        graph.add_node(step1(), "S1")
        graph.add_node(step2(), "S2")
        graph.add_edge("S1", "S2", data_mapping={"s": "result"}) # S1输出result, S2输入s
        graph.set_entry_point("S1").set_output_nodes(["S2"])

        # 1. 导出
        json_data = graph.to_json()
        self.assertEqual(json_data["name"], "TestGraph")
        self.assertEqual(len(json_data["nodes"]), 2)
        self.assertEqual(len(json_data["edges"]), 1)

        # 2. 恢复 (使用 Registry)
        registry = RunnableRegistry()
        registry.register("S1", step1) # 注册工厂
        registry.register("S2", step2)
        
        # 为了测试恢复，我们需要 JSON 里的 ref 匹配 registry key
        # 默认 ref 是 node_name，这里我们手动 mock 一下让它匹配
        json_data["nodes"][0]["ref"] = "S1"
        json_data["nodes"][1]["ref"] = "S2"

        rebuilt = WorkflowGraph.from_json(json_data, registry)
        compiled = rebuilt.compile()
        
        res = compiled.invoke(NO_INPUT)
        self.assertEqual(res, "AB")

class TestGraphExpansion(unittest.TestCase):
    def test_conditional_expansion_structure(self):
        """
        测试 Conditional (% >>) 导出为图时，是否正确展开为：
        DataNode -> CheckAdapter -> Router -> Branches
        """
        @task
        def data_gen() -> int: return 10
        @task
        def true_path(v: int) -> str: return "T"
        @task
        def false_path(v: int) -> str: return "F"

        # 设置一个检查逻辑
        logic = data_gen().set_check(lambda x: x > 5) % true_path() >> false_path()
        
        graph = logic.to_graph("CondGraph")
        
        # 验证节点数量：
        # 1. data_gen
        # 2. CheckAdapter (自动生成)
        # 3. Router (自动生成)
        # 4. true_path
        # 5. false_path
        self.assertEqual(len(graph.nodes), 5)
        
        node_names = list(graph.nodes.keys())
        # 检查是否包含生成的特殊节点
        self.assertTrue(any("_CheckAdapter" in name for name in node_names))
        self.assertTrue(any("_Router" in name for name in node_names))

        # 编译运行验证逻辑依然正确
        compiled = graph.compile()
        res = compiled.invoke(NO_INPUT)
        self.assertEqual(res, "T") # 10 > 5

class TestContextGC(unittest.TestCase):
    def test_garbage_collection_sync(self):
        """测试同步执行时的内存回收 (Context GC)"""
        @task
        def producer() -> str: return "HeavyData"
        
        @task
        def consumer(d: str) -> str: return f"Consumed-{d}"
        
        @task
        def finalizer(d: str) -> str: return "Done"

        # 构建图: Producer -> Consumer -> Finalizer
        # Producer 的数据只被 Consumer 使用。
        # 当 Consumer 执行完，Producer 的数据应该被 GC。
        # Consumer 的数据被 Finalizer 使用。
        graph = WorkflowGraph(name="GCTest")
        graph.add_node(producer(), "P")
        graph.add_node(consumer(), "C")
        graph.add_node(finalizer(), "F")
        
        graph.add_edge("P", "C", data_mapping={"d": "result"})
        graph.add_edge("C", "F", data_mapping={"d": "result"})
        
        graph.set_entry_point("P").set_output_nodes(["F"])
        
        # 启用 GC 编译
        compiled = graph.compile(enable_gc=True)
        ctx = InMemoryExecutionContext()
        
        # 执行
        compiled.invoke(NO_INPUT, ctx)
        
        # 验证 Context 状态
        # "P" 应该被移除，因为它不是 output node，且下游已执行完
        self.assertNotIn("P", ctx.node_outputs)
        
        # "C" 也应该被移除
        self.assertNotIn("C", ctx.node_outputs)
        
        # "F" 是 output node，应该保留
        self.assertIn("F", ctx.node_outputs)
        self.assertEqual(ctx.node_outputs["F"], "Done")

    def test_garbage_collection_disabled(self):
        """测试禁用 GC 时数据是否保留"""
        @task
        def producer() -> str: return "KeepMe"
        @task
        def consumer(d: str) -> str: return "OK"

        graph = WorkflowGraph()
        graph.add_node(producer(), "P")
        graph.add_node(consumer(), "C")
        graph.add_edge("P", "C", data_mapping={"d": "result"})
        graph.set_entry_point("P")

        # 禁用 GC
        compiled = graph.compile(enable_gc=False)
        ctx = InMemoryExecutionContext()
        compiled.invoke(NO_INPUT, ctx)

        # P 应该还在
        self.assertIn("P", ctx.node_outputs)
        self.assertEqual(ctx.node_outputs["P"], "KeepMe")

if __name__ == "__main__":
    unittest.main()