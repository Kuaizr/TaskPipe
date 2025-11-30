"""
Example: Chat LLM with Human-in-the-loop (HITL), Streaming, and Loop.

This example demonstrates:
1. Long-running Loop for conversation management.
2. WaitForInput for pausing/resuming execution (HITL).
3. Switch for control flow (Chat vs Exit).
4. Streaming output simulation via context bubbling.
5. Simplified Resume Logic using CompiledGraph.prepare_resume().

Run with: `python examples/chat_llm.py`
"""

import time
import sys
from typing import List, Dict, Any
from pydantic import BaseModel, Field
from taskpipe import (
    task, START, END, Switch, Loop, WaitForInput, 
    InMemoryExecutionContext, SuspendExecution, CompiledGraph
)

# ==========================
# 1. 定义数据契约 (Contract)
# ==========================
class ChatContext(BaseModel):
    history: List[Dict[str, str]] = Field(default_factory=list)
    last_user_input: str = ""
    status: str = "CONTINUE"  # "CONTINUE" | "EXIT"

# ==========================
# 2. 定义业务原子能力 (Tasks)
# ==========================
@task
def llm_chat(user_input: str, history: List[Dict[str, str]], context: InMemoryExecutionContext) -> ChatContext:
    """业务逻辑：调用 LLM 生成回复，支持流式输出"""
    # 模拟 LLM 生成
    response_text = f"复读机: {user_input}"
    
    # 模拟流式推送 (Context 会自动向上冒泡到主程序)
    for char in response_text:
        context.send_stream("DoChat", char)
        time.sleep(0.05) # 模拟网络延迟
    
    new_history = history + [
        {"role": "user", "content": user_input},
        {"role": "ai", "content": response_text}
    ]
    return ChatContext(history=new_history, last_user_input=user_input, status="CONTINUE")

@task
def exit_logic(history: List[Dict[str, str]]) -> ChatContext:
    """业务逻辑：处理退出"""
    print("\n[系统] 正在保存会话记录...")
    return ChatContext(history=history, status="EXIT")

# ==========================
# 3. 核心：构建单轮对话子图
# ==========================
def build_chat_round_graph():
    # --- A. 实例化节点 ---
    start_node = START(ChatContext, name="Start")
    
    # 暂停点：等待人类输入
    # 注意：WaitForInput 默认输出 {"result": ...}
    wait_node = WaitForInput(name="HumanInput")
    
    # 逻辑判断：如果输入是 /bye 则退出
    switch_node = Switch(
        config={
            "rules": [("user_input == '/bye'", "EXIT_BRANCH")],
            "default_branch": "CHAT_BRANCH"
        },
        name="Router"
    )
    
    # 分支任务：聊天
    # map_inputs 指定数据来源，TaskPipe 会自动建立跨节点的数据依赖
    chat_branch = llm_chat(name="DoChat").map_inputs(
        user_input=wait_node.Output.result, 
        history=start_node.Output.history
    )
    
    # 分支任务：退出
    exit_branch = exit_logic(name="DoExit").map_inputs(
        history=start_node.Output.history
    )
    
    end_node = END(ChatContext, name="End")

    # --- B. 链式组装 (Pipeline DSL) ---
    # 使用括号 (switch | dict) 确保创建 SwitchBranches 结构
    pipeline_def = (
        start_node
        | wait_node
        | (
            switch_node.map_inputs(user_input=wait_node.Output.result)
            | {
                "CHAT_BRANCH": chat_branch,
                "EXIT_BRANCH": exit_branch
            }
        )
        | end_node
    )
    
    # 转换为编译好的图 (CompiledGraph)
    # 这会自动处理所有的隐式数据连接
    return pipeline_def.to_graph(graph_name="RoundGraph").compile()

# ==========================
# 4. 构建主程序 (Outer Loop)
# ==========================
def build_app():
    # 获取单轮对话逻辑（作为一个 CompiledGraph 节点）
    chat_body = build_chat_round_graph()
    
    # 只要 status != EXIT，就一直循环执行 chat_body
    main_loop = Loop(
        body=chat_body,
        config={
            "condition": "status != 'EXIT'", 
            "max_loops": 10
        },
        name="MainLoop"
    )
    return main_loop

# ==========================
# 5. 模拟运行 (Mock Runtime)
# ==========================
if __name__ == "__main__":
    app = build_app()
    ctx = InMemoryExecutionContext()
    
    # 注册流式回调 (实现打字机效果)
    def on_stream(node, chunk):
        if node == "DoChat":
            print(chunk, end="", flush=True)
    ctx.register_stream_callback(on_stream)

    # 初始化数据
    init_data = ChatContext(history=[])
    resume_payload = None
    
    print("=== AI 助理已启动 (输入 /bye 退出) ===")

    while True:
        try:
            # 执行应用
            # 如果是第一次运行，resume_payload 为 None
            # 如果是恢复运行，resume_payload 包含了之前的状态和新注入的输入
            result = app.invoke(init_data, context=ctx, resume_state=resume_payload)
            
            # 如果正常结束 (Loop 结束)
            print("\n[系统] 流程结束。")
            break

        except SuspendExecution as e:
            # === 捕获暂停信号 (HITL) ===
            # 此时工作流已暂停并保存了完整快照 (e.snapshot)
            # 在真实系统中，你可以将 snapshot 序列化存入数据库，然后结束进程
            
            user_text = input("\n👤 你: ")
            
            # === 构造恢复状态 (Resume State) ===
            # [修正] 直接使用 CompiledGraph.prepare_resume 处理整个快照
            # 它会自动穿透 Loop -> Graph -> 找到 "HumanInput" 并注入数据
            # 开发者完全不需要知道 snapshot 内部结构（如 loop_cnt, child_state 等）
            
            resume_payload = CompiledGraph.prepare_resume(
                e.snapshot, 
                inputs={"HumanInput": user_text}
            )
            
            print("🤖 ", end="")