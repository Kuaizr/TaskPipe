"""
Example: WorkflowGraph with START/END and parallel stages.
"""
import time
import asyncio
from pydantic import BaseModel
from taskpipe import (
    AsyncRunnable, WorkflowGraph, task, START, END, NO_INPUT
)

class ContextPayload(BaseModel):
    user: str; ticket: int
class EmailStatus(BaseModel):
    status: str
class ScreenshotPayload(BaseModel):
    path: str; ticket: int
class AnalysisPayload(BaseModel):
    summary: str; ticket: int
class EmailPayload(BaseModel):
    body: str

class CaptureScreenshot(AsyncRunnable):
    class InputModel(BaseModel): user: str; ticket: int
    class OutputModel(ScreenshotPayload): pass
    async def _internal_invoke_async(self, input_data, context):
        await asyncio.sleep(0.2)
        return ScreenshotPayload(path="/tmp/img.png", ticket=input_data.ticket)

class AnalyzeScreenshot(AsyncRunnable):
    class InputModel(BaseModel): user: str; ticket: int
    class OutputModel(AnalysisPayload): pass
    async def _internal_invoke_async(self, input_data, context):
        await asyncio.sleep(0.2)
        return AnalysisPayload(summary="All good", ticket=input_data.ticket)

@task
def collect_context(user: str, ticket: int) -> ContextPayload:
    return ContextPayload(user=user, ticket=ticket)

@task
def assemble_email(image_path: str, summary: str) -> EmailPayload:
    return EmailPayload(body=f"Summary: {summary}, Img: {image_path}")

@task
def send_email(email: EmailPayload) -> EmailStatus:
    # 兼容 dict 输入
    body_text = email.body if hasattr(email, "body") else email["body"]
    return EmailStatus(status=f"SENT: {body_text}")

def build_graph():
    graph = WorkflowGraph(name="AsyncReport")
    
    start = START(ContextPayload)
    end = END(EmailStatus)
    
    graph.add_node(start, "Start")
    graph.add_node(collect_context(), "Collect")
    graph.add_node(CaptureScreenshot(), "Screenshot")
    graph.add_node(AnalyzeScreenshot(), "Analyze")
    graph.add_node(assemble_email(), "Assemble")
    graph.add_node(send_email(), "Send")
    graph.add_node(end, "End")
    
    graph.add_edge("Start", "Collect", data_mapping={"user": "user", "ticket": "ticket"})
    graph.add_edge("Collect", "Screenshot", data_mapping={"user": "user", "ticket": "ticket"})
    graph.add_edge("Collect", "Analyze", data_mapping={"user": "user", "ticket": "ticket"})
    graph.add_edge("Screenshot", "Assemble", data_mapping={"image_path": "path"})
    graph.add_edge("Analyze", "Assemble", data_mapping={"summary": "summary"})
    graph.add_edge("Assemble", "Send", data_mapping={"email": "*"})
    graph.add_edge("Send", "End", data_mapping={"status": "status"})
    
    return graph.compile()

async def main():
    print("Running Async Graph...")
    compiled = build_graph()
    start = time.perf_counter()
    res = await compiled.invoke_async({"user": "Alice", "ticket": 101})
    duration = time.perf_counter() - start
    
    status = res.status if hasattr(res, "status") else res.get("status")
    print(f"Result: {status}")
    print(f"Duration: {duration:.2f}s (Should be parallel)")

if __name__ == "__main__":
    asyncio.run(main())