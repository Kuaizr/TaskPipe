"""
Example: WorkflowGraph with async parallel stages and explicit data mappings.

This example demonstrates:
1. WorkflowGraph for declarative workflow definition
2. AsyncRunnable and async @task decorator
3. Parallel execution of independent tasks
4. Explicit data mappings between nodes
5. Mixed sync/async tasks in the same workflow

Run with: `python examples/parallel_report_workflow.py`
"""

from __future__ import annotations

import asyncio
import time
from typing import Callable

from pydantic import BaseModel

from taskpipe import (
    AsyncRunnable,
    InMemoryExecutionContext,
    NO_INPUT,
    WorkflowGraph,
    task,
)


class ContextPayload(BaseModel):
    user: str
    ticket: int


class ScreenshotPayload(BaseModel):
    path: str
    ticket: int


class AnalysisPayload(BaseModel):
    summary: str
    ticket: int


class EmailPayload(BaseModel):
    body: str


class EmailStatus(BaseModel):
    status: str


class CaptureScreenshot(AsyncRunnable):
    class InputModel(BaseModel):
        user: str
        ticket: int

    class OutputModel(ScreenshotPayload):
        pass

    def __init__(self, delay: float = 0.6):
        super().__init__(name="CaptureScreenshot")
        self.delay = delay

    async def _internal_invoke_async(self, input_data: InputModel, context):
        await asyncio.sleep(self.delay)
        payload = ScreenshotPayload(path="/tmp/dashboard.png", ticket=input_data.ticket)
        context.log_event(f"[{self.name}] finished")
        return payload


class AnalyzeScreenshot(AsyncRunnable):
    class InputModel(BaseModel):
        user: str
        ticket: int

    class OutputModel(AnalysisPayload):
        pass

    def __init__(self, delay: float = 0.6):
        super().__init__(name="AnalyzeScreenshot")
        self.delay = delay

    async def _internal_invoke_async(self, input_data: InputModel, context):
        await asyncio.sleep(self.delay)
        payload = AnalysisPayload(
            summary=f"Alert count for {input_data.user} is nominal.",
            ticket=input_data.ticket,
        )
        context.log_event(f"[{self.name}] finished")
        return payload


@task
def collect_context() -> ContextPayload:
    """Synchronous task using @task decorator."""
    return ContextPayload(user="alex", ticket=42)


@task
async def collect_context_async() -> ContextPayload:
    """Asynchronous task using @task decorator - automatically generates AsyncRunnable."""
    await asyncio.sleep(0.1)  # Simulate async I/O
    return ContextPayload(user="alex_async", ticket=42)


@task
def assemble_email(image_path: str, summary: str, ticket: int) -> EmailPayload:
    """Assemble email from multiple inputs."""
    body = (
        f"Daily Report #{ticket}\n\n"
        f"- Summary: {summary}\n"
        f"- Screenshot: {image_path}\n"
    )
    return EmailPayload(body=body)


@task
async def send_email_async(email: EmailPayload) -> EmailStatus:
    """Asynchronous email sending task."""
    await asyncio.sleep(0.2)  # Simulate async email sending
    return EmailStatus(status=f"EMAIL SENT (async)\n{email.body}")


@task
def send_email(email: EmailPayload) -> EmailStatus:
    """Synchronous email sending task."""
    # Handle both Pydantic model and dict input
    if isinstance(email, dict):
        body = email.get("body", "")
    else:
        body = email.body
    return EmailStatus(status=f"EMAIL SENT\n{body}")


def build_graph() -> WorkflowGraph:
    graph = WorkflowGraph(name="AsyncDailyReport")

    collect = collect_context()
    screenshot = CaptureScreenshot()
    analyze = AnalyzeScreenshot()
    merge = assemble_email()
    dispatch = send_email()

    graph.add_node(collect, "CollectContext")
    graph.add_node(screenshot, "CaptureScreenshot")
    graph.add_node(analyze, "AnalyzeScreenshot")
    graph.add_node(merge, "AssembleEmail")
    graph.add_node(dispatch, "SendEmail")

    graph.add_edge("CollectContext", "CaptureScreenshot", data_mapping={"user": "user", "ticket": "ticket"})
    graph.add_edge("CollectContext", "AnalyzeScreenshot", data_mapping={"user": "user", "ticket": "ticket"})
    graph.add_edge("CaptureScreenshot", "AssembleEmail", data_mapping={"image_path": "path", "ticket": "ticket"})
    graph.add_edge("AnalyzeScreenshot", "AssembleEmail", data_mapping={"summary": "summary", "ticket": "ticket"})
    graph.add_edge("AssembleEmail", "SendEmail", data_mapping={"email": "*"})

    graph.set_entry_point("CollectContext")
    graph.set_output_nodes(["SendEmail"])
    return graph


async def main() -> None:
    print("=" * 60)
    print("Example: Async Parallel Workflow with WorkflowGraph")
    print("=" * 60)
    
    graph = build_graph()
    compiled = graph.compile()
    ctx = InMemoryExecutionContext()

    start = time.perf_counter()
    result = await compiled.invoke_async(NO_INPUT, ctx)
    duration = time.perf_counter() - start

    print("\nWorkflow result:")
    result_status = result.status if hasattr(result, 'status') else result
    print(f"  Status: {result_status}")
    print(f"  Total duration: {duration:.2f}s")
    print(f"  Note: Parallel stage (CaptureScreenshot + AnalyzeScreenshot) should take ~= max task delay (~0.6s)")

    print("\n" + "=" * 60)
    print("Execution Stages (for parallel execution):")
    print("=" * 60)
    for i, stage in enumerate(compiled.execution_stages, 1):
        print(f"  Stage {i}: {stage}")

    print("\n" + "=" * 60)
    print("Recent Status Events:")
    print("=" * 60)
    for event in ctx.node_status_events[-8:]:
        print("  ", event)
    
    print("\n" + "=" * 60)
    print("Event Log (last 6 events):")
    print("=" * 60)
    for event in ctx.event_log[-6:]:
        print("  ", event)


if __name__ == "__main__":
    asyncio.run(main())
