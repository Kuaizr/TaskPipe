"""
Example: WorkflowGraph with async parallel stages and explicit data mappings.
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
    return ContextPayload(user="alex", ticket=42)


@task
def assemble_email(image_path: str, summary: str, ticket: int) -> EmailPayload:
    body = (
        f"Daily Report #{ticket}\n\n"
        f"- Summary: {summary}\n"
        f"- Screenshot: {image_path}\n"
    )
    return EmailPayload(body=body)


@task
def send_email(email: EmailPayload) -> EmailStatus:
    return EmailStatus(status=f"EMAIL SENT\n{email.body}")


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
    graph.add_edge("AssembleEmail", "SendEmail", data_mapping={"email": "body"})

    graph.set_entry_point("CollectContext")
    graph.set_output_nodes(["SendEmail"])
    return graph


async def main() -> None:
    graph = build_graph()
    compiled = graph.compile()
    ctx = InMemoryExecutionContext()

    start = time.perf_counter()
    result = await compiled.invoke_async(NO_INPUT, ctx)
    duration = time.perf_counter() - start

    print("Workflow result:\n", result)
    print(f"Total duration: {duration:.2f}s (parallel stage should be ~= max task delay)")

    print("\nRecent status events:")
    for event in ctx.node_status_events[-8:]:
        print("  ", event)


if __name__ == "__main__":
    asyncio.run(main())
