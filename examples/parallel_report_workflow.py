"""
Example: WorkflowGraph with async parallel stages.
Run with: `python examples/parallel_report_workflow.py`
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from taskpipe import (
    AsyncRunnable,
    InMemoryExecutionContext,
    MergeInputs,
    NO_INPUT,
    SimpleTask,
    WorkflowGraph,
)


class AsyncDelayTask(AsyncRunnable):
    def __init__(self, name: str, delay: float, payload_factory):
        super().__init__(name=name)
        self.delay = delay
        self.payload_factory = payload_factory

    async def _internal_invoke_async(self, input_data: Any, context: InMemoryExecutionContext) -> Any:
        context.log_event(f"[{self.name}] started with input={input_data}")
        await asyncio.sleep(self.delay)
        payload = self.payload_factory(input_data)
        context.log_event(f"[{self.name}] finished")
        return payload


def build_graph() -> WorkflowGraph:
    graph = WorkflowGraph(name="AsyncDailyReport")

    collect_context = SimpleTask(lambda: {"user": "alex", "ticket": 42}, name="CollectContext")
    screenshot = AsyncDelayTask(
        "CaptureScreenshot",
        delay=0.6,
        payload_factory=lambda ctx: {"path": "/tmp/dashboard.png", "ticket": ctx["ticket"]},
    )
    analyze = AsyncDelayTask(
        "AnalyzeScreenshot",
        delay=0.6,
        payload_factory=lambda ctx: {
            "summary": f"Alert count for {ctx['user']} is nominal.",
            "ticket": ctx["ticket"],
        },
    )

    def render_email(image, analysis):
        return f"""
Daily Report #{analysis['ticket']}

- Summary: {analysis['summary']}
- Screenshot: {image['path']}
"""

    merge = MergeInputs(
        {"image": "CaptureScreenshot", "analysis": "AnalyzeScreenshot"},
        merge_function=render_email,
        name="AssembleEmail",
    )
    send_email = SimpleTask(lambda body: f"EMAIL SENT\n{body}", name="SendEmail")

    graph.add_node(collect_context, "CollectContext")
    graph.add_node(screenshot, "CaptureScreenshot")
    graph.add_node(analyze, "AnalyzeScreenshot")
    graph.add_node(merge, "AssembleEmail")
    graph.add_node(send_email, "SendEmail")

    graph.add_edge("CollectContext", "CaptureScreenshot")
    graph.add_edge("CollectContext", "AnalyzeScreenshot")
    graph.add_edge("CaptureScreenshot", "AssembleEmail")
    graph.add_edge("AnalyzeScreenshot", "AssembleEmail")
    graph.add_edge("AssembleEmail", "SendEmail")

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

    print("\nExecution stages:", compiled.execution_stages)
    print("\nRecent status events:")
    for event in ctx.node_status_events[-8:]:
        print("  ", event)


if __name__ == "__main__":
    asyncio.run(main())

