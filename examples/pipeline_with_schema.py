"""
Example: Type-safe pipeline using Pydantic Input/Output/Config models and explicit data mapping.

This example demonstrates:
1. Custom Runnable with InputModel/OutputModel/ConfigModel
2. @task decorator for function-to-Runnable conversion
3. Pipeline composition using | operator
4. Explicit data mapping with map_inputs()
5. Pydantic model validation

Run with: `python examples/pipeline_with_schema.py`
"""

from __future__ import annotations

from pydantic import BaseModel

from taskpipe import InMemoryExecutionContext, Runnable, task


class MetricsInput(BaseModel):
    cpu: float
    ram: float


class MetricsResult(BaseModel):
    status: str
    summary: str


class ReportPayload(BaseModel):
    text: str
    severity: str


class ThresholdConfig(BaseModel):
    cpu_limit: float = 0.80
    ram_limit: float = 0.75


class EvaluateUsage(Runnable):
    """
    Demonstrates InputModel/OutputModel/ConfigModel on top of Runnable.
    """

    InputModel = MetricsInput
    OutputModel = MetricsResult
    ConfigModel = ThresholdConfig

    def _internal_invoke(self, input_data: MetricsInput, context: InMemoryExecutionContext) -> MetricsResult:
        over_cpu = input_data.cpu > self.config.cpu_limit
        over_ram = input_data.ram > self.config.ram_limit
        status = "alert" if (over_cpu or over_ram) else "ok"
        summary = (
            f"cpu={input_data.cpu:.2f}/{self.config.cpu_limit:.2f}, "
            f"ram={input_data.ram:.2f}/{self.config.ram_limit:.2f}"
        )
        context.log_event(f"[EvaluateUsage] {summary} => {status}")
        return {"status": status, "summary": summary}


@task
def format_report(status: str, summary: str) -> ReportPayload:
    """Format metrics into a report with severity level."""
    severity = "HIGH" if status == "alert" else "LOW"
    return ReportPayload(
        text=f"[{status.upper()}] {summary}",
        severity=severity
    )


@task
def add_timestamp(text: str, severity: str) -> ReportPayload:
    """Add timestamp to report text."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return ReportPayload(
        text=f"{text} | Generated at {timestamp}",
        severity=severity
    )


def main() -> None:
    print("=" * 60)
    print("Example 1: Basic Pipeline with | operator")
    print("=" * 60)
    
    evaluator = EvaluateUsage(config={"cpu_limit": 0.65, "ram_limit": 0.70}, name="EvaluateUsage")
    # Use map_inputs to map specific fields from evaluator's output
    formatter = format_report().map_inputs(
        status=evaluator.Output.status,
        summary=evaluator.Output.summary
    )
    pipeline = evaluator | formatter

    ctx = InMemoryExecutionContext()
    healthy = pipeline.invoke({"cpu": 0.5, "ram": 0.55}, ctx)
    print("Healthy pipeline output:", healthy.text)
    print("Severity:", healthy.severity)

    alert = pipeline.invoke({"cpu": 0.92, "ram": 0.4}, ctx)
    print("Alert pipeline output:", alert.text)
    print("Severity:", alert.severity)

    print("\n" + "=" * 60)
    print("Example 2: Explicit Data Mapping with map_inputs()")
    print("=" * 60)
    
    # Using map_inputs to explicitly map fields and add static values
    evaluator2 = EvaluateUsage(config={"cpu_limit": 0.65, "ram_limit": 0.70}, name="EvaluateUsage2")
    
    # Create a task that needs specific fields
    @task
    def custom_formatter(status: str, summary: str, threshold: float) -> ReportPayload:
        return ReportPayload(
            text=f"Custom: {status} - {summary} (threshold: {threshold})",
            severity="MEDIUM"
        )
    
    # Use map_inputs to map evaluator's output fields and add a static value
    custom_task = custom_formatter().map_inputs(
        status=evaluator2.Output.status,
        summary=evaluator2.Output.summary,
        threshold=0.70  # Static value
    )
    
    pipeline2 = evaluator2 | custom_task
    result2 = pipeline2.invoke({"cpu": 0.5, "ram": 0.55}, InMemoryExecutionContext())
    print("Custom formatted output:", result2.text)

    print("\n" + "=" * 60)
    print("Example 3: Multi-stage Pipeline")
    print("=" * 60)
    
    formatter3 = format_report().map_inputs(
        status=evaluator.Output.status,
        summary=evaluator.Output.summary
    )
    timestamp_task = add_timestamp().map_inputs(
        text=formatter3.Output.text,
        severity=formatter3.Output.severity
    )
    pipeline3 = evaluator | formatter3 | timestamp_task
    result3 = pipeline3.invoke({"cpu": 0.92, "ram": 0.4}, InMemoryExecutionContext())
    print("Multi-stage pipeline output:", result3.text)

    print("\n" + "=" * 60)
    print("Event Log (last 5 events):")
    print("=" * 60)
    for event in ctx.event_log[-5:]:
        print("  ", event)

    print("\n" + "=" * 60)
    print("Status Events (last 6 events):")
    print("=" * 60)
    for event in ctx.node_status_events[-6:]:
        print("  ", event)


if __name__ == "__main__":
    main()

