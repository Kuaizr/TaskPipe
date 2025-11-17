"""
Example: type-safe pipeline using Pydantic Input/Output/Config models.
Run with: `python examples/pipeline_with_schema.py`
"""

from __future__ import annotations

from pydantic import BaseModel

from taskpipe import InMemoryExecutionContext, Runnable, SimpleTask


class MetricsInput(BaseModel):
    cpu: float
    ram: float


class MetricsResult(BaseModel):
    status: str
    summary: str


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


format_report = SimpleTask(
    lambda metrics: f"[{metrics.status.upper()}] {metrics.summary}",
    name="FormatReport",
)


def main() -> None:
    evaluator = EvaluateUsage(config={"cpu_limit": 0.65, "ram_limit": 0.70}, name="EvaluateUsage")
    pipeline = evaluator | format_report

    ctx = InMemoryExecutionContext()
    healthy = pipeline.invoke({"cpu": 0.5, "ram": 0.55}, ctx)
    print("Healthy pipeline output:", healthy)

    alert = pipeline.invoke({"cpu": 0.92, "ram": 0.4}, ctx)
    print("Alert pipeline output:", alert)

    print("\nLogged events:")
    for event in ctx.event_log[-2:]:
        print("  ", event)

    print("\nStatus events:")
    for event in ctx.node_status_events[-4:]:
        print("  ", event)


if __name__ == "__main__":
    main()

