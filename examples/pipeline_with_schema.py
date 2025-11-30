"""
Example: Type-safe pipeline with Pydantic models and V2 Deep Mapping features.

This example demonstrates:
1. Custom Runnable with InputModel/OutputModel/ConfigModel.
2. @task decorator for function-to-Runnable conversion.
3. Pipeline composition using | operator (Code-First approach).
4. **Deep Data Mapping**: Accessing nested fields (e.g., `Output.data.usage.cpu`) directly.

Run with: `python examples/pipeline_with_schema.py`
"""

from __future__ import annotations
from pydantic import BaseModel
from taskpipe import InMemoryExecutionContext, Runnable, task, NO_INPUT

# ==========================
# 1. 定义数据模型 (支持嵌套)
# ==========================
class MetricsInput(BaseModel):
    cpu: float
    ram: float

class UsageDetail(BaseModel):
    cpu_percent: float
    ram_percent: float

class MetricsResult(BaseModel):
    status: str
    summary: str
    # 嵌套对象，用于演示深层映射
    details: UsageDetail

class ReportPayload(BaseModel):
    text: str
    severity: str

class ThresholdConfig(BaseModel):
    cpu_limit: float = 0.80
    ram_limit: float = 0.75

# ==========================
# 2. 定义任务
# ==========================
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
        
        summary = f"System Load: CPU={input_data.cpu:.0%}, RAM={input_data.ram:.0%}"
        
        context.log_event(f"[EvaluateUsage] {summary} => {status}")
        
        return MetricsResult(
            status=status,
            summary=summary,
            details=UsageDetail(
                cpu_percent=input_data.cpu * 100,
                ram_percent=input_data.ram * 100
            )
        )

@task
def format_report(status: str, summary: str) -> ReportPayload:
    """Standard formatter using top-level fields."""
    severity = "HIGH" if status == "alert" else "LOW"
    return ReportPayload(
        text=f"[{status.upper()}] {summary}",
        severity=severity
    )

@task
def deep_insight_reporter(cpu_val: float, status: str) -> ReportPayload:
    """
    A reporter that needs specific nested data.
    """
    return ReportPayload(
        text=f"Detailed CPU Analysis: {cpu_val:.1f}% (System Status: {status})",
        severity="INFO"
    )

# ==========================
# 3. 运行示例
# ==========================
def main() -> None:
    print("=" * 60)
    print("Example 1: Basic Pipeline & Schema Validation")
    print("=" * 60)
    
    # Configure task
    evaluator = EvaluateUsage(
        config={"cpu_limit": 0.65, "ram_limit": 0.70}, 
        name="EvaluateUsage"
    )
    
    # Map top-level fields
    formatter = format_report().map_inputs(
        status=evaluator.Output.status,
        summary=evaluator.Output.summary
    )
    
    # Linear composition: A | B
    pipeline = evaluator | formatter

    ctx = InMemoryExecutionContext()
    alert = pipeline.invoke({"cpu": 0.92, "ram": 0.4}, ctx)
    print("Alert Output:", alert.text)
    print("Severity:", alert.severity)

    print("\n" + "=" * 60)
    print("Example 2: V2 Deep Data Mapping (New Feature)")
    print("=" * 60)
    
    # 演示 V2 新特性：直接映射深层嵌套字段
    # evaluator.Output.details.cpu_percent 会自动解析为 result['details']['cpu_percent']
    
    deep_reporter = deep_insight_reporter().map_inputs(
        # [关键] 链式访问深层字段
        cpu_val=evaluator.Output.details.cpu_percent, 
        status=evaluator.Output.status
    )
    
    pipeline2 = evaluator | deep_reporter
    
    res = pipeline2.invoke({"cpu": 0.45, "ram": 0.55})
    print("Deep Mapped Output:", res.text)
    # Expected: Detailed CPU Analysis: 45.0% (System Status: ok)

    print("\n" + "=" * 60)
    print("Event Log:")
    for event in ctx.event_log:
        print("  ", event)

if __name__ == "__main__":
    main()