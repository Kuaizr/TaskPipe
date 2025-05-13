from taskpipe import SimpleTask, ExecutionContext, Runnable, NO_INPUT


def two() -> int:
    return 2

def twen() -> int:
    return 13

def process_large_value(val: int) -> str:
    return str(val * 10)

def process_small_value(val: int) -> str:
    return str(val + 5)

def process_add_three(val: str) -> str:
    return str(int(val) + 3)

# 创建 Runnable 实例
# 对于条件，我们需要一个 Runnable，其 check 方法定义条件逻辑
class ValueCondition(Runnable):
    def _internal_invoke(self, input_data: int, context: ExecutionContext) -> int:
        return input_data # Pass through
    def _default_check(self, data_from_invoke: int) -> bool:
        return data_from_invoke > 10 # 条件：值是否大于10

condition_runnable = ValueCondition(name="ValueCheck")
large_val_task = SimpleTask(process_large_value, name="ProcessLarge")
small_val_task = SimpleTask(process_small_value, name="ProcessSmall")
add_three = SimpleTask(process_add_three, name="addThree")
tt = SimpleTask(two, name="tt")
twenn = SimpleTask(twen, name="twenn")


work1 = tt | (condition_runnable % (large_val_task) >> (small_val_task)) |add_three
work2 = twenn | (condition_runnable % (large_val_task) >> (small_val_task)) |add_three

print("--- Input: 2 (should take true branch) ---")
ctx1 = ExecutionContext()
result1 = work1.invoke(NO_INPUT, ctx1)
print(f"Result for 2: {result1}")


print("\n--- Input: 13 (should take false branch) ---")
ctx2 = ExecutionContext()
result2 = work2.invoke(NO_INPUT, ctx2)
print(f"Result for 13: {result2}")
# --- 新增测试：合并 work1 和 work2 的输出 ---
from taskpipe.runnables import SourceParallel

def add_all_results(**results_kwargs: str) -> str:
    """
    接收包含 'work1_output' 和 'work2_output' 等键的关键字参数，
    将其值（字符串形式的数字）相加并返回字符串结果。
    """
    val1_str = results_kwargs.get("work1_output", "0")
    val2_str = results_kwargs.get("work2_output", "0")
    total = int(val1_str) + int(val2_str)
    return str(total)


add_all_task = SimpleTask(add_all_results, name="AddAllCombined")

# 构建最终的工作流
# parallel_tasks 的输出是一个字典: {"work1_output": "10", "work2_output": "133"}
# 这个字典会作为输入传递给 add_all_task
combined_workflow = SourceParallel({ "work1_output": work1, "work2_output": work2 }) | add_all_task

print("\n--- Testing combined workflow (work1 and work2 parallel, then add_all) ---")
ctx_combined = ExecutionContext()
# SourceParallel 中的 work1 和 work2 会使用它们内部定义的 NO_INPUT 行为
# （即 tt 和 twenn 分别作为它们的起点）
result_combined = combined_workflow.invoke(NO_INPUT, ctx_combined)
print(f"Result of combined_workflow: {result_combined}")

# 预期:
# work1 -> "10"
# work2 -> "133"
# add_all_results({"work1_output": "10", "work2_output": "133"}) -> "143"
# 所以 result_combined 应该是 "143"

# 我们可以检查一下上下文中的中间输出
# print(f"Output of ParallelWork1Work2 in context: {ctx_combined.get_output('ParallelWork1Work2')}")
# print(f"Output of AddAllCombined in context: {ctx_combined.get_output('AddAllCombined')}")
# --- 新增测试: 测试 Runnable_A | {"key1": Runnable_C, "key2": Runnable_D} 语法 ---
# combined_workflow 的输出是 "143"

def multiply_by_two(value_str: str) -> str:
    num = int(value_str)
    return str(num * 2)

def divide_by_two(value_str: str) -> str:
    num = float(value_str) # Use float for division
    return str(num / 2)

multiply_task = SimpleTask(multiply_by_two, name="MultiplyByTwo")
divide_task = SimpleTask(divide_by_two, name="DivideByTwo")

# 构建分支工作流
# combined_workflow 的输出 ("143") 将作为输入分别传递给 multiply_task 和 divide_task
branched_fan_out_workflow = combined_workflow | {
    "multiplied_value": multiply_task,
    "divided_value": divide_task
}

print("\n--- Testing branched fan-out workflow (combined_workflow | {mul, div}) ---")
ctx_branched = ExecutionContext()
# combined_workflow 内部会处理其 NO_INPUT 启动
result_branched = branched_fan_out_workflow.invoke(NO_INPUT, ctx_branched)
print(f"Result of branched_fan_out_workflow: {result_branched}")

# 预期:
# combined_workflow -> "143"
# multiply_task("143") -> "286"
# divide_task("143") -> "71.5"
# result_branched 应该是 {"multiplied_value": "286", "divided_value": "71.5"}

# 检查上下文中的中间输出
# print(f"Output of combined_workflow in branched context: {ctx_branched.get_output(combined_workflow.name)}")
# print(f"Output of MultiplyByTwo in branched context: {ctx_branched.get_output(multiply_task.name)}")
# print(f"Output of DivideByTwo in branched context: {ctx_branched.get_output(divide_task.name)}")
# print(f"Output of {branched_fan_out_workflow.name} in branched context: {ctx_branched.get_output(branched_fan_out_workflow.name)}")
# --- 新增测试: 测试 A | {"B": B_task, "C": C_task} | D_task 语法 ---
# branched_fan_out_workflow 的输出是 {'multiplied_value': '286', 'divided_value': '71.5'}

def summarize_results_from_dict_kwargs(**kwargs: str) -> str:
    """
    接收包含 'multiplied_value' 和 'divided_value' 等键的关键字参数,
    并生成一个总结字符串。
    """
    multiplied = kwargs.get("multiplied_value", "N/A_mul")
    divided = kwargs.get("divided_value", "N/A_div")
    return f"Final Summary: Multiplied value is {multiplied}, Divided value is {divided}."

summary_task = SimpleTask(summarize_results_from_dict_kwargs, name="SummarizePipelineOutput")

# 构建 A | {B,C} | D 形式的流水线
# branched_fan_out_workflow 是 A | {B,C} 部分
# summary_task 是 D 部分
pipeline_A_BC_D = branched_fan_out_workflow | summary_task

print("\n--- Testing A | {B,C} | D pipeline (branched_fan_out | summary) ---")
ctx_pipeline_A_BC_D = ExecutionContext()
# branched_fan_out_workflow 内部会处理其 NO_INPUT 启动
result_pipeline_A_BC_D = pipeline_A_BC_D.invoke(NO_INPUT, ctx_pipeline_A_BC_D)
print(f"Result of pipeline_A_BC_D: {result_pipeline_A_BC_D}")

# 预期:
# branched_fan_out_workflow -> {'multiplied_value': '286', 'divided_value': '71.5'}
# summary_task (input: {'multiplied_value': '286', 'divided_value': '71.5'})
# -> "Final Summary: Multiplied value is 286, Divided value is 71.5."
# result_pipeline_A_BC_D 应该是 "Final Summary: Multiplied value is 286, Divided value is 71.5."

# 检查上下文
# print(f"Output of branched_fan_out_workflow in final context: {ctx_pipeline_A_BC_D.get_output(branched_fan_out_workflow.name)}")
# print(f"Output of SummarizePipelineOutput in final context: {ctx_pipeline_A_BC_D.get_output(summary_task.name)}")
# --- 新增测试: 一步到位定义 A | {B,C} | D 工作流 ---
# (combined_workflow | {mul, div}) | summary

pipeline_one_step = (combined_workflow | {
    "multiplied_value": multiply_task,
    "divided_value": divide_task
}) | summary_task # summary_task 和 multiply_task, divide_task 已在前面定义

print("\n--- Testing A | {B,C} | D pipeline (one-step definition) ---")
ctx_one_step = ExecutionContext()
result_one_step = pipeline_one_step.invoke(NO_INPUT, ctx_one_step)
print(f"Result of pipeline_one_step: {result_one_step}")

# 预期结果与之前的 pipeline_A_BC_D 相同:
# "Final Summary: Multiplied value is 286, Divided value is 71.5."

# 验证一下名称 (可选)
# print(f"Name of pipeline_one_step: {pipeline_one_step.name}")
# 它会自动生成一个类似 Pipeline[(Pipeline[...]_then_BranchFanIn[...])_then_SummarizePipelineOutput] 的名称