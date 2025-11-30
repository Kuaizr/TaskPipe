"""
Example: Advanced TaskPipe features.

This example demonstrates:
1. Loop (replacing While/AgentLoop).
2. Map (Dynamic Parallelism).
3. Error handling & Retry.
4. Suspend/Resume basics.

Run with: `python examples/advanced_features.py`
"""

from __future__ import annotations
import time
from pydantic import BaseModel
from taskpipe import (
    task, Loop, Map, InMemoryExecutionContext, NO_INPUT
)

class NumberState(BaseModel):
    count: int

@task
def risky_task() -> int:
    import random
    if random.random() < 0.7:
        raise ValueError("Random glitch!")
    return 100

def example_retry():
    print("=" * 60)
    print("1. Retry Mechanism")
    print("=" * 60)
    
    # Retry 3 times
    worker = risky_task().retry(max_attempts=5, delay_seconds=0.1)
    
    try:
        res = worker.invoke(NO_INPUT)
        print(f"Success: {res.result}")
    except Exception as e:
        print(f"Failed after retries: {e}")

def example_loop():
    print("\n" + "=" * 60)
    print("2. Loop (Accumulator)")
    print("=" * 60)
    
    @task
    def increment(count: int) -> int:
        print(f"  Looping... count={count}")
        return count + 1
        
    # Loop runs while 'count < 5'
    # 'loop_count' is an internal variable available in condition
    loop = Loop(
        body=increment(),
        config={"condition": "count < 5"} 
    )
    
    res = loop.invoke(0)
    # Result is the output of the last iteration
    val = res.result if hasattr(res, "result") else res
    print(f"Final Value: {val}")

def example_map():
    print("\n" + "=" * 60)
    print("3. Map (Dynamic Parallelism)")
    print("=" * 60)
    
    @task
    def process_item(item_id: int) -> str:
        # Simulate work
        time.sleep(0.1)
        return f"Processed-{item_id}"
    
    mapper = Map(
        body=process_item(),
        config={"max_concurrency": 4}
    )
    
    items = [{"item_id": i} for i in range(1, 6)]
    print(f"Mapping over {len(items)} items...")
    
    results = mapper.invoke(items)
    
    clean_results = [r.result if hasattr(r, 'result') else r for r in results]
    print(f"Results: {clean_results}")

if __name__ == "__main__":
    example_retry()
    example_loop()
    example_map()