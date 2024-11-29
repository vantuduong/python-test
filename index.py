from random import random

from metric_collector import collect_metrics, metrics_collector
import time

@collect_metrics
def example_function():
    time_sleep = random()
    time.sleep(time_sleep)
    print(f"Function executed for {time_sleep}.")
    if time_sleep > 0.7:
        raise Exception



if __name__ == '__main__':
    example_function()

    metrics = metrics_collector.get_metrics("example_function")
    print(metrics)
