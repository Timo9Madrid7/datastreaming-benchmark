import sys
from core.orchestrator.benchmark_manager import BenchmarkManager
from core.orchestrator.utils.logger import logger


if __name__ == "__main__":
    config_path = "benchmark_scenarios.json"
    benchmark_manager = BenchmarkManager(config_path, metrics_interval=0.1)
    mode = None
    duration_messages = "dm"
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    if len(sys.argv) > 2:
        duration_messages = sys.argv[2]
    i = 0
    while i < len(sys.argv):
        logger.debug(f"Argument {i}: {sys.argv[i]}")
        i += 1
    logger.info(f"Executing benchmark in mode {mode}")
    benchmark_manager.run(mode=mode,  duration_messages=duration_messages)
    