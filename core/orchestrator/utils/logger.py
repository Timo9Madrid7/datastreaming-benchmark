import loguru
import sys

logger = loguru.logger

logger.remove()

logger.add(
    sys.stdout, 
    level = "DEBUG", 
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green>|<level>{level}</level>|<cyan>{module}</cyan>: <level>{message}</level>",
    enqueue=True,
    backtrace=False,
    diagnose=False,
    colorize=True
)