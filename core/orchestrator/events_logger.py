import datetime
import os
from typing import Dict, Optional

import docker
import polars as pl

from .utils.logger import logger


class ContainerEventsLogger:
    def __init__(
        self,
        tech_name,
        scenario_name,
        scenario_config,
        separator=";",
        log_level="STUDY",
    ) -> None:
        self.tech_name = tech_name
        self.scenario_name = scenario_name
        self.log_file = os.path.join(
            "logs", scenario_config, tech_name, f"{scenario_name}_events.parquet"
        )
        self.client = docker.from_env()
        self.fieldnames = [
            "container_name",
            "timestamp",
            "event_type",
            "message_id",
            "logical_size",
            "topic",
            "serialized_size",
        ]
        self.separator = separator
        self.logs = []
        self.log_level = log_level

    def collect_logs(self) -> None:
        """Collect logs from all containers related to the technology."""
        self.logs = []  # ensure idempotency
        containers = self.client.containers.list(
            all=True, filters={"name": f"{self.tech_name}-*"}
        )
        logger.debug(
            f"Collecting logs from {len(containers)} containers for technology {self.tech_name} and scenario {self.scenario_name}..."
        )
        for container in containers:
            try:
                logs = container.logs().decode("utf-8").strip().split("\n")
                for log in logs:
                    # else continue
                    parsed = self._parse_log(log, container.name)
                    if parsed:
                        self.logs.append(parsed)
            except Exception as e:
                logger.error(
                    f"Error collecting logs from container {container.id}: {e}"
                )

    def write_logs(self) -> None:
        """Write collected logs to a Parquet file."""
        if not self.logs:
            logger.warning(
                f"No logs to save for technology {self.tech_name} and scenario {self.scenario_name}."
            )
            return
        df = pl.DataFrame(self.logs)
        df.write_parquet(self.log_file)
        # with open(self.log_file, mode='w', encoding='utf-8') as file:
        #     file.write(self.separator.join(self.fieldnames) + "\n")
        #     file.writelines(self.logs)
        logger.info(f"Logs saved to {self.log_file}")

    def _parse_log(self, log_line: str, container_name: str) -> Optional[Dict]:
        """
        Parse a single log line and extract relevant fields.

        Args:
            log_line (str): The log line to parse.
            container_name (str): The name of the container from which the log was collected.

        Returns:
            Optional[Dict]: A dictionary with parsed fields or None if parsing fails.
        """

        if self.log_level not in log_line:
            return None
        try:
            _, log = log_line.split(f"[{self.log_level}]", 1)
            log_parts = log.strip().split(",")
            timestamp_part = log_parts[0] if len(log_parts) > 0 else None
            event_type_part = log_parts[1] if len(log_parts) > 1 else None
            message_id_part = log_parts[2] if len(log_parts) > 2 else None
            logical_size_part = log_parts[3] if len(log_parts) > 3 else None
            topic_part = log_parts[4] if len(log_parts) > 4 else None
            serialized_size_part = log_parts[5] if len(log_parts) > 5 else None
            return {
                "container_name": container_name,
                "timestamp": datetime.datetime.strptime(
                    timestamp_part, "%Y-%m-%d %H:%M:%S.%f"
                ),
                "event_type": event_type_part,
                "message_id": message_id_part,
                "logical_size": (
                    int(logical_size_part) if serialized_size_part is not None else None
                ),
                "topic": topic_part,
                "serialized_size": (
                    int(serialized_size_part)
                    if serialized_size_part is not None
                    else None
                ),
            }

        except Exception as e:
            logger.error(f"Failed to parse log line: {log_line} — {e}")
            return None
