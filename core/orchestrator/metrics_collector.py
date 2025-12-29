import concurrent.futures
import csv
import os
import threading
import time
from datetime import datetime
from typing import Any, Dict, List

import docker
from docker.models.containers import Container

from .utils.logger import logger


class MetricsCollector:
    def __init__(
        self,
        tech_name: str,
        scenario_name: str,
        scenario_config: str,
        date_time: str,
        interval: float = 2.0,
    ) -> None:
        self.tech_name = tech_name
        self.logs_dir = os.path.join("logs", scenario_config, tech_name, date_time)
        self.log_file = os.path.join(self.logs_dir, f"{scenario_name}")
        self.scenario_name = scenario_name
        self.interval = interval if interval > 2.0 else 2.0
        self.running = False
        self.thread = None
        self.client = docker.from_env()
        self.previous_cpu = {}
        self.previous_system_cpu = {}
        self.metrics = []
        self.fieldnames = [
            "timestamp",
            "cpu_usage_ns",
            "cpu_usage_perc",
            "memory_usage",
            "network_rx",
            "network_tx",
            "disk_read",
            "disk_write",
        ]

    def start(self) -> None:
        """Start the background thread for collecting metrics."""
        if not self.running:
            if not os.path.exists(self.logs_dir):
                os.makedirs(self.logs_dir)
            self.running = True
            self.thread = threading.Thread(target=self._collect_metrics)
            self.thread.start()

    def _calculate_cpu_percent(
        self,
        container_id: str,
        cpu_usage: float,
        system_cpu_usage: float,
        num_cpus: int,
    ) -> float:
        """
        Calculate CPU usage as a percentage of total system capacity.

        Args:
            container_id (str): The ID of the Docker container.
            cpu_usage (float): The current CPU usage of the container.
            system_cpu_usage (float): The current total system CPU usage.
            num_cpus (int): The number of CPUs available on the host system.

        Returns:
            float: The CPU usage percentage.
        """
        if (
            container_id in self.previous_cpu
            and container_id in self.previous_system_cpu
        ):
            # Delta calculation
            delta_cpu = cpu_usage - self.previous_cpu[container_id]
            delta_system_cpu = system_cpu_usage - self.previous_system_cpu[container_id]

            if delta_system_cpu > 0 and num_cpus > 0:
                cpu_percent = (delta_cpu / delta_system_cpu) * num_cpus * 100
                return round(cpu_percent, 2)

        return 0.0

    def _collect_metrics(self) -> None:
        """Collect metrics from all containers related to the technology in parallel.This method runs in a separate thread and collects metrics at regular intervals."""
        logger.info(f"Starting metrics collection for technology '{self.tech_name}'...")
        containers: List[Container] = self.client.containers.list(
            filters={"name": f"{self.tech_name}-*"}
        )
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            fc = {}
            for container in containers:
                futures.append(
                    executor.submit(self.collect_metrics_container, container)
                )
                fc[futures[-1]] = container.name
                logger.info(
                    f"Metrics collection started for container: {container.name}"
                )
            for future in concurrent.futures.as_completed(futures):
                try:
                    logger.info(
                        f"Metrics collection completed for container: {fc[future]}, {future.result()}"
                    )
                except Exception as e:
                    logger.error(
                        f"Error in metrics collection for container {fc[future]}: {e}"
                    )
            logger.info(
                f"Metrics collection finished for technology '{self.tech_name}'."
            )

    def collect_metrics_container(self, container: Container) -> str:
        """
        Collect metrics for a single container and save them to a CSV file.

        Args:
            container (Container): The Docker container instance.

        Returns:
            str: A summary string indicating the number of logs collected.
        """
        client_info: Dict[str, Any] = self.client.info()
        num_cpus: int = client_info.get("NCPU", 1)
        # Open the file once and keep appending to avoid file locks
        container_file = self.log_file + f"_{container.name}.csv"
        nlogs = 0
        with open(container_file, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=self.fieldnames)
            writer.writeheader()
            while self.running:
                try:
                    timestamp = datetime.now().isoformat()
                    stats: Dict[str, Any] = container.stats(stream=False)

                    if (
                        not stats
                        or not stats.get("cpu_stats")
                        or not stats.get("memory_stats")
                    ):
                        logger.warning(
                            f"No stats available for container {container.name}. Skipping this interval."
                        )
                        time.sleep(self.interval)
                        continue

                    # Collect metrics
                    cpu_stats = stats.get("cpu_stats") or {}
                    cpu_usage_ns: float = cpu_stats.get("cpu_usage", {}).get(
                        "total_usage", 0
                    )
                    system_cpu_usage: float = cpu_stats.get("system_cpu_usage", 0)

                    memory_stats = stats.get("memory_stats") or {}
                    memory_usage: float = memory_stats.get("usage", 0)

                    networks: dict = stats.get("networks") or {}
                    network_rx: float = sum(
                        v.get("rx_bytes", 0) for v in networks.values()
                    )
                    network_tx: float = sum(
                        v.get("tx_bytes", 0) for v in networks.values()
                    )

                    blkio_stats: dict = stats.get("blkio_stats") or {}
                    io_service_bytes: list = (
                        blkio_stats.get("io_service_bytes_recursive") or []
                    )
                    disk_read: float = sum(
                        x.get("value", 0)
                        for x in io_service_bytes
                        if x.get("op") == "Read"
                    )
                    disk_write: float = sum(
                        x.get("value", 0)
                        for x in io_service_bytes
                        if x.get("op") == "Write"
                    )

                    # Calculate CPU percentage
                    cpu_usage_perc = self._calculate_cpu_percent(
                        container.id, cpu_usage_ns, system_cpu_usage, num_cpus
                    )
                    self.previous_cpu[container.id] = cpu_usage_ns
                    self.previous_system_cpu[container.id] = system_cpu_usage

                    # Write row to CSV
                    writer.writerow(
                        {
                            "timestamp": timestamp,
                            "cpu_usage_ns": cpu_usage_ns,
                            "cpu_usage_perc": cpu_usage_perc,
                            "memory_usage": memory_usage,
                            "network_rx": network_rx,
                            "network_tx": network_tx,
                            "disk_read": disk_read,
                            "disk_write": disk_write,
                        }
                    )
                except TypeError as e:
                    logger.warning(
                        f"TypeError while collecting metrics for container {container.name}: {e}"
                    )
                    continue
                except Exception as e:
                    logger.warning(
                        f"Error while collecting metrics for container {container.name}: {e}"
                    )
                    continue

                logger.debug(
                    f"Metrics for {container.name}: CPU {cpu_usage_perc}%, Memory {memory_usage} bytes, Network RX {network_rx} bytes, Network TX {network_tx} bytes, Disk Read {disk_read} bytes, Disk Write {disk_write} bytes"
                )
                time.sleep(self.interval)
                nlogs += 1
        return f"{nlogs} logs collected"

    def stop(self) -> None:
        """Stop the background thread and save metrics to file."""
        if self.running:
            logger.info(
                f"Stopping metrics collection for technology '{self.tech_name}'..."
            )
            self.running = False
        self.thread.join()
        logger.info(
            f"Metrics collection thread for technology '{self.tech_name}' has stopped."
        )
