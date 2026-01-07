import os
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Iterable, Optional

import docker
import polars as pl
from docker.models.containers import Container

from .utils.logger import logger


class ContainerEventsLogger:
    def __init__(
        self,
        tech_name: str,
        scenario_name: str,
        scenario_config: str,
        date_time: str,
        separator: str = ";",
        log_level: str = "STUDY",
    ) -> None:
        self.tech_name = tech_name
        self.scenario_name = scenario_name
        self.log_file = os.path.join(
            "logs",
            scenario_config,
            tech_name,
            date_time,
            f"{scenario_name}_events.parquet",
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
        self._events_columns: Optional[dict[str, list]] = None

    def collect_logs(self) -> None:
        """Collect logs from all containers related to the technology."""
        self.logs = []  # ensure idempotency
        self._events_columns = None
        containers = self.client.containers.list(
            all=True, filters={"name": f"{self.tech_name}-*"}
        )
        logger.debug(
            f"Collecting logs from {len(containers)} containers for technology {self.tech_name} and scenario {self.scenario_name}..."
        )

        if not containers:
            return

        max_workers = min(8, len(containers))
        started = time.perf_counter()
        columns: dict[str, list] = {name: [] for name in self.fieldnames}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._collect_one_container, container): container
                for container in containers
            }

            completed = 0
            pending = set(futures.keys())
            last_heartbeat = started

            while pending:
                done, pending = wait(pending, timeout=5.0, return_when=FIRST_COMPLETED)

                now = time.perf_counter()
                if not done and now - last_heartbeat >= 10.0:
                    elapsed_s = now - started
                    logger.info(
                        "Collecting logs... completed={}/{}, pending={}, total_records={}, elapsed={:.1f}s",
                        completed,
                        len(containers),
                        len(pending),
                        len(columns["timestamp"]),
                        elapsed_s,
                    )
                    last_heartbeat = now

                for future in done:
                    container = futures[future]
                    try:
                        container_columns = future.result()
                        for name in self.fieldnames:
                            columns[name].extend(container_columns[name])
                        completed += 1
                        elapsed_s = time.perf_counter() - started
                        logger.debug(
                            "Collected {}/{} containers (last={}, +{} records). Total records={}. Elapsed={:.1f}s",
                            completed,
                            len(containers),
                            container.name,
                            len(container_columns["timestamp"]),
                            len(columns["timestamp"]),
                            elapsed_s,
                        )
                    except Exception as e:
                        logger.error(
                            f"Error collecting logs from container {container.id}: {e}"
                        )

        self._events_columns = columns
        logger.info(
            "Finished collecting logs from {} containers. Parsed records={}. Total time={:.1f}s",
            len(containers),
            len(columns["timestamp"]),
            time.perf_counter() - started,
        )

    def _iter_container_logs(self, container: Container) -> Iterable[str]:
        """Stream container logs line-by-line to avoid large decode+split overhead."""
        stream = container.logs(
            stdout=True,
            stderr=True,
            stream=True,
            follow=False,
        )
        buffer = ""
        for chunk in stream:
            if chunk is None:
                continue
            if isinstance(chunk, (bytes, bytearray)):
                text = bytes(chunk).decode("utf-8", errors="replace")
            else:
                text = str(chunk)

            # Docker can yield arbitrary chunks; a single log line may span chunks.
            buffer += text
            while True:
                newline_index = buffer.find("\n")
                if newline_index == -1:
                    break
                line = buffer[:newline_index]
                buffer = buffer[newline_index + 1 :]
                yield line.rstrip("\r")

        if buffer:
            yield buffer.rstrip("\r")

    def _collect_one_container(self, container: Container) -> dict[str, list]:
        """Collect logs from a single container."""
        out: dict[str, list] = {name: [] for name in self.fieldnames}
        started = time.perf_counter()
        last_progress = started
        total_lines = 0
        matched_lines = 0

        marker = f"[{self.log_level}]"
        marker_len = len(marker)

        logger.debug(
            "Start collecting logs from container {} ({})", container.name, container.id
        )

        for log_line in self._iter_container_logs(container):
            if not log_line or log_line.isspace():
                continue

            total_lines += 1
            line = log_line

            # Fast-path: for huge logs, skip parsing non-study lines entirely.
            marker_index = line.find(marker)
            if marker_index == -1:
                continue

            matched_lines += 1
            parsed = self._parse_log(
                line, marker_index=marker_index, marker_len=marker_len
            )
            if parsed is not None:
                (
                    timestamp_part,
                    event_type_part,
                    message_id_part,
                    logical_size,
                    topic_part,
                    serialized_size,
                ) = parsed
                out["container_name"].append(container.name)
                out["timestamp"].append(timestamp_part)
                out["event_type"].append(event_type_part)
                out["message_id"].append(message_id_part)
                out["logical_size"].append(logical_size)
                out["topic"].append(topic_part)
                out["serialized_size"].append(serialized_size)

            now = time.perf_counter()
            if now - last_progress >= 5.0:
                elapsed_s = now - started
                logger.debug(
                    "Progress container {}: lines={}, matched={}, parsed={}, elapsed={:.1f}s",
                    container.name,
                    total_lines,
                    matched_lines,
                    len(out["timestamp"]),
                    elapsed_s,
                )
                last_progress = now

        logger.debug(
            "Done collecting container {}: lines={}, matched={}, parsed={}, elapsed={:.1f}s",
            container.name,
            total_lines,
            matched_lines,
            len(out["timestamp"]),
            time.perf_counter() - started,
        )
        return out

    def write_logs(self) -> None:
        """Write collected logs to a Parquet file."""
        if self._events_columns is None and not self.logs:
            logger.warning(
                f"No logs to save for technology {self.tech_name} and scenario {self.scenario_name}."
            )
            return
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)

        if self._events_columns is not None:
            df = pl.DataFrame(self._events_columns)
        else:
            df = pl.DataFrame(self.logs)
        df.write_parquet(self.log_file)
        # with open(self.log_file, mode='w', encoding='utf-8') as file:
        #     file.write(self.separator.join(self.fieldnames) + "\n")
        #     file.writelines(self.logs)
        logger.info(f"Logs saved to {self.log_file}")

    def _parse_log(
        self, log_line: str, *, marker_index: int, marker_len: int
    ) -> Optional[
        tuple[
            str,
            Optional[str],
            Optional[str],
            Optional[int],
            Optional[str],
            Optional[int],
        ]
    ]:
        """
        Parse a single log line and extract relevant fields.

        Args:
            log_line (str): The log line to parse.
            marker_index (int): The index where the log level marker starts.
            marker_len (int): The length of the log level marker.

        Returns:
            Optional[tuple]: A tuple containing the parsed fields, or None if parsing failed.
        """

        try:
            # Slice after the marker; avoids partition() rescanning the string.
            rest = log_line[marker_index + marker_len :]
            # Most logs have a single space after marker.
            if rest and rest[0] == " ":
                rest = rest[1:]
            else:
                rest = rest.lstrip()

            # Expected formats:
            #   [STUDY] <ts>,<event_type>,<message_id>,<logical_size>,<topic>,<serialized_size>
            # or (no topic):
            #   [STUDY] <ts>,<event_type>,<message_id>,<logical_size>,<serialized_size>
            # Split at most 5 times to limit allocations.
            parts = rest.split(",", 5)
            if len(parts) < 4:
                return None

            timestamp_part = parts[0].strip() if len(parts) > 0 else None
            event_type_part = parts[1].strip() if len(parts) > 1 else None
            message_id_part = parts[2].strip() if len(parts) > 2 else None
            logical_size_part = parts[3].strip() if len(parts) > 3 else None
            topic_part: Optional[str] = None
            serialized_size_part: Optional[str] = None
            if len(parts) >= 6:
                topic_part = parts[4].strip() if parts[4] else None
                serialized_size_part = parts[5].strip() if parts[5] else None
            elif len(parts) == 5:
                serialized_size_part = parts[4].strip() if parts[4] else None

            if not timestamp_part:
                return None

            logical_size: Optional[int]
            if not logical_size_part:
                logical_size = None
            else:
                if "." in logical_size_part:
                    logical_size = int(float(logical_size_part))
                else:
                    logical_size = int(logical_size_part)

            serialized_size: Optional[int]
            if not serialized_size_part:
                serialized_size = None
            else:
                # Fast path: most sizes are integers. Some logs may emit decimals.
                if "." in serialized_size_part:
                    serialized_size = int(float(serialized_size_part))
                else:
                    serialized_size = int(serialized_size_part)

            return (
                timestamp_part,
                event_type_part,
                message_id_part,
                logical_size,
                topic_part,
                serialized_size,
            )

        except Exception as e:
            # Avoid flooding logs in huge runs.
            logger.error("Failed to parse log line: {} — {}", log_line, e)
            return None
