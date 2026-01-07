import time
from functools import wraps
from typing import Callable, List, Optional, Tuple

import docker
from docker.errors import APIError, DockerException, NotFound
from docker.models.containers import Container

from .utils.logger import logger


class ContainerManager:

    def __init__(self, network_name="benchmark_network") -> None:
        self.client = docker.from_env()
        self.containers: List[Container] = []
        try:
            self.network = self.client.networks.get(network_name)
            self.network_name = network_name
        except NotFound:
            self.network = self.client.networks.create(network_name, driver="bridge")
            self.network_name = network_name
        self.topics_map = {}

    def reset_between_experiments(self) -> None:
        """Reset the container manager state between experiments."""
        self.topics_map = {}
        self.containers = []

    def topics_and_publishers_lists(
        self, topic_filter: List[str]
    ) -> Tuple[List[str], List[str]]:
        """
        Generate lists of topics and their corresponding publishers based on a filter.

        Args:
            topic_filter (List[str]): List of topics to filter.

        Returns:
            Tuple[List[str], List[str]]: Two lists - one of topics and another of corresponding publishers.
        """
        topics_list = []
        publishers_list = []
        for topic in self.topics_map:
            if topic not in topic_filter:
                continue
            for publisher in self.topics_map[topic]:
                topics_list.append(topic)
                publishers_list.append(publisher)
        return topics_list, publishers_list

    def _pause_safely(self, container: Container, *, timeout_s: int = 10) -> None:
        """
        Safely pause a container, handling potential exceptions.

        Args:
            container (Container): The Docker container instance to pause.
            timeout (int, optional): Timeout in seconds for the pause operation. Defaults to 10.
        """
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            container.reload()
            status = container.status or None

            if status == "running":
                try:
                    container.pause()
                    return
                except APIError:
                    # Can still race with runtime; retry briefly.
                    time.sleep(0.1)
                    continue

            if status in ("exited", "dead"):
                try:
                    tail = container.logs(tail=200).decode("utf-8", errors="replace")
                except Exception:
                    tail = "<failed to read container logs>"
                raise ValueError(
                    f"Container '{container.name}' exited before pause(). status={status}\n"
                    f"--- last 200 log lines ---\n{tail}"
                )

            # created/restarting/paused/unknown -> wait a bit
            time.sleep(0.1)

        # Timed out: do one last reload + attempt
        container.reload()
        if container.status != "running":
            raise ValueError(
                f"Timed out waiting to pause container '{container.name}'. status={container.status}"
            )
        try:
            container.pause()
        except Exception as e:
            raise ValueError(
                f"Failed to pause container '{container.name}': {e}"
            ) from e

    @staticmethod
    def return_container_ids(method) -> Callable:
        """
        Decorator to append container IDs to the return value of the method.

        Args:
            method (_type_): _the method to be decorated_

        Returns:
            _type_: _modified return value with container IDs appended_
        """

        @wraps(method)
        def wrapper(self, *args, **kwargs):
            # Execute the original method
            result = method(self, *args, **kwargs)

            # Add container IDs to the return value (or create one if result is None)
            container_ids = [container.name for container in self.containers]
            if result is None:
                return container_ids
            elif isinstance(result, tuple):
                return (*result, container_ids)
            else:
                return result, container_ids

        return wrapper

    @staticmethod
    def validate_container(method) -> Callable:
        """
        Decorator to validate if a container ID exists before executing the method.

        Args:
            method (_type_): _the method to be decorated_

        Raises:
            ValueError: If the container ID is not found.

        Returns:
            _type_: _the result of the original method if validation passes_
        """

        @wraps(method)
        def wrapper(self, container_id, *args, **kwargs):
            container = next((c for c in self.containers if c.id == container_id), None)
            if container is None:
                raise ValueError(f"Container ID '{container_id}' not found")
            return method(self, container, *args, **kwargs)

        return wrapper

    # @return_container_ids
    def start_publisher(
        self,
        tech_name: str,
        pub_id: int,
        topics: List[str],
        pub_rate: float,
        message_size: int,
        n_messages: Optional[int] = None,
        duration: Optional[int] = None,
        paused: bool = True,
        mode: Optional[str] = None,
    ) -> str:
        """
        Starts a publisher container with the specified configuration.

        Args:
            tech_name (str): _the technology name (e.g., kafka, zeromq_p2p)_
            pub_id (int): _publisher ID_
            topics (List[str]): _list of topics to publish to_
            pub_rate (float): _update rate for the publisher_
            message_size (int): _size of each message_
            n_messages (Optional[int], optional): _number of messages to publish_. Defaults to None.
            duration (Optional[int], optional): _duration to run the publisher_. Defaults to None.
            paused (bool, optional): _enable paused start_. Defaults to True.
            mode (Optional[str], optional): _mode to run the publisher in_. Defaults to None.

        Raises:
            ValueError: If both or neither of 'n_messages' and 'duration' are provided.
            ValueError: If starting the publisher container fails.

        Returns:
            str: The name of the started container.
        """
        if (n_messages is None and duration is None) or (
            n_messages is not None and duration is not None
        ):
            raise ValueError(
                "One and only one of 'n_messages' and 'duration' must be passed."
            )
        logger.debug(
            f"Starting publisher {pub_id} on topics {topics} using {tech_name}"
        )
        try:
            container_name = f"{tech_name}-{pub_id}"
            publisher_endpoint = (
                "0.0.0.0"
                if "p2p" in container_name
                else "benchmark_" + tech_name + "_broker"
            )
            environment = {
                "TECHNOLOGY": tech_name,
                "CONTAINER_ID": pub_id,
                "PUBLISHER_ENDPOINT": publisher_endpoint,
                "TOPICS": ",".join(topics),
                "MESSAGES": n_messages,
                "DURATION": duration,
                "UPDATE_EVERY": pub_rate,
                "PAYLOAD_SIZE": message_size,
                "PAYLOAD_SAMPLES": 5,  # can be hardcoded for now?
                "PAYLOAD_KIND": "FLAT",  # TODO read payload kind from config
            }
            logger.debug(f"Environment: {environment}")
            logger.debug(
                f"Starting container from image {tech_name}_publisher in mode {mode}"
            )
            container: Container = self.client.containers.run(
                name=container_name,
                image=f"{tech_name}_publisher",
                environment=environment,
                network=self.network_name,
                detach=True,
                command=[mode],
            )
            if paused:
                self._pause_safely(container)
            self.containers.append(container)
            logger.debug(f"Created container {container.name}")
        except DockerException as e:
            raise ValueError(f"Failed to start publisher {pub_id}") from e
        for topic in topics:
            if topic not in self.topics_map:
                self.topics_map[topic] = []
            self.topics_map[topic].append(container.name)
        return container.name

    # @return_container_ids
    def start_consumer(
        self,
        tech_name: str,
        con_id: int,
        topics: List[str],
        backlog_size: Optional[int] = None,
        paused: bool = True,
        mode: Optional[str] = None,
    ) -> str:
        """
        Starts a consumer container with the specified configuration.

        Args:
            tech_name (str): _the technology name (e.g., kafka, zeromq_p2p)_
            con_id (int): _consumer ID_
            topics (List[str]): _list of topics to subscribe to_
            backlog_size (Optional[int], optional): _size of the backlog_. Defaults to None.
            paused (bool, optional): _enable paused start_. Defaults to True.
            mode (Optional[str], optional): _mode to run the consumer in_. Defaults to None.

        Raises:
            ValueError: If starting the consumer container fails.

        Returns:
            str: The name of the started container.
        """
        logger.debug(
            f"Starting consumer {con_id} subscribed to topics {topics} with backlog_size {backlog_size} using {tech_name}"
        )
        try:
            topics_list, publishers_list = self.topics_and_publishers_lists(topics)
            environment = {
                "TECHNOLOGY": tech_name,
                "CONTAINER_ID": con_id,
                "TOPICS": ",".join(topics_list),
                "BACKLOG_SIZE": backlog_size,
            }
            if "p2p" in tech_name:
                logger.debug(f"Using p2p broker {publishers_list}")
                environment["CONSUMER_ENDPOINT"] = ",".join(publishers_list)
            else:
                logger.debug(f"Using tech-specific broker benchmark_{tech_name}_broker")
                environment["CONSUMER_ENDPOINT"] = "benchmark_" + tech_name + "_broker"

            container = self.client.containers.run(
                name=f"{tech_name}-{con_id}",
                image=f"{tech_name}_consumer",
                environment=environment,
                network=self.network_name,
                detach=True,
                command=[mode],
            )
            if paused:
                self._pause_safely(container)
            self.containers.append(container)
            logger.debug(f"Created container {container.name}")
        except DockerException as e:
            raise ValueError(f"Failed to start consumer {con_id}") from e
        return container.name

    def wake_all(self) -> None:
        """Wakes all paused containers."""
        logger.debug("Waking all containers...")
        for container in self.containers:
            container.unpause()

    @validate_container
    def wake_container(self, container_id: str) -> None:
        logger.debug(f"Waking container {container_id}...")
        container = self.client.containers.get(container_id)
        container.unpause()

    def stop_all(self) -> None:
        """Stops all running containers."""
        logger.debug("Stopping all containers...")
        for container in self.containers:
            container.stop()

    @validate_container
    def stop_container(self, container_id: str) -> None:
        logger.debug(f"Stopping container {container_id}...")
        container = self.client.containers.get(container_id)
        container.stop()

    @return_container_ids
    def remove_all(self) -> None:
        """Removes all containers."""
        logger.debug("Removing all containers...")
        for container in self.containers:
            container.remove()
        self.containers = []

    @validate_container
    @return_container_ids
    def remove_container(self, container_id: str) -> None:
        logger.debug(f"Removing container {container_id}...")
        container = self.client.containers.get(container_id)
        container.remove()
        self.containers = [c for c in self.containers if c.id != container_id]

    def wait_for_all(self) -> None:
        """Waits for all containers to finish."""
        logger.debug("Waiting for all containers to finish...")
        for container in self.containers:
            container.wait()

    @validate_container
    def wait_for_container(self, container_id: str) -> None:
        logger.debug(f"Waiting for container {container_id} to finish...")
        container = self.client.containers.get(container_id)
        container.wait()

    def is_healthy(self, container_id: str) -> bool:
        """Checks if a container is healthy."""
        status = self.client.containers.get(container_id).attrs["Health"]
        return status == "healthy"
