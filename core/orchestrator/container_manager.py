import docker
from functools import wraps
from .utils.logger import logger

class ContainerManager:
    
    def __init__(self, network_name="benchmark_network"):
        self.client = docker.from_env()
        self.containers = []
        try:
            self.network = self.client.networks.get(network_name)
            self.network_name = network_name
        except docker.errors.NotFound:
            self.network = self.client.networks.create(network_name, driver="bridge")
            self.network_name = network_name
        self.topics_map = {}
        
    def reset_between_experiments(self) -> None:
        self.topics_map = {}
        self.containers = []
        
    def topics_and_publishers_lists(self, topic_filter):
        topics_list = []
        publishers_list = []
        for topic in self.topics_map:
            if not topic in topic_filter:
                continue
            for publisher in self.topics_map[topic]:
                topics_list.append(topic)
                publishers_list.append(publisher)
        return topics_list, publishers_list
        
    @staticmethod
    def return_container_ids(method):
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
    def validate_container(method):
        @wraps(method)
        def wrapper(self, container_id, *args, **kwargs):
            container = next((c for c in self.containers if c.id == container_id), None)
            if container is None:
                raise ValueError(f"[CM] Container ID '{container_id}' not found")
            return method(self, container, *args, **kwargs)
        return wrapper
        
    # @return_container_ids
    def start_publisher(self, tech_name, pub_id, topics, pub_rate, message_size, n_messages=None, duration=None, paused = True, mode = None):
        if (n_messages is None and duration is None) or (n_messages is not None and duration is not None):
            raise ValueError("One and only one of 'n_messages' and 'duration' must be passed.")
        logger.debug(f"[CM] Starting publisher {pub_id} on topics {topics} using {tech_name}")
        try:
            container_name = f"{tech_name}-{pub_id}"
            publisher_endpoint = "0.0.0.0" if "p2p" in container_name else "benchmark_" + tech_name + "_broker"
            environment={
                "TECHNOLOGY": tech_name,
                "CONTAINER_ID": pub_id,
                "PUBLISHER_ENDPOINT": publisher_endpoint,
                "TOPICS": ','.join(topics),
                "MESSAGES": n_messages,
                "DURATION": duration,
                "UPDATE_EVERY": pub_rate,
                "PAYLOAD_SIZE": message_size,
                "PAYLOAD_SAMPLES": 5, # can be hardcoded for now?
                "PAYLOAD_KIND": "FLAT", # TODO read payload kind from config
            }
            logger.debug(f"[CM] Environment: {environment}")
            logger.debug(f"[CM] Starting container from image {tech_name}_publisher in mode {mode}")
            container = self.client.containers.run(
                name=container_name,
                image=f"{tech_name}_publisher",
                environment=environment,
                network=self.network_name,
                detach=True,
                command=[mode]
            )
            if paused:
                container.pause()
            self.containers.append(container)
            logger.debug(f"[CM] Created container {container.name}")
        except docker.errors.DockerException as e:
            raise ValueError(f"[CM] Failed to start publisher {pub_id}") from e
        for topic in topics:
            if not topic in self.topics_map:
                self.topics_map[topic] = []
            self.topics_map[topic].append(container.name)
        return container.name
    
    # @return_container_ids
    def start_consumer(self, tech_name, con_id, topics, backlog_size = None, paused = True, mode = None):
        logger.debug(f"[CM] Starting consumer {con_id} subscribed to topics {topics} with backlog_size {backlog_size} using {tech_name}")
        try:
            topics_list, publishers_list = self.topics_and_publishers_lists(topics)
            environment = {
                "TECHNOLOGY": tech_name,
                "CONTAINER_ID": con_id,
                "TOPICS": ','.join(topics_list),
                "BACKLOG_SIZE": backlog_size
            }
            if "p2p" in tech_name:
                logger.debug(f"[CM] Using p2p broker {publishers_list}")
                environment["CONSUMER_ENDPOINT"] = ','.join(publishers_list)
            else:
                logger.debug(f"[CM] Using tech-specific broker benchmark_" + tech_name + "_broker")
                environment["CONSUMER_ENDPOINT"] = "benchmark_" + tech_name + "_broker"
                
            container = self.client.containers.run(
                name=f"{tech_name}-{con_id}",
                image=f"{tech_name}_consumer",
                environment=environment,
                network=self.network_name,
                detach=True,
                command=[mode]
            )
            if paused:
                container.pause()
            self.containers.append(container)
            logger.debug(f"[CM] Created container {container.name}")
        except docker.errors.DockerException as e:
            raise ValueError(f"[CM] Failed to start consumer {con_id}") from e
        return container.name

    def wake_all(self):
        logger.debug("[CM] Waking all containers...")
        for container in self.containers:
            container.unpause()
            
    @validate_container
    def wake_container(self, container_id):
        logger.debug(f"[CM] Waking container {container_id}...")
        container = self.client.containers.get(container_id)
        container.unpause()

    def stop_all(self):
        logger.debug("[CM] Stopping all containers...")
        for container in self.containers:
            container.stop()
            
    @validate_container
    def stop_container(self, container_id):
        logger.debug(f"[CM] Stopping container {container_id}...")
        container = self.client.containers.get(container_id)
        container.stop()
        
    @return_container_ids
    def remove_all(self):
        logger.debug("[CM] Removing all containers...")
        for container in self.containers:
            container.remove()
        self.containers = []
        
    @validate_container
    @return_container_ids
    def remove_container(self, container_id):
        logger.debug(f"[CM] Removing container {container_id}...")
        container = self.client.containers.get(container_id)
        container.remove()
        self.containers = [c for c in self.containers if c.id != container_id]
        
    def wait_for_all(self):
        logger.debug("[CM] Waiting for all containers to finish...")
        for container in self.containers:
            container.wait()
    
    @validate_container
    def wait_for_container(self, container_id):
        logger.debug(f"[CM] Waiting for container {container_id} to finish...")
        container = self.client.containers.get(container_id)
        container.wait()
    
    def is_healthy(self, container_id):
        status = self.client.containers.get(container_id).attrs['Health']
        return status == 'healthy'
