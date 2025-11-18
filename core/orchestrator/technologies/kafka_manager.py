import docker
import uuid
import time
from confluent_kafka.admin import AdminClient, NewTopic
import concurrent.futures

from ..technology_manager import TechnologyManager, register_technology

KAFKA_IMAGE = "bitnami/kafka:latest"
KAFKA_CONTAINER_NAME = "benchmark_kafka_broker"
KAFKA_PORT = 9092
CONTROLLER_PORT = 9093

@register_technology("kafka")
class KafkaManager(TechnologyManager):
    
    def __init__(self, tech_path, network_name = "benchmark_network", broker_host = KAFKA_CONTAINER_NAME, broker_port = KAFKA_PORT, controller_port = CONTROLLER_PORT):
        TechnologyManager.__init__(self, tech_path, network_name)
        self.client = docker.from_env()
        self.container = None
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.controller_port = controller_port
        
    def setup_tech(self):
        # existing = None
        # try:
        #     existing = self.client.containers.get(self.broker_host)
        # except docker.errors.NotFound:
        #     pass  # Nothing to stop
        # if existing is not None:
        #     self.container = existing
        #     self.reset_broker_state()
        # else:
        self.start_broker()
    
    def reset_tech(self):
        self.reset_broker_state()
    
    def teardown_tech(self):
        self.stop_broker()

    def start_broker(self):
        print("[KM] Starting new Kafka broker...")
        cluster_id = uuid.uuid4().hex
        env_vars = {
            "KAFKA_CFG_PROCESS_ROLES": "broker,controller",
            "KAFKA_CFG_NODE_ID": "1",
            "KAFKA_CFG_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
            "KAFKA_CFG_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
            "KAFKA_CFG_ADVERTISED_LISTENERS": f"PLAINTEXT://{self.broker_host}:9092",
            "KAFKA_CFG_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_KRAFT_CLUSTER_ID": cluster_id,
            "ALLOW_PLAINTEXT_LISTENER": "yes"
        }

        print("[KM] Starting Kafka broker container...")

        self.container = self.client.containers.run(
            image=KAFKA_IMAGE,
            name=KAFKA_CONTAINER_NAME,
            environment=env_vars,
            ports={
                f"{self.broker_port}/tcp": self.broker_port,
                f"{self.controller_port}/tcp": self.controller_port
            },
            detach=True,
            network=self.network_name,  # TODO or use a shared network if your benchmark containers need it
            remove=True  # auto-remove container on stop
        )

        self._wait_for_readiness()

        print(f"[KM] Kafka broker is up and running at localhost:{self.broker_port}")
        return f"localhost:{self.broker_port}"

    def stop_broker(self):
        try:
            existing = self.client.containers.get(self.broker_host)
            print("[KM] Stopping existing Kafka broker container...")
            # existing.stop()
            existing.remove(force=True)
        except docker.errors.NotFound:
            pass  # Nothing to stop

    def _wait_for_readiness(self, timeout=30):
        print("[KM] Waiting for Kafka broker to become ready...")
        start = time.time()
        while time.time() - start < timeout:
            logs = self.container.logs().decode("utf-8")
            if "Kafka startTimeMs" in logs or "started (kafka.server.KafkaServer)" in logs:
                return
            time.sleep(1)
        raise TimeoutError("Kafka broker did not become ready in time.")

    def reset_broker_state(self):
        """Delete all non-internal topics from the broker."""
        print("[KM] Resetting Kafka broker state...")

        admin_conf = {'bootstrap.servers': "localhost:9092"}
        admin_client = AdminClient(admin_conf)

        # Fetch list of topics
        metadata = admin_client.list_topics(timeout=10)
        topics_to_delete = [
            t for t in metadata.topics.keys()
            if not t.startswith("__")  # Exclude internal topics
        ]

        if not topics_to_delete:
            print("[KM] No topics to delete.")
            return

        print(f"[KM] Deleting topics: {topics_to_delete}")
        delete_futures = admin_client.delete_topics(topics_to_delete, operation_timeout=30)

        # Wait for each deletion to complete
        for topic, future in delete_futures.items():
            try:
                future.result()
                print(f"[KM] Deleted topic: {topic}")
            except Exception as e:
                print(f"[KM] Failed to delete topic {topic}: {e}")
