from ..technology_manager import register_technology
from .kafka_manager import KafkaManager

KAFKA_IMAGE = "bitnamilegacy/kafka:latest"
KAFKA_CONTAINER_NAME = "benchmark_kafka_cpp_broker"
KAFKA_PORT = 9092
CONTROLLER_PORT = 9093


@register_technology("kafka_cpp")
class KafkaCppManager(KafkaManager):
    def __init__(
        self,
        tech_path,
        network_name="benchmark_network",
        broker_host=KAFKA_CONTAINER_NAME,
        broker_port=KAFKA_PORT,
        controller_port=CONTROLLER_PORT,
        kafka_image=KAFKA_IMAGE,
    ):
        super().__init__(
            tech_path,
            network_name,
            broker_host,
            broker_port,
            controller_port,
            kafka_image,
        )
