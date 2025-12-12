from typing import Any, Dict, List, Optional

from .scenario_config_manager import ScenarioConfigManager
from .utils.logger import logger


class ScenarioManager:

    def __init__(self, scenario_config=None) -> None:
        self.scenario = scenario_config
        self.valid = False
        if self.validate_config():
            self.load_config()
            self.valid = True

    def load_config(self) -> None:
        """Loads scenario configuration parameters from the provided scenario config."""
        # todo: these functions don't handle errors gracefully
        # todo: host them here instead of in the config manager?
        self.producer_strat = ScenarioConfigManager.get_producerAssignmentStrategy(
            self.scenario
        )
        self.consumer_strat = ScenarioConfigManager.get_consumerAssignmentStrategy(
            self.scenario
        )
        self.num_producers_per_topic = ScenarioConfigManager.get_numProducersPerTopic(
            self.scenario
        )
        self.num_consumers = ScenarioConfigManager.get_numConsumers(self.scenario)
        self.num_topics = ScenarioConfigManager.get_numTopics(self.scenario)
        self.parallel_channels_per_topic = (
            ScenarioConfigManager.get_parallelSubscriptionsPerTopic(self.scenario)
        )
        self.message_size = ScenarioConfigManager.get_messageSizeBytes(self.scenario)
        self.producer_rate = ScenarioConfigManager.get_producerWaitInMicroSeconds(
            self.scenario
        )
        self.backlog_size = ScenarioConfigManager.get_backlogSizeMessages(self.scenario)
        self.bandwidth = ScenarioConfigManager.get_bandwidthMbps(self.scenario)
        self.latency = ScenarioConfigManager.get_latencyMs(self.scenario)
        self.packet_loss = ScenarioConfigManager.get_packetLossPerc(self.scenario)
        self.jitter = ScenarioConfigManager.get_jitterMs(self.scenario)
        self.number_of_messages = ScenarioConfigManager.get_numberOfMessages(
            self.scenario
        )
        self.duration = ScenarioConfigManager.get_testDurationS(self.scenario)

    def validate_config(self) -> bool:
        """
        Validates the scenario configuration.

        Returns:
            bool: True if the configuration is valid.

        Raises:
            ValueError: If no scenario configuration is provided.
        """
        if self.scenario is None:
            raise ValueError(f"No config provided ({self.scenario}) nor loaded.")
        return True

    @staticmethod
    def build_publisher_config(
        pub_id: int,
        topics: List[str],
        pub_rate: float,
        n_messages: Optional[int] = None,
        duration: Optional[int] = None,
    ) -> Dict[str, Any]:
        return {
            "id": pub_id,
            "topics": topics,
            "pub_rate": pub_rate,
            "n_messages": n_messages,
            "duration": duration,
        }

    @staticmethod
    def build_consumer_config(
        con_id: int, topics: List[str], backlog_size: Optional[int] = None
    ) -> Dict[str, Any]:
        return {"id": con_id, "topics": topics, "backlog_size": backlog_size}

    def publisher_configs(self) -> Dict[int, Dict[str, Any]]:
        """
        Builds publisher configurations based on the scenario settings.

        Returns:
            Dict[int, Dict[str, Any]]: A dictionary mapping publisher IDs to their configurations.
        """
        pub_configs: Dict[int, Dict[str, Any]] = {}
        for i in range(self.num_producers_per_topic):
            pub_id = i
            logger.debug(
                f"building config for producer {pub_id} of {self.num_producers_per_topic}"
            )
            topics = []
            # TODO assignment strategies
            # if self.producer_strat == "round-robin":
            #     topics.append(f"{i%self.num_topics}")
            # elif self.producer_strat == "random":
            #     topics.append(f"{random.choice(range(self.num_topics))}")
            for i in range(self.num_topics):
                topics.append(i)
            logger.info(f"TOPICS = {topics} -> {','.join([str(i) for i in topics])}")

            pub_configs[pub_id] = {
                "pub_id": f"P{pub_id}",
                "topics": [str(i) for i in topics],
                "pub_rate": self.producer_rate,
                "message_size": self.message_size,
                "n_messages": self.number_of_messages,
                "duration": self.duration,
            }
            logger.debug(f"config for publisher {f'P{pub_id}'}: {pub_configs[pub_id]}")
        logger.debug(f"pub_configs: {pub_configs}")
        return pub_configs

    def consumer_configs(self) -> Dict[int, Dict[str, Any]]:
        """
        Builds consumer configurations based on the scenario settings.

        Returns:
            Dict[int, Dict[str, Any]]: A dictionary mapping consumer IDs to their configurations.
        """
        con_configs: Dict[int, Dict[str, Any]] = {}
        for i in range(self.num_consumers):
            con_id = i
            logger.info(
                f"building config for consumer {con_id} of {self.num_consumers}"
            )
            topics = []
            if self.consumer_strat == "round-robin":
                topics.append(f"{i%self.num_topics}")
            if self.consumer_strat == "shared":
                topics = [f"{i}" for i in list(range(self.num_topics))]
            con_configs[con_id] = {
                "con_id": f"C{con_id}",
                "topics": topics,
                "backlog_size": self.backlog_size,
            }
            logger.debug(f"config for consumer {f'C{con_id}'}: {con_configs[con_id]}")
        logger.debug(f"con_configs: {con_configs}")
        return con_configs

    # def get_publishers(self):
    #     return self.config['publishers']

    # def get_publishers_ids(self):
    #     return [pub['id'] for pub in self.config['publishers']]

    # def get_publisher_by_id(self, pub_id):
    #     for pub in self.config['publishers']:
    #         if pub['id'] == pub_id:
    #             return pub
    #     return None

    # def get_consumers(self):
    #     return self.config['consumers']

    # def get_consumers_ids(self):
    #     return [sub['id'] for sub in self.config['consumers']]

    # def get_consumer_by_id(self, sub_id):
    #     for sub in self.config['consumers']:
    #         if sub['id'] == sub_id:
    #             return sub
    #     return None
