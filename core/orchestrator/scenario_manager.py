import json
import random
from .scenario_config_manager import ScenarioConfigManager
from .utils.logger import logger


class ScenarioManager:
    
    def __init__(self, scenario_config = None):
        self.scenario = scenario_config
        self.valid = False
        if self.validate_config():
            self.load_config()
            self.valid = True

    def load_config(self):
        # todo: these functions don't handle errors gracefully
        # todo: host them here instead of in the config manager? 
        self.producer_strat = ScenarioConfigManager.get_producerAssignmentStrategy(self.scenario)
        self.consumer_strat = ScenarioConfigManager.get_consumerAssignmentStrategy(self.scenario)
        self.num_producers_per_topic = ScenarioConfigManager.get_numProducersPerTopic(self.scenario)
        self.num_consumers = ScenarioConfigManager.get_numConsumers(self.scenario)
        self.num_topics = ScenarioConfigManager.get_numTopics(self.scenario)
        self.parallel_channels_per_topic = ScenarioConfigManager.get_parallelSubscriptionsPerTopic(self.scenario)
        self.message_size = ScenarioConfigManager.get_messageSizeBytes(self.scenario)
        self.producer_rate = ScenarioConfigManager.get_producerWaitInMicroSeconds(self.scenario)
        self.backlog_size = ScenarioConfigManager.get_backlogSizeMessages(self.scenario)
        self.bandwidth = ScenarioConfigManager.get_bandwidthMbps(self.scenario)
        self.latency = ScenarioConfigManager.get_latencyMs(self.scenario)
        self.packet_loss = ScenarioConfigManager.get_packetLossPerc(self.scenario)
        self.jitter = ScenarioConfigManager.get_jitterMs(self.scenario)
        self.number_of_messages = ScenarioConfigManager.get_numberOfMessages(self.scenario)
        self.duration = ScenarioConfigManager.get_testDurationS(self.scenario)

    def validate_config(self):
        if self.scenario is None:
            raise ValueError(f"No config provided ({self.scenario}) nor loaded.")
        return True

    @staticmethod
    def build_publisher_config(pub_id, topics, pub_rate, n_messages=None, duration=None):
        return {
                "id": pub_id,
                "topics": topics,
                "pub_rate": pub_rate,
                "n_messages": n_messages,
                "duration": duration
            }
    
    @staticmethod
    def build_consumer_config(con_id, topics, backlog_size=None):
        return {
                "id": con_id,
                "topics": topics,
                "backlog_size": backlog_size
            }
        
    def publisher_configs(self):
        pub_configs = {}
        for i in range(self.num_producers_per_topic):
            pub_id = i
            logger.debug(f"[SM] building config for producer {pub_id} of {self.num_producers_per_topic}")
            topics = []
            # TODO assignment strategies
            # if self.producer_strat == "round-robin":
            #     topics.append(f"{i%self.num_topics}")
            # elif self.producer_strat == "random":
            #     topics.append(f"{random.choice(range(self.num_topics))}")
            for i in range(self.num_topics):
                topics.append(i)
            logger.info(f"[SM] TOPICS = {topics} -> {','.join([str(i) for i in topics])}")
            
            pub_configs[pub_id] = {
                "pub_id": f"P{pub_id}", 
                "topics": [str(i) for i in topics],
                "pub_rate": self.producer_rate,
                "message_size": self.message_size,
                "n_messages": self.number_of_messages,
                "duration": self.duration,
            }
            logger.debug(f"[SM]: config for publisher {f'P{pub_id}'}: {pub_configs[pub_id]}")
        logger.debug(f"[SM]: pub_configs: {pub_configs}")
        return pub_configs
    
    def consumer_configs(self):
        con_configs = {}
        for i in range(self.num_consumers):
            con_id = i
            logger.info(f"[SM] building config for consumer {con_id} of {self.num_consumers}")
            topics = []
            if self.consumer_strat == "round-robin":
                topics.append(f"{i%self.num_topics}")
            if self.consumer_strat == "shared":
                topics = [f"{i}" for i in list(range(self.num_topics))]
            con_configs[con_id] = {
                "con_id": f"C{con_id}", 
                "topics": topics,
                "backlog_size": self.backlog_size
            }
            logger.debug(f"[SM]: config for consumer {f'C{con_id}'}: {con_configs[con_id]}")
        logger.debug(f"[SM]: con_configs: {con_configs}")
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
