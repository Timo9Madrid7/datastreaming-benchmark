import json
import itertools
from typing import Dict, Union, Optional, Iterable
from .utils.logger import logger

EXCLUSIVE_MSG = "numberOfMessages"
EXCLUSIVE_TIME = "testDurationS"

class ScenarioConfig:
    COMMON = "common"
    NETWORK = "network"
    EXCLUSIVE = "exclusive"

class ScenarioConfigManager:
    
    def __init__(self, config_file):
        self.assignment_strats = [
            "producerAssignmentStrategy",
            "consumerAssignmentStrategy"
        ]
        self.common_parts = [
            "numProducersPerTopic",
            "numConsumers", 
            "numTopics", 
            "parallelSubscriptionsPerTopic",
            "messageSizeBytes",
            "producerWaitInMicroSeconds",
            "backlogSizeMessages"
        ]
        self.network_parts = [
            "bandwidthMbps",
            "latencyMs",
            "packetLossPerc",
            "jitterMs"
        ]
        self.exclusive_parts = [
            EXCLUSIVE_TIME,
            EXCLUSIVE_MSG
        ]
        logger.info(f"Scenario config file: {config_file}")
        with open(config_file, 'r', encoding='utf-8') as file:
            self.config = json.load(file)
        
    @staticmethod
    def iter_over_range(lower: Union[int, float], upper: Union[int, float], step: Union[int, float], step_operator: str, midpoint: Union[int, float, None] = None, step2: Union[int, float, None] = None, step_operator2: Optional[str] = None) -> Iterable[Union[int, float]]:
        current = lower - step if step_operator == '+' else lower / step # hack to yield `lower` as the first value
        while current <= upper:
            if midpoint and step2 and current >= midpoint:
                current = current + step2 if step_operator2 == '+' else current * step2
            else:
                current = current + step if step_operator == '+' else current * step
            if current > upper:
                return
            yield current
    
    def iter_valid_combinations(self, exclusive_part: str) -> Iterable[Dict[str, Union[int, float, str]]]:
        generators: Dict[str, Iterable[Union[int, float, str]]] = {}
        for common_part in self.common_parts:
            generators[common_part] = self.iter_over_range(**self.config[ScenarioConfig.COMMON][common_part])
        for network_part in self.network_parts:
            generators[network_part] = self.iter_over_range(**self.config[ScenarioConfig.NETWORK][network_part])
        for strat in self.assignment_strats:
            generators[strat] = iter(self.config[strat])
        generators[exclusive_part] = self.iter_over_range(**self.config[ScenarioConfig.EXCLUSIVE][exclusive_part])
        for scenario_config in itertools.product(*generators.values()):
            scenario = dict(zip(generators.keys(), scenario_config))
            yield scenario
    
    @staticmethod
    def generate_scenario_name(scenario: Dict[str, Union[int, float, str]]) -> str:
        name_parts = []

        # Common messaging identifiers
        p = ScenarioConfigManager.get_numProducersPerTopic(scenario)
        c = ScenarioConfigManager.get_numConsumers(scenario)
        t = ScenarioConfigManager.get_numTopics(scenario)
        # pc = ScenarioConfigManager.get_parallelSubscriptionsPerTopic(scenario)
        b = ScenarioConfigManager.get_messageSizeBytes(scenario)
        # w = ScenarioConfigManager.get_producerWaitInMicroSeconds(scenario)
        # bm = ScenarioConfigManager.get_backlogSizeMessages(scenario)
        name_parts.append(f"{p}p{c}c{t}t{b}b")
        # name_parts.append(f"{p}p{c}c{t}t{pc}pc{b}b{w}us{bm}bm")

        # Exclusive mode
        if EXCLUSIVE_TIME in scenario:
            name_parts.append(f"{int(scenario[EXCLUSIVE_TIME])}s")
        elif EXCLUSIVE_MSG in scenario:
            name_parts.append(f"{int(scenario[EXCLUSIVE_MSG])}m")

        # NOTE: not used anymore since we want to test how fast things can go without any meddling
        # # Network-related identifiers
        # bw = ScenarioConfigManager.get_bandwidthMbps(scenario)
        # lat = ScenarioConfigManager.get_latencyMs(scenario)
        # pl = ScenarioConfigManager.get_packetLossPerc(scenario)
        # jit = ScenarioConfigManager.get_jitterMs(scenario)
        # name_parts.append(f"{bw}mbps{lat}ms{pl}pl{jit}j")

        return "-".join(name_parts).replace('.','_')
    
    @staticmethod
    def get_producerAssignmentStrategy(scenario: Dict[str, Union[int, float, str]]) -> str:
        return scenario['producerAssignmentStrategy']
    
    @staticmethod
    def get_consumerAssignmentStrategy(scenario: Dict[str, Union[int, float, str]]) -> str:
        return scenario['consumerAssignmentStrategy']
    
    @staticmethod
    def get_numProducersPerTopic(scenario: Union[int, float, str]) -> int:
        return scenario['numProducersPerTopic']
 
    @staticmethod
    def get_numConsumers(scenario: Union[int, float, str]) -> int:
        return scenario['numConsumers']
 
    @staticmethod
    def get_numTopics(scenario: Union[int, float, str]) -> int:
        return scenario['numTopics']
 
    @staticmethod
    def get_parallelSubscriptionsPerTopic(scenario: Union[int, float, str]) -> int:
        return scenario['parallelSubscriptionsPerTopic']

    @staticmethod
    def get_messageSizeBytes(scenario: Union[int, float, str]) -> int:
        return int(scenario['messageSizeBytes'])

    @staticmethod
    def get_producerWaitInMicroSeconds(scenario: Dict[str, Union[int, float, str]]) -> int:
        return scenario['producerWaitInMicroSeconds']

    @staticmethod
    def get_backlogSizeMessages(scenario: Dict[str, Union[int, float, str]]) -> int:
        return scenario['backlogSizeMessages']


    @staticmethod
    def get_bandwidthMbps(scenario: Dict[str, Union[int, float, str]]) -> int:
        return scenario['bandwidthMbps']

    @staticmethod
    def get_latencyMs(scenario: Dict[str, Union[int, float, str]]) -> int:
        return scenario['latencyMs']

    @staticmethod
    def get_packetLossPerc(scenario: Dict[str, Union[int, float, str]]) -> float:
        return scenario['packetLossPerc']

    @staticmethod
    def get_jitterMs(scenario: Dict[str, Union[int, float, str]]) -> int:
        return scenario['jitterMs']


    @staticmethod
    def get_numberOfMessages(scenario: Union[int, float, str]) -> Optional[int]:
        if 'numberOfMessages' in scenario:
            return scenario['numberOfMessages']
        else:
            return None

    @staticmethod
    def get_testDurationS(scenario: Dict[str, Union[int, float, str]]) -> Optional[float]:
        if 'testDurationS' in scenario:
            return scenario['testDurationS']
        else:
            return None
