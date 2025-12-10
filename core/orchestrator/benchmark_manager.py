import os
import json
from typing import Optional, List, Dict, Union
from .technology_manager import TechnologyManager, get_technology_manager
from .scenario_manager import ScenarioManager
from .container_manager import ContainerManager
from .metrics_collector import MetricsCollector
from .events_logger import ContainerEventsLogger
from .scenario_config_manager import ScenarioConfigManager, EXCLUSIVE_MSG, EXCLUSIVE_TIME
from .utils.logger import logger

TECHNOLOGIES_DIR = "technologies"
SCENARIOS_DIR = "test_scenarios"

class BenchmarkScenarios:
    SCENARIO_BATCH = "scenario_batch"
    TECHNOLOGIES = "technologies"

class BenchmarkManager:
    
    def __init__(self, config_path: str, metrics_interval: float = 2.0) -> None:
        self.interval = metrics_interval
        
        with open(config_path, 'r', encoding='utf-8') as file:
            self.config = json.load(file)
        self.scenario_config_files = self.config[BenchmarkScenarios.SCENARIO_BATCH]
        
        self.scenario_name = ""
        
        self.cm = ContainerManager()
        self.scm = None
        self.tm = None

    def run(self, mode: Optional[str] = None, duration_messages: Optional[str] = None) -> None:
        scenario_batch: List[str] = self.config[BenchmarkScenarios.SCENARIO_BATCH]
        for scenario in scenario_batch:
            self.run_config(scenario, mode, duration_messages = duration_messages)
    
    def run_config(self, scenario: str, mode: Optional[str] = None, duration_messages: str = "md") -> None:
        self.scm = ScenarioConfigManager(os.path.join(SCENARIOS_DIR, scenario))
        self.scenario_name = scenario.split(".json")[0]
        logger.info(f'Using scenario_config from {self.scenario_name}')
        
        technologies: List[str] = self.config[BenchmarkScenarios.TECHNOLOGIES]
        for tech_name in technologies:
            self.tm = get_technology_manager(tech_name)(os.path.join(TECHNOLOGIES_DIR, tech_name))
            if not self.tm.validate_technology():
                logger.error(f"Technology {tech_name} validation failed.")
                raise ValueError(f"Invalid technology: {tech_name}")
            
            logger.info(f'Running experiments for technology {tech_name} in mode {mode}...')
            if "m" in duration_messages:
                for scenario_messages in self.scm.iter_valid_combinations(EXCLUSIVE_MSG):
                    self.execute_experiment(tech_name, scenario_messages, mode)
            if "d" in duration_messages:
                for scenario_time in self.scm.iter_valid_combinations(EXCLUSIVE_TIME):
                    self.execute_experiment(tech_name, scenario_time, mode)
            self.tm = None

    def execute_experiment(self, tech_name: str, scenario_config: Dict[str, Union[int, float]], mode: Optional[str] = None) -> None:
        self.cm.reset_between_experiments()
        
        logger.info(f'Setting up technology {tech_name}...')
        self.tm.setup_tech()
        
        scenario_name = ScenarioConfigManager.generate_scenario_name(scenario_config)
        metrics = MetricsCollector(tech_name, scenario_name, self.scenario_name, interval=self.interval)
        os.makedirs(os.path.join("logs", self.scenario_name, tech_name), exist_ok=True)
        try:
            logger.info(f'Executing experiment for technology {tech_name} with scenario {scenario_name}...')
            logger.info(f'Starting and pausing all containers in mode {mode}...')
            sm = ScenarioManager(scenario_config)
            for p_id, p_config in sm.publisher_configs().items():
                logger.debug(f'Publisher config: {p_config}')
                container = self.cm.start_publisher(
                    tech_name = tech_name,
                    **p_config,
                    mode = mode
                )
                #todo save p_config
                config_file = os.path.join("logs", self.scenario_name, tech_name, f"{scenario_name}_{container}_scenarioconfig.json")
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(p_config, f, indent=4)
                    logger.debug(f'Publisher {container} started with config {p_config}')
                # if not container_manager.is_healthy(container_id):
                #     raise ValueError(f"Publisher {pub_config['id']} failed to start correctly.")

            for c_id, c_config in sm.consumer_configs().items():
                logger.debug(f'Consumer config: {c_config}')
                container = self.cm.start_consumer(
                    tech_name, 
                    **c_config, 
                    mode = mode
                )
                config_file = os.path.join("logs", self.scenario_name, tech_name, f"{scenario_name}_{container}_scenarioconfig.json")
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(c_config, f, indent=4)
                    logger.debug(f'Consumer {container} started with config {c_config}')
                # if not container_manager.is_healthy(container_id):
                #     raise ValueError(f"Consumer {sub_config['id']} failed to start correctly.")
            
            metrics.start()
            logger.info("All containers started. Unpausing...")
            self.cm.wake_all()
            logger.info("Containers unpaused. Collecting metrics...")
            self.cm.wait_for_all()
            metrics.stop()
            events_logger = ContainerEventsLogger(tech_name, scenario_name, self.scenario_name)
            events_logger.collect_logs()
            events_logger.write_logs()
            for container in self.cm.containers:
                if "broker" not in container.name:
                    self.tm.save_runtime_container_config(container, self.scenario_name, scenario_name)

        finally:
            logger.info("Stopping and removing all containers...")
            self.cm.stop_all()
            self.cm.remove_all()
            logger.info(f"All containers stopped and removed for {tech_name}.")
            self.tm.teardown_tech()
            logger.info(f"Teardown completed for {tech_name}.")