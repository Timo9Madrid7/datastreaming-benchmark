from abc import ABC, abstractmethod
from typing import Dict, Type
import importlib
import os
import json
import re

from .utils.logger import logger

class TechnologyManager (ABC):
    
    def __init__(self, tech_path: str, network_name = "benchmark_network"):
        self.tech_path = tech_path
        self.tech_name = os.path.basename(tech_path)
        self.network_name = network_name

    @abstractmethod
    def setup_tech(self) -> None:
        pass
    
    @abstractmethod
    def teardown_tech(self):
        pass
        
    @abstractmethod
    def reset_tech(self):
        pass
    
    def extract_runtime_container_config(self, container):
        logs = container.logs().decode("utf-8").strip().split("\n")
        pattern = r'.*\[CONFIG\] (?P<key>[^=]+)=(?P<value>.+)'
        config = {}
        inside_block = False
        for line in logs:
            if "[CONFIG_BEGIN]" in line:
                inside_block = True
                continue
            if "[CONFIG_END]" in line:
                break
            if inside_block:
                match = re.match(pattern, line)
                if match:
                    key = match.group("key").strip()
                    value = match.group("value").strip()
                    config[key] = value
        return config
    
    def save_runtime_container_config(self, container, scenario_config, scenario_name):
        config = self.extract_runtime_container_config(container)
        config_file = os.path.join("logs", scenario_config, self.tech_name, f"{scenario_name}_{container.name}_runtimeconfig.json")
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4)
        logger.info(f"[TM] Saved {container.name} config to {config_file}")
            
    def validate_technology(self) -> bool:
        for dockerfile in [self.base_dockerfile(), self.publisher_dockerfile(), self.consumer_dockerfile()]:
            if not os.path.exists(dockerfile):
                return False
        return True
    
    def base_dockerfile(self) -> str:
        return os.path.join(self.tech_path, "Dockerfile.base")
    
    def publisher_dockerfile(self) -> str:
        return os.path.join(self.tech_path, "Dockerfile.publisher")
    
    def consumer_dockerfile(self) -> str:
        return os.path.join(self.tech_path, "Dockerfile.consumer")
    

technology_registry: Dict[str, Type[TechnologyManager]] = {}

def register_technology(name: str):
    def wrapper(cls: Type[TechnologyManager]):
        if name in technology_registry:
            logger.error(f"Technology '{name}' is already registered.")
            raise ValueError(f"Technology '{name}' is already registered.")
        technology_registry[name] = cls
        return cls
    return wrapper

def get_technology_manager(name: str) -> Type[TechnologyManager]:
    if name not in technology_registry:
        try:
            logger.info(f"Attempt to dynamically import the module responsible for {name}")
            importlib.import_module(f"core.orchestrator.technologies.{name}_manager")
        except ModuleNotFoundError as e:
            logger.error(f"Technology module '{name}_manager.py' could not be found.")
            raise ValueError(f"Technology module '{name}_manager.py' not found.") from e
    
    return technology_registry[name]