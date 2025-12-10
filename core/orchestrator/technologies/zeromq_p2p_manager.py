from ..technology_manager import TechnologyManager, register_technology
from typing_extensions import override

@register_technology("zeromq_p2p")
class ZeroMQP2PManager(TechnologyManager):
    
    def __init__(self, tech_path, network_name = "benchmark_network"):
        TechnologyManager.__init__(self, tech_path, network_name)   
    
    @override
    def setup_tech(self) -> None:
        return
    
    @override
    def reset_tech(self) -> None:
        return
    
    @override
    def teardown_tech(self) -> None:
        return