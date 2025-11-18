from ..technology_manager import TechnologyManager, register_technology

@register_technology("zeromq_p2p")
class ZeroMQP2PManager(TechnologyManager):
    
    def __init__(self, tech_path, network_name = "benchmark_network"):
        TechnologyManager.__init__(self, tech_path, network_name)   
    
    def setup_tech(self):
        return
    
    def reset_tech(self):
        return
    
    def teardown_tech(self):
        return