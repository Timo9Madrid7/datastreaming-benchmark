from typing_extensions import override

from ..technology_manager import TechnologyManager, register_technology


@register_technology("nats_p2p")
class NatsP2PManager(TechnologyManager):

    def __init__(self, tech_path, network_name="benchmark_network"):
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
