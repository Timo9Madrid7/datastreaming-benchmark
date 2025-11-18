# Messaging Benchmark Framework

This project provides an extensible framework to benchmark messaging technologies in controlled, repeatable conditions. The system is designed for internal experiments to evaluate latency, throughput, and overhead under various network and scenario configurations.

---

## 🧭 Project Purpose

This tool exists to support **internal, comparative evaluation** of message-passing technologies. It enables structured experimentation across combinations of:
- Message sizes and frequencies
- Numbers of producers, consumers, and topics
- Subscription patterns and topic multiplexing
- Network conditions (latency, jitter, bandwidth, loss)

The output is a basis for comparing **performance**, **overhead**, and **behavioral characteristics** across technologies.

---

## 🎯 High-Level Goals

1. **Isolate and measure technology overhead**  
   Capture the cost of using Kafka, ZeroMQ, etc., beyond the base network transport.

2. **Support modular experimentation**  
   New scenarios and technologies can be tested without modifying core logic.

3. **Automate end-to-end test orchestration**  
   Given a configuration file, the system launches the required containers, injects environment, collects logs, and tears down after execution.

---

## 🧱 Design Overview

### Architecture Principles

- **Separation of Concerns**
  - Core logic defines interfaces and orchestration, not implementation specifics.
  - Tech-specific code lives in loadable modules (`.so`, `.dll`).

- **Dynamic Factory + Shared Libs**
  - Messaging implementations register themselves at runtime via factories.

- **Technology Isolation via Docker**
  - One image per publisher/consumer pair per tech.
  - Shared `Dockerfile.base` for build dependencies.

- **Interface-Based Extensibility**
  - `IPublisher` and `IConsumer` define the contract.
  - Payloads are lightweight (`label + vector<double>`).

- **Scenario-as-Data**
  - Experiment dimensions (e.g. producers, size, rate, etc.) come from JSON.
  - The orchestrator uses these configs to coordinate container deployment.

---

## 🧪 Experiment Flow

1. Define a test scenario in JSON (see `test_scenarios/quick_test.json` for template).
2. The orchestrator reads the config, generates combinations, and launches the matching containers.
3. All containers are paused at startup, then synchronized and unpaused together.
4. Metrics and events are logged.
5. Containers terminate on poison-pill signals and are then cleaned up.

---

## 🗂 Project Structure (Core-Only)

    core/                        # Contains common logic files and modules
    ├── analyses/                   # Contains notebooks to inspect retrieved experimental data
    │   ├── messaging_stats.ipynb      # Visualize graphs based on messaging-related data from the experiments
    │   ├── resources_stats.ipynb      # Visualize graphs based on resources usage data from the experiments
    ├── applications/               # PublisherApp and ConsumerApp (main executables)
    ├── factory/                    # Factory pattern logic for dynamic tech binding
    ├── interfaces/                 # Core abstractions: IPublisher, IConsumer
    ├── logger/                     # Logger implementation with level-based control
    ├── orchestrator/               # Python modules for scenario execution and orchestration
    │   ├── benchmark_manager.py       # Main entry point for experiment lifecycle
    │   ├── container_manager.py       # Handles Docker container management logic
    │   ├── events_logger.py           # Retrieves messages logged by the containers
    │   ├── metrics_collector.py       # Monitors system performance 
    │   ├── scenario_manager.py        # Instantiates scenarios from JSON
    │   ├── scenario_config_manager.py # Iterates and validates scenarios
    │   ├── technology_manager.py      # Interface that technologies must implement beyond IPublisher and IConsumer 
    │   └──technologies/               # Python module with technology-specific implementations of the TechnologyManager interface
    ├── payload/                    # Message structures that the benchmark supports
    ├── technology_loader/          # Handles technology-specific plugin dynamic loading
    ├── Dockerfile.base             # Base image with C++ build dependencies
    ├── Dockerfile.publisher        # Publisher-specific image (extends base)
    ├── Dockerfile.consumer         # Consumer-specific image (extends base)
    diagrams/                    # Contains diagrams defined using PlantUML to help understand the architecture
    logs/                        # Contains benchmark log files under their scenario_config json file and technology name folders.
    ├── quick_test/                 # Contains logs related to the benchmarking of quick_test scenario conditions
    │   ├── kafka/                     # Contains logs from the Kafka implementation in this benchmarking experiment
    │   ├── zeromq_p2p/                # Contains logs from the ZeroMQ implementation in this benchmarking experiment
    technologies/                # Contains technology-specific Docker images definition as well as implementations of IPublisher, IConsumer, and their registration logic
    ├── kafka/                      # Kafka implementation
    ├── zeromq_p2p/                 # ZeroMQ implementation
    └── ...                         # Additional tech modules

    test_scenarios/              # Contains scenario configuration json files
    ├── quick_test.json             # Scenario config: topics, producers, rate, etc.
    └── ...                         # Optional experimental configurations
    benchmark_scenarios.json     # Parameterizes scenario configuration and technologies to use in the benchmarking experiment
    build_all_images.bat         # Creates Docker images for all technologies. Extend as new technologies are  implemented
    execute_experiments.py       # Entry point to execute an experiment. Admits 2 parameters: mode and duration_messages. The first is a logger level as defined in core/logger. The second is a string used to filter subsets of the configurable scenarios based on their completion criteria.
    requirements.txt
    README.md
    setup_instructions.md

---

## ⚙️ Technologies

Each messaging technology lives in its own subdirectory under `technologies/`. Each implementation must:
- Extend `IPublisher` and `IConsumer`
- Register itself via the factory
- Compile into a shared object

Each also gets its own Dockerfiles for consumer/publisher images.

---

## 📌 Notes

- Have the combinatory explosion threat in mind when designing scenario configuration dimension parameters.
- Technology-specific implementations belong to 2 scopes: their `technology` subfolder for messaging interfaces definition and registration, and in the `core/orchestrator/technology_loader/` folder for technology-specific global setup handling.
- The orchestrator uses Docker Python API — make sure there is access to the Docker daemon.

---

## 🧠 For Extension

When adding a new tech or extending the scenario model:
- Follow the interface and registration pattern — nothing in `core/` should need to change except for the addition of a new python module in `core/orchestrator/technology_loader/`.
- Keep Docker images lean: start from `Dockerfile.base`, add only what’s necessary.
- Update the orchestrator only if scenario structure or orchestration behavior needs to evolve.

---

