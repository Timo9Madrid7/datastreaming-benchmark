# Messaging Benchmark Framework

This project provides an extensible framework to benchmark messaging technologies in controlled, repeatable conditions. It is designed for internal experiments to evaluate latency, throughput, and overhead under varied network and scenario configurations while keeping experiments reproducible and comparable.

---

## 🧭 Project Purpose

This tool exists to support **internal, comparative evaluation** of message-passing technologies. It enables structured experimentation across combinations of:
- Message sizes and frequencies
- Numbers of producers, consumers, and topics
- Subscription patterns and topic multiplexing
- Network conditions (latency, jitter, bandwidth, loss)

The output is a basis for comparing **performance**, **overhead**, and **behavioral characteristics** across technologies with consistent orchestration and logging.

---

## 🎯 High-Level Goals

1. **Isolate and measure technology overhead**  
  Capture the cost of using Kafka, ZeroMQ, Arrow Flight, NATS, and RabbitMQ beyond the base network transport.

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
  - Shared `Dockerfile.base` for build dependencies and consistent build inputs.

- **Interface-Based Extensibility**
  - `IPublisher` and `IConsumer` define the contract.
  - Payloads are lightweight (`label + vector<double>`).

- **Scenario-as-Data**
  - Experiment dimensions (e.g., producers, size, rate) come from JSON.
  - The orchestrator uses these configs to coordinate container deployment.

---

## 🧪 Experiment Flow

1. Define a test scenario in JSON (see `test_scenarios/quick_test.json` for a template).
2. The orchestrator reads the config, generates combinations, and launches the matching containers.
3. All containers are paused at startup, then synchronized and unpaused together.
4. Metrics and events are logged per scenario and technology.
5. Containers terminate on poison-pill signals and are then cleaned up.

---

## ✅ Quickstart

1. Follow the environment setup in `setup_instructions.md`.
2. Build all technology images with `build_all_images.bat`.
3. Run a scenario via `execute_experiments.py`.
4. Inspect results under `logs/` and use the `analysis/` helpers to plot metrics.

---

## 🧰 Prerequisites

- Docker (daemon accessible from the host running the orchestrator)
- CMake + a C++ toolchain for the core applications
- Python environment for the orchestrator and analysis helpers

---

## 🧾 Configuration

- `benchmark_scenarios.json` defines which technologies and scenario files to execute.
- `test_scenarios/` holds the scenario templates (message sizes, rates, topics, producers, consumers).
- Technology-specific container behavior is defined under `technologies/` and hooked through the orchestrator’s technology loader.

---

## 🗂 Project Structure

    analysis/                     # Python utilities for loading results and building plots
    ├── data_loader.py              # Load experiment logs and scenario metadata
    ├── metrics.py                  # Compute derived metrics from raw logs
    └── visuals.py                  # Charts and report visualizations
    core/                         # C++ core logic and orchestration tooling
    ├── applications/               # PublisherApp and ConsumerApp (main executables)
    ├── factory/                    # Factory pattern logic for dynamic tech binding
    ├── interfaces/                 # Core abstractions: IPublisher, IConsumer
    ├── logger/                     # Logger implementation with level-based control
    ├── orchestrator/               # Python modules for scenario execution and orchestration
    │   ├── benchmark_manager.py       # Main entry point for experiment lifecycle
    │   ├── container_manager.py       # Docker container management logic
    │   ├── events_logger.py           # Retrieves messages logged by the containers
    │   ├── metrics_collector.py       # Monitors system performance
    │   ├── scenario_manager.py        # Instantiates scenarios from JSON
    │   ├── scenario_config_manager.py # Iterates and validates scenarios
    │   ├── technology_manager.py      # Technology manager interface
    │   └── technologies/              # Tech-specific implementations of TechnologyManager
    ├── payload/                    # Message structures that the benchmark supports
    ├── technology_loader/          # Handles technology-specific plugin dynamic loading
    ├── Dockerfile.base             # Base image with C++ build dependencies
    ├── Dockerfile.publisher        # Publisher-specific image (extends base)
    └── Dockerfile.consumer         # Consumer-specific image (extends base)
    technologies/                # Tech-specific Docker images + IPublisher/IConsumer implementations
    ├── arrowflight_bin_p2p/         # Arrow Flight (binary payload) implementation
    ├── arrowflight_p2p/             # Arrow Flight implementation
    ├── kafka_p2p/                   # Kafka implementation
    ├── nats_p2p/                    # NATS implementation
    ├── rabbitmq_p2p/                # RabbitMQ implementation
    └── zeromq_p2p/                  # ZeroMQ implementation
    test_scenarios/              # Scenario configuration JSON files
    ├── quick_test.json             # Scenario config: topics, producers, rate, etc.
    └── ...                         # Additional experimental configurations
    logs/                        # Benchmark log files organized per scenario and technology
    diagrams/                    # Architecture diagrams (PlantUML)
    third_party_libs/            # Vendored dependencies (e.g., spsc_queue, thread_pool)
    build/                       # CMake build artifacts
    app.py                       # CLI entry point for analysis helpers
    execute_experiments.py       # Entry point to execute an experiment (mode + duration_messages)
    benchmark_scenarios.json     # Parameterizes scenario configuration and technologies to run
    build_all_images.bat         # Builds Docker images for all technologies
    clean_log_tech.bat           # Removes technology-specific log folders
    CMakeLists.txt               # Root CMake configuration
    requirements.txt
    pyproject.toml
    README.md
    setup_instructions.md

---

## ⚙️ Technologies

Each messaging technology lives in its own subdirectory under `technologies/`. Each implementation must:
- Extend `IPublisher` and `IConsumer`
- Register itself via the factory
- Compile into a shared object

Each also gets its own Dockerfiles for consumer/publisher images.

Current implementations include Arrow Flight, Kafka, NATS, RabbitMQ, and ZeroMQ, with both standard and binary-payload variants where applicable.

---

## 📦 Outputs & Logs

- Logs are written under `logs/` and grouped by scenario and technology.
- The analysis helpers in `analysis/` can load these logs and produce charts for latency, throughput, and resource metrics.

---

## 📌 Notes

- Have the combinatory explosion threat in mind when designing scenario configuration dimension parameters.
- Technology-specific implementations belong to 2 scopes: their `technology` subfolder for messaging interfaces definition and registration, and in the `core/orchestrator/technology_loader/` folder for technology-specific global setup handling.
- The orchestrator uses Docker Python API — make sure there is access to the Docker daemon.
- Large scenario grids can grow quickly; tune sizes, rates, and durations to keep runs manageable.

---

## 🧠 For Extension

When adding a new tech or extending the scenario model:
- Follow the interface and registration pattern — nothing in `core/` should need to change except for the addition of a new python module in `core/orchestrator/technology_loader/`.
- Keep Docker images lean: start from `Dockerfile.base`, add only what’s necessary.
- Update the orchestrator only if scenario structure or orchestration behavior needs to evolve.

---

