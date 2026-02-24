# Messaging Benchmark Framework

This project provides an extensible framework to benchmark messaging technologies in
controlled, repeatable conditions. It supports internal experiments for latency,
throughput, and overhead under varied scenario configurations while keeping results
reproducible and comparable.

## Purpose

The framework supports comparative evaluation of message-passing technologies across
combinations of:

- Message sizes and rates
- Numbers of producers, consumers, and topics
- Subscription patterns and topic multiplexing
- Network conditions (latency, jitter, bandwidth, loss)

The output is a consistent basis for comparing performance, overhead, and behavioral
characteristics across technologies.

## High-Level Goals

1. Isolate and measure technology overhead beyond base network transport.
2. Support modular experimentation without changing core logic.
3. Automate end-to-end test orchestration from a configuration file.

## Design Overview

### Architecture Principles

- Separation of concerns: core logic defines interfaces and orchestration.
- Dynamic factory + shared libs: implementations register at runtime.
- Technology isolation via Docker: one publisher/consumer image per technology.
- Interface-based extensibility: `IPublisher` and `IConsumer` define the contract.
- Scenario-as-data: experiment dimensions come from JSON configs.

## Experiment Flow

1. Define a test scenario in JSON (see [test_scenarios/1p1c1t_10MB-30s-demo.json](test_scenarios/1p1c1t_10MB-30s-demo.json)).
2. The orchestrator reads the config, generates combinations, and launches containers.
3. Containers are paused, synchronized, and unpaused together.
4. Metrics and events are logged per scenario and technology.
5. Containers terminate on poison-pill signals and are cleaned up.

## Prerequisites

- Docker
- Recommended: Conda
- Python 3.10+
- Optional: CMake and a C++17 toolchain

## Quickstart

```shell
.\build_images.bat

# optional
conda create -n bm python=3.10
conda activate bm

pip install docker loguru pandas polars streamlit

# Modify/create *.json files in test_scenarios/ to define your scenarios
# Modify benchmark_scenarios.json to specify scenarios and technologies

# python execute_experiments.py [STUDY/CONFIG/DEBUG/INFO/ERROR] [d/m/dm]
# arg1: log level inside containers (STUDY for benchmarking evaluation)
# arg2: run mode (d=duration-based, m=message-count-based, dm=both)
python execute_experiments.py STUDY d

# After execution, for visualization and analysis, run:
streamlit run app.py
```

## Configuration

- [benchmark_scenarios.json](benchmark_scenarios.json) defines which technologies and
  scenario files to execute.
- [test_scenarios/](test_scenarios/) holds scenario templates (sizes, rates, topics,
  producers, consumers).
- Technology-specific container behavior is defined under [technologies/](technologies/)
  and wired through the orchestrator's technology loader.

## Project Structure

```shell
analysis/                 # Python utilities for loading results and building plots
  data_loader.py          # Load experiment logs and scenario metadata
  metrics.py              # Compute derived metrics from raw logs
  visuals.py              # Charts and report visualizations
core/                     # C++ core logic and orchestration tooling
  applications/           # PublisherApp and ConsumerApp executables
  factory/                # Factory pattern logic for dynamic tech binding
  interfaces/             # Core abstractions: IPublisher, IConsumer
  logger/                 # Logger implementation with level-based control
  orchestrator/           # Python modules for scenario execution
    benchmark_manager.py
    container_manager.py
    events_logger.py
    metrics_collector.py
    scenario_manager.py
    scenario_config_manager.py
    technology_manager.py
    technologies/          # Tech-specific TechnologyManager implementations
  payload/                # Message structures supported by the benchmark
  technology_loader/      # Handles technology-specific plugin dynamic loading
  Dockerfile.base
  Dockerfile.publisher
  Dockerfile.consumer
technologies/             # Tech-specific Docker images + IPublisher/IConsumer impls
  arrowflight_bin_p2p/
  arrowflight_p2p/
  kafka_p2p/
  nats_jetstream_p2p/
  nats_p2p/
  rabbitmq_p2p/
  zeromq_p2p/
test_scenarios/           # Scenario configuration JSON files
logs/                     # Benchmark logs grouped by scenario and technology
third_party_libs/         # Vendored dependencies (spsc_queue, thread_pool)
app.py                    # Streamlit analysis UI
execute_experiments.py    # Experiment runner
benchmark_scenarios.json  # Scenario batch and technologies to run
build_imgeas.bat          # Builds Docker images for all technologies
CMakeLists.txt
pyproject.toml
README.md
setup_instructions.md
```

## Technologies

Each messaging technology lives in its own subdirectory under [technologies/](technologies/).
Each implementation must:

- Extend `IPublisher` and `IConsumer`.
- Register itself via the factory.
- Compile into a shared object.

Each also has Dockerfiles for consumer/publisher images. Current implementations
include Arrow Flight, Kafka, NATS, RabbitMQ, and ZeroMQ, with both standard and
binary-payload variants where applicable.

## Outputs and Logs

- Logs are written under [logs/](logs/) and grouped by scenario and technology.
- The analysis helpers in [analysis/](analysis/) load logs and produce charts for
  latency, throughput, and resource metrics.

## Notes

- Scenario grids can grow quickly; tune sizes, rates, and durations to keep runs
  manageable.
- Tech-specific implementations exist in two scopes: their technology folder and the
  matching orchestrator loader in `core/orchestrator/technology_loader/`.
- The orchestrator uses the Docker Python API; ensure Docker daemon access.

## Extending

When adding a new technology or extending the scenario model:

- Follow the interface and registration pattern; core logic should not change.
- Keep Docker images lean; start from the base Dockerfile.
- Update orchestration only if scenario structure or lifecycle changes are required.
