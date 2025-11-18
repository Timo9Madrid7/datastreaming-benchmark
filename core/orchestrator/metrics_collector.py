import os
import threading
import time
import docker
import csv
from datetime import datetime
import concurrent.futures

class MetricsCollector:
    def __init__(self, tech_name, scenario_name, scenario_config, interval=2.0):
        self.tech_name = tech_name
        self.logs_dir = os.path.join("logs", scenario_config, tech_name)
        self.log_file = os.path.join(self.logs_dir, f"{scenario_name}")
        self.scenario_name = scenario_name
        self.interval = interval if interval > 2.0 else 2.0
        self.running = False
        self.thread = None
        self.client = docker.from_env()
        self.previous_cpu = {}
        self.previous_system_cpu = {}
        self.metrics = []
        self.fieldnames = [
            "timestamp", "cpu_usage_ns", "cpu_usage_perc", "memory_usage", 
            "network_rx", "network_tx", "disk_read", "disk_write"
        ]

    def start(self):
        """Start the background thread for collecting metrics."""
        if not self.running:
            if not os.path.exists(self.logs_dir):
                os.makedirs(self.logs_dir)
            self.running = True
            self.thread = threading.Thread(target=self._collect_metrics)
            self.thread.start()
            
    def _calculate_cpu_percent(self, container_id, cpu_usage, system_cpu_usage, num_cpus):
        """Calculate CPU usage as a percentage of total system capacity."""
        if container_id in self.previous_cpu and container_id in self.previous_system_cpu:
            # Delta calculation
            delta_cpu = cpu_usage - self.previous_cpu[container_id]
            delta_system_cpu = system_cpu_usage - self.previous_system_cpu[container_id]

            if delta_system_cpu > 0 and num_cpus > 0:
                cpu_percent = (delta_cpu / delta_system_cpu) * num_cpus * 100
                return round(cpu_percent, 2)
        
        return 0.0

    def _collect_metrics(self):
        print(f"[MC] Starting metrics collection for '{self.tech_name}'...")
        containers = self.client.containers.list(filters={"name": f"{self.tech_name}-*"})
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            fc = {}
            for container in containers:
                futures.append(executor.submit(self.collect_metrics_container, container))
                fc[futures[-1]] = container.name
                print(f"[MC] Metrics collection started for container: {container.name}")
            for future in concurrent.futures.as_completed(futures):
                try:
                    print(f"[MC] Metrics collection completed for {fc[future]}: {future.result()}")
                except Exception as e:
                    print(f"[MC] Error in metrics collection thread: {e} in future {future.__repr__()}")
        print(f"[MC] Metrics collection finished for all containers.")

            

    def collect_metrics_container(self, container):
        num_cpus = self.client.info().get("NCPU", 1)
        # Open the file once and keep appending to avoid file locks
        container_file = self.log_file + f"_{container.name}.csv"
        nlogs = 0
        with open(container_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=self.fieldnames)
            writer.writeheader()
            while self.running:
                try:
                    timestamp = datetime.now().isoformat()
                    stats = container.stats(stream=False)

                    # Collect metrics
                    cpu_usage_ns = stats.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0)
                    system_cpu_usage = stats.get("cpu_stats", {}).get("system_cpu_usage", 0)
                    memory_usage = stats.get("memory_stats", {}).get("usage", 0)
                    network_rx = sum(v.get("rx_bytes", 0) for v in stats.get("networks", {}).values())
                    network_tx = sum(v.get("tx_bytes", 0) for v in stats.get("networks", {}).values())
                    blkio_stats = stats.get("blkio_stats", {}).get("io_service_bytes_recursive", [])
                    disk_read = sum(x.get("value", 0) for x in blkio_stats if x.get("op") == "Read")
                    disk_write = sum(x.get("value", 0) for x in blkio_stats if x.get("op") == "Write")
                    
                    # Calculate CPU percentage
                    cpu_usage_perc = self._calculate_cpu_percent(container.id, cpu_usage_ns, system_cpu_usage, num_cpus)
                    self.previous_cpu[container.id] = cpu_usage_ns
                    self.previous_system_cpu[container.id] = system_cpu_usage

                    # Write row to CSV
                    writer.writerow({
                        "timestamp": timestamp,
                        "cpu_usage_ns": cpu_usage_ns,
                        "cpu_usage_perc": cpu_usage_perc,
                        "memory_usage": memory_usage,
                        "network_rx": network_rx,
                        "network_tx": network_tx,
                        "disk_read": disk_read,
                        "disk_write": disk_write
                    })
                except TypeError as e:
                    return f"TypeError after {nlogs}  logs. Probably the container is not running: {e}"
                except Exception as e:
                    return (f"Error while collecting metrics: {e}")
                
                print(f"[MC] Metrics for {container.name} collected at {timestamp}, waiting for {self.interval}s...")
                time.sleep(self.interval)
                nlogs += 1
        return f"{nlogs} logs collected"


    def stop(self):
        """Stop the background thread and save metrics to file."""
        if self.running:
            print("[MC] Stopping metrics collection...")
            self.running = False
        self.thread.join()
        print(f"[MC] Metrics saved to {self.log_file}_<container_name>.csv")

