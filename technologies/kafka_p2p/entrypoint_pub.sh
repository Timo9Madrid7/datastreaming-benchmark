#!/bin/sh
set -e

# Ensure Kafka advertises an address reachable from other containers.
# Using the container IP avoids Docker DNS edge-cases (e.g., underscores in names).
BROKER_IP="$(hostname -i | awk '{print $1}')"
if [ -z "$BROKER_IP" ]; then
  BROKER_IP="127.0.0.1"
fi

# Patch KRaft config for this container instance.
sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://${BROKER_IP}:9092,CONTROLLER://${BROKER_IP}:9093|" /app/technologies/kafka_p2p/server.properties

KAFKA_CLUSTER_ID="$(/opt/kafka/bin/kafka-storage.sh random-uuid)" && \
/opt/kafka/bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c /app/technologies/kafka_p2p/server.properties \
&& /opt/kafka/bin/kafka-server-start.sh -daemon /app/technologies/kafka_p2p/server.properties &
until ss -ltn | grep -Eq ':9092[[:space:]]'; do
  sleep 2
done

if [ $# -eq 0 ]; then
  set -- INFO DEBUG STUDY ERROR
fi

./PublisherApp "$@"

# Keep container (and broker) alive for orchestrator to stop later.
# Default: keep alive (so consumers can finish); set KEEP_ALIVE_AFTER_PUBLISH=0 to exit immediately.
PUB_RC=$?
if [ "${KEEP_ALIVE_AFTER_PUBLISH:-1}" = "1" ]; then
  tail -f /dev/null
fi
exit "$PUB_RC"