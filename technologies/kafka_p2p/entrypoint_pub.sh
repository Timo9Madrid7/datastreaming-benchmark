#!/bin/sh
set -e

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)" && \
/opt/kafka/bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c /app/technologies/kafka_p2p/server.properties \
&& /opt/kafka/bin/kafka-server-start.sh /app/technologies/kafka_p2p/server.properties &
until ss -ltn | grep -Eq ':9092[[:space:]]'; do
  sleep 2
done

if [ $# -eq 0 ]; then
  set -- INFO DEBUG STUDY ERROR
fi

exec ./PublisherApp "$@"