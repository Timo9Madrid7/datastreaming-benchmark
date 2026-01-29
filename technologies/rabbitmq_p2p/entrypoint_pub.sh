#!/bin/sh
set -e

# Start RabbitMQ broker inside the publisher container, then run PublisherApp.
# This mirrors the kafka_p2p/nats_p2p pattern: broker in background, app in foreground,
# then keep the container alive unless KEEP_ALIVE_AFTER_PUBLISH=0.

RABBITMQ_CONFIG_FILE="/etc/rabbitmq/rabbitmq.conf"

# Ensure RabbitMQ listens on all interfaces and allows guest from other containers.
# (By default, RabbitMQ restricts the 'guest' user to localhost only.)
mkdir -p /etc/rabbitmq
if [ ! -f "$RABBITMQ_CONFIG_FILE" ]; then
  cat > "$RABBITMQ_CONFIG_FILE" <<'EOF'
listeners.tcp.default = 5672
loopback_users.guest = false
vm_memory_high_watermark.absolute = 4GB
EOF
fi

# Start RabbitMQ in the background.
rabbitmq-server -detached

# Wait until the broker is ready.
until rabbitmq-diagnostics -q ping >/dev/null 2>&1; do
  sleep 0.5
done

# Also wait until AMQP port is listening (helps avoid early connect race).
until ss -ltn | grep -Eq ':5672[[:space:]]'; do
  sleep 0.2
done

# Default log levels if none provided.
if [ $# -eq 0 ]; then
  set -- INFO DEBUG STUDY ERROR
fi

./PublisherApp "$@"

# Keep container (and broker) alive for orchestrator to stop later.
PUB_RC=$?
if [ "${KEEP_ALIVE_AFTER_PUBLISH:-1}" = "1" ]; then
  tail -f /dev/null
fi
exit "$PUB_RC"
