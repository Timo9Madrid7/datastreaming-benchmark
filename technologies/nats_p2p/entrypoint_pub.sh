#!/bin/sh
set -e

nats-server -a 0.0.0.0 -p 4222 -c /app/technologies/nats_p2p/nats.config -l /tmp/nats.log &
until ss -ltn | grep -Eq ':4222[[:space:]]'; do
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