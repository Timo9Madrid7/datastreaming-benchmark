#!/bin/sh
set -e

nats-server -a 0.0.0.0 -p 4222 &
until ss -ltn | grep -q ":4222 "; do
  sleep 2
done

if [ $# -eq 0 ]; then
  set -- INFO DEBUG STUDY ERROR
fi

exec ./PublisherApp "$@"