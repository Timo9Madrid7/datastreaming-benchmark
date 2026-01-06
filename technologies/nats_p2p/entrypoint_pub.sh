#!/bin/bash
set -e
nats-server --daemon


until ss -ltn | grep -q ":4222 "; do
sleep 2
done

if [ $# -eq 0 ]; then
  set -- INFO DEBUG STUDY ERROR
fi

exec ./PublisherApp "$@"
