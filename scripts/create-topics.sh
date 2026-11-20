#!/usr/bin/env bash

BROKER="kafka1:9092"

create() {
  local topic="$1"
  local partitions="$2"
  local extra="$3"

  echo "Creating topic: $topic ..."
  docker exec kafka1 kafka-topics \
    --bootstrap-server "$BROKER" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 2 \
    $extra
}

create price.ticker 6 "--config cleanup.policy=compact"
create offer.inbound 12
create policy.config 1 "--config cleanup.policy=compact"
create offer.scored 12
create offer.accepted 12
create offer.rejected 12
create order.created 6
