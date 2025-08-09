#!/usr/bin/env bash
set -euo pipefail

# Adjust these if you’ve overridden them in your env or compose file
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-kafka:9092}
TOPIC=${EVENTS_TOPIC:-events}
NEO4J_USER=${NEO4J_USER:-neo4j}
NEO4J_PASSWORD=${NEO4J_PASSWORD:-s3cr3t123}  # or export this in your shell
SLEEP_SECONDS=10

# 1) Generate a unique ID
TEST_ID="smoke-$(date +%s)"

# 2) Produce a test event into Kafka
echo "⏳ Producing message with id=$TEST_ID to Kafka…"
docker compose exec kafka bash -c \
  "echo '{\"service\":\"TEST-SVC\",\"event_type\":\"heartbeat\",\"ts\":$(date +%s),\"attributes\":{\"id\":\"$TEST_ID\",\"foo\":\"bar\"}}' \
   | kafka-console-producer.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${TOPIC}"

# 3) Give graph-updater time to pick it up
echo "⏳ Waiting ${SLEEP_SECONDS}s for graph-updater to ingest…"
sleep ${SLEEP_SECONDS}

# 4) Tail the graph-updater logs looking for our TEST_ID
echo "📖 Checking graph-updater logs for ingestion…"
docker compose logs graph-updater --since "${SLEEP_SECONDS}s" | grep "$TEST_ID" || echo "⚠️  No ingestion log found."

# 5) Query Neo4j for the new node
echo "🔍 Querying Neo4j for the node…"
docker compose exec neo4j cypher-shell -u "${NEO4J_USER}" -p "${NEO4J_PASSWORD}" <<EOF
MATCH (e:Episodic {name:'$TEST_ID'})
RETURN 
  e.name        AS name, 
  e.event_type  AS event_type, 
  e.created_at  AS created_at 
LIMIT 1;
EOF
