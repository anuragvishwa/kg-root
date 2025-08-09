#!/usr/bin/env python3
import os, time, uuid, json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from neo4j import GraphDatabase, basic_auth

def main():
    # 1) read the same env-vars your graph-updater uses
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    topic           = os.getenv("EVENTS_TOPIC",   "events")
    neo4j_uri       = os.getenv("NEO4J_URI",      "bolt://neo4j:7687")
    neo4j_user      = os.getenv("NEO4J_USER",     "neo4j")
    neo4j_password  = os.getenv("NEO4J_PASSWORD", "")
    wait_seconds    = int(os.getenv("WAIT_SECONDS", "5"))

    # 2) produce a unique test event
    test_id = f"container-test-{uuid.uuid4()}"
    event = {
        "service":    "TEST-SVC",
        "event_type": "heartbeat",
        "ts":         int(time.time()),
        "attributes": {"id": test_id, "foo": "bar"}
    }
    print(f"⏳ Sending event id={test_id} to Kafka…")
    producer = KafkaProducer(
        bootstrap_servers=[kafka_bootstrap],
        value_serializer=lambda v: json.dumps(v).encode()
    )
    try:
        md = producer.send(topic, event).get(timeout=10)
        print(f"✔ Sent to {md.topic}@{md.partition}:{md.offset}")
    except KafkaError as e:
        print("✗ Kafka send failed:", e)
        return
    producer.flush()

    # 3) wait for graph-updater to ingest
    print(f"⏳ Waiting {wait_seconds}s for graph-updater…")
    time.sleep(wait_seconds)

    # 4) query Neo4j for the new node
    print("⏳ Querying Neo4j…")
    driver = GraphDatabase.driver(
        neo4j_uri,
        auth=basic_auth(neo4j_user, neo4j_password)
    )
    with driver.session() as session:
        rec = session.run(
            'MATCH (e:Episodic {name:$name}) RETURN e.name AS name, e.created_at AS ts',
            name=test_id
        ).single()
        if rec:
            print("✅ Found in Neo4j:", rec["name"], "at", rec["ts"])
        else:
            print("✗ No node found — check graph-updater logs.")
    driver.close()

if __name__ == "__main__":
    main()
