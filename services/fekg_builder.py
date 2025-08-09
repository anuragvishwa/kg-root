#!/usr/bin/env python3
import os
import sys
import time
from datetime import datetime
from neo4j import GraphDatabase, basic_auth

# ── config ────────────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
CUT_MINUTES    = int(os.getenv("CUT_MINUTES", "5"))
# ── end config ────────────────────────────────────────────────────────────

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD)
)

# 1) wait for Neo4j to be reachable
while True:
    try:
        with driver.session() as s:
            s.run("RETURN 1")
        break
    except Exception:
        print("[fekg-builder] waiting for Neo4j…")
        time.sleep(2)

# 2) helper to see if there are any Episode nodes yet
def episode_label_exists(tx):
    row = tx.run(
        "CALL db.labels() YIELD label "
        "WHERE label = 'Episode' "
        "RETURN count(*) AS cnt"
    ).single()
    return row["cnt"] > 0

with driver.session() as s:
    if not s.execute_read(episode_label_exists):
        print("[fekg-builder] no Episode nodes yet; skipping fetch")
        sys.exit(0)

# 3) our actual fetch
def fetch_new(tx, cut_iso):
    return tx.run(
        """
        MATCH (e:Episode)
        WHERE e.ts > datetime($cut)
        RETURN
          e.service      AS svc,
          e.event_type   AS et,
          e.id           AS id,
          e.ts           AS ts
        ORDER BY e.ts
        """,
        cut=cut_iso
    ).data()

cut_time = datetime.utcnow().isoformat()
rows = driver.session().execute_read(lambda tx: fetch_new(tx, cut_time))

for row in rows:
    print(f"[fekg-builder] svc={row['svc']}  evt={row['et']}  id={row['id']}  ts={row['ts']}")
