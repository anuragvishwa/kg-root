#!/usr/bin/env python3
"""
graph_updater.py – KGroot-style streaming graph builder

1) Consumes events from Kafka topic `events`
2) Builds an in-memory Failure-Propagation Graph (FPG) per fault window
3) Flushes FPGs to Graphiti/Neo4j as Episodic nodes + RELATES_TO edges
4) (Optional) dumps each finished FPG to JSONL under /data/fpgs
"""

from __future__ import annotations
import os, json, uuid, time, asyncio, argparse
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timezone

import networkx as nx
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from neo4j import GraphDatabase, basic_auth

from graphiti_core import Graphiti
try:
    # correct location in graphiti-core
    from graphiti_core.nodes import EpisodeType
except Exception:
    # safety fallback for odd versions – Graphiti accepts the string "text"
    class EpisodeType:
        text = "text"
from graphiti_core.llm_client.openai_client import OpenAIClient
from graphiti_core.embedder.openai import OpenAIEmbedder

from datetime import datetime, timezone

def coerce_ts(value) -> int:
    """Return epoch seconds from int/float/ms or ISO-8601 string."""
    if isinstance(value, (int, float)):
        v = float(value)
        if v > 1e12:  # milliseconds
            v /= 1000.0
        return int(v)
    if isinstance(value, str):
        s = value.strip()
        # numeric string?
        try:
            v = float(s)
            if v > 1e12:
                v /= 1000.0
            return int(v)
        except ValueError:
            pass
        # ISO 8601 (handle trailing Z)
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            return int(dt.timestamp())
        except Exception:
            pass
    raise ValueError(f"unsupported timestamp format: {value!r}")


# ──────────────────────────── constants ────────────────────────────
TOPIC       = os.getenv("EVENT_TOPIC", "events")
GROUP_ID    = os.getenv("GROUP_ID", "graph-updater")
GRAPH_NAME  = os.getenv("GRAPH_NAME", "live_fpg")

# Alg.1 hyperparams
MAX_ASSOCIATED  = int(os.getenv("MAX_ASSOCIATED", 8))      # M
CORR_THRESHOLD  = float(os.getenv("CORR_THRESHOLD", 0.2))  # θ

# window split: seconds of inactivity that closes a fault episode
INACTIVITY_GAP  = int(os.getenv("FAULT_GAP_SECS", 60))

_NEO4J_DRIVER = None
def get_neo4j_driver():
    global _NEO4J_DRIVER
    if _NEO4J_DRIVER is None:
        uri  = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        pwd  = os.getenv("NEO4J_PASSWORD", "neo4j")
        _NEO4J_DRIVER = GraphDatabase.driver(uri, auth=basic_auth(user, pwd))
    return _NEO4J_DRIVER
# ──────────────────────────── data classes ─────────────────────────
@dataclass
class Event:
    ts:      int
    eid:     str
    service: str
    e_type:  str
    attrs:   Dict[str, str]

@dataclass
class FPGBuilder:
    """Simplified Alg. 1: attach each event to up to M recent nodes with a heuristic score."""
    M: int = MAX_ASSOCIATED
    theta: float = CORR_THRESHOLD
    graph: nx.MultiDiGraph = field(default_factory=nx.MultiDiGraph)

    def add_event(self, ev: Event) -> None:
        if self.graph.number_of_nodes() == 0:
            self.graph.add_node(ev.eid, **ev.__dict__)
            return

        candidates: List[nx.MultiDiGraph] = self._candidate_graphs(ev)
        best = self._best_graph(candidates)
        if best is None:
            self.graph.add_node(ev.eid, **ev.__dict__)
            return
        self.graph = best

    def _candidate_graphs(self, ev: Event) -> List[nx.MultiDiGraph]:
        cand: List[nx.MultiDiGraph] = []
        last_nodes = list(sorted(self.graph.nodes(data=True), key=lambda n: n[1]["ts"]))[-self.M:]
        for src, data in last_nodes:
            r_type, score = self._classify_relation(data, ev)
            if score < self.theta:
                continue
            g = self.graph.copy()
            g.add_node(ev.eid, **ev.__dict__)
            g.add_edge(src, ev.eid, r=r_type, score=score)
            cand.append(g)
        return cand

    @staticmethod
    def _classify_relation(src: dict, tgt: Event) -> Tuple[str, float]:
        if src["service"] == tgt.service and src["ts"] <= tgt.ts:
            return "sequential", 1.0
        dt = max(1, tgt.ts - src["ts"])
        score = max(0.01, min(1.0, 1 / (dt / 1000 + 1)))
        return "causal", score

    @staticmethod
    def _best_graph(graphs: List[nx.MultiDiGraph]) -> Optional[nx.MultiDiGraph]:
        if not graphs:
            return None
        return max(graphs, key=lambda g: sum(ed["score"] for *_ , ed in g.edges(data=True)))

# ───────────────────────────── utils ───────────────────────────────
async def wait_for_neo4j(uri: str, user: str, pwd: str) -> None:
    while True:
        try:
            drv = GraphDatabase.driver(uri, auth=basic_auth(user, pwd))
            with drv.session() as s:
                s.run("RETURN 1").consume()
            print("[wait] ✅ Neo4j reachable", flush=True)
            return
        except Exception as exc:
            print(f"[wait] …waiting for Neo4j: {exc}", flush=True)
            await asyncio.sleep(2)

async def persist_fpg(g: nx.MultiDiGraph, graphiti: Graphiti, fault_id: str) -> None:
    """Write all nodes/edges in the FPG to Graphiti/Neo4j and dump a JSONL file."""
    if g.number_of_nodes() == 0:
        return

    for n, data in g.nodes(data=True):
        ref_dt = datetime.fromtimestamp(int(data["ts"]), tz=timezone.utc)
        await graphiti.add_episode(
            name               = n,
            episode_body       = json.dumps(data, indent=2),
            source             = EpisodeType.text,
            source_description = f"online-fpg:{fault_id}",
            reference_time     = ref_dt,
            group_id           = GRAPH_NAME,
        )
        
    driver = get_neo4j_driver()
    query = """
    MATCH (s:Episodic {name:$src,  group_id:$group}),
        (t:Episodic {name:$dst,  group_id:$group})
    MERGE (s)-[r:RELATES_TO {group_id:$group}]->(t)
    ON CREATE SET
    r.name       = $rtype,
    r.score      = $score,
    r.created_at = timestamp()
    RETURN id(r) AS rid
    """
    for src, dst, edata in g.edges(data=True):
        params = {
            "src": src,
            "dst": dst,
            "group": GRAPH_NAME,
            "rtype": edata.get("r", "causal"),
            "score": float(edata.get("score", 1.0)),
        }
        with driver.session() as s:
            s.run(query, params).consume()

    out_dir = os.getenv("FPG_DUMP_DIR", "/data/fpgs")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{fault_id}_{int(time.time())}.jsonl")
    with open(path, "w") as fp:
        for n, d in g.nodes(data=True):
            fp.write(json.dumps({"id": n, **d}) + "\n")
        for u, v, d in g.edges(data=True):
            fp.write(json.dumps({"src": u, "dst": v, **d}) + "\n")
    print(f"[flush] 💾 persisted FPG for fault={fault_id} with "
          f"{g.number_of_nodes()} nodes, {g.number_of_edges()} edges → {path}", flush=True)

# ────────────────────────── consumers ──────────────────────────────
async def consume_loop(graphiti: Graphiti, bootstrap: str) -> None:
    """Consume Kafka events, coerce timestamps (ISO/epoch), and build+flush FPGs."""
    # --- helper: robust timestamp coercion ---
    def coerce_ts(value) -> int:
        """Return epoch seconds from int/float/ms or ISO-8601 string."""
        if isinstance(value, (int, float)):
            v = float(value)
            if v > 1e12:  # milliseconds
                v /= 1000.0
            return int(v)
        if isinstance(value, str):
            s = value.strip()
            # numeric string?
            try:
                v = float(s)
                if v > 1e12:
                    v /= 1000.0
                return int(v)
            except ValueError:
                pass
            # ISO 8601 (handle trailing Z)
            try:
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                dt = datetime.fromisoformat(s)  # uses timezone-aware if provided
                return int(dt.timestamp())
            except Exception:
                pass
        raise ValueError(f"unsupported timestamp format: {value!r}")

    # --- wait for Kafka to be reachable ---
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[bootstrap],
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                value_deserializer=lambda m: json.loads(m.decode()),
            )
            print("[wait] ✅ Kafka reachable", flush=True)
            break
        except NoBrokersAvailable:
            print("[wait] …waiting for Kafka", flush=True)
            await asyncio.sleep(2)

    # per-fault window state: fault_id -> (builder, last_ts)
    builders: Dict[str, Tuple[FPGBuilder, int]] = {}

    # --- main loop ---
    for msg in consumer:
        try:
            ev_raw = msg.value or {}
            raw_ts = ev_raw.get("ts") or ev_raw.get("timestamp") or ev_raw.get("time")
            if raw_ts is None:
                print(f"[warn] skipping event with no timestamp field: {ev_raw}", flush=True)
                continue

            ts = coerce_ts(raw_ts)
            attrs = ev_raw.get("attributes") or {}
            ev = Event(
                ts=ts,
                eid=attrs.get("id") or str(uuid.uuid4()),
                service=ev_raw.get("service", "UNKNOWN"),
                e_type=ev_raw.get("event_type", "unknown"),
                attrs=attrs,
            )
            fault_id = ev_raw.get("fault_id", ev.service)

            builder, last_ts = builders.get(fault_id, (FPGBuilder(), 0))

            # close & flush the previous window if we've been idle too long
            if last_ts and (ev.ts - last_ts) > INACTIVITY_GAP and builder.graph.number_of_nodes() > 0:
                await persist_fpg(builder.graph, graphiti, fault_id)
                builders[fault_id] = (FPGBuilder(), 0)
                builder, last_ts = builders[fault_id]

            builder.add_event(ev)
            builders[fault_id] = (builder, ev.ts)

        except Exception as e:
            # never let a single bad message kill the consumer
            print(f"[warn] skipping bad message ({type(e).__name__}): {e} — payload={getattr(msg, 'value', None)}",
                  flush=True)
            continue

# ─────────────────────────── entry points ──────────────────────────
async def main() -> None:
    uri  = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd  = os.getenv("NEO4J_PASSWORD", "neo4j")

    await wait_for_neo4j(uri, user, pwd)

    graphiti = Graphiti(
        uri=uri,
        user=user,
        password=pwd,
        llm_client=OpenAIClient(),   # requires OPENAI_API_KEY
        embedder=OpenAIEmbedder(),
    )
    await graphiti.build_indices_and_constraints()
    print("[graph-updater] ✅ indices & constraints ensured", flush=True)

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    await consume_loop(graphiti, bootstrap)

async def run_test() -> None:
    """Create an in-memory FPG with three events and persist it."""
    uri  = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd  = os.getenv("NEO4J_PASSWORD", "neo4j")
    await wait_for_neo4j(uri, user, pwd)

    graphiti = Graphiti(
        uri=uri, user=user, password=pwd,
        llm_client=OpenAIClient(), embedder=OpenAIEmbedder()
    )

    builder = FPGBuilder()
    t0 = int(time.time())
    for i in range(3):
        builder.add_event(Event(
            ts=t0 + i,
            eid=f"selftest-{i}",
            service="SELFTEST",
            e_type="heartbeat",
            attrs={"seq": i}
        ))

    await persist_fpg(builder.graph, graphiti, "selftest")
    print("[test] ✅ synthetic FPG written; check Neo4j for group_id=live_fpg and name STARTS WITH 'selftest-'", flush=True)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true", help="run synthetic FPG persistence")
    args = ap.parse_args()
    asyncio.run(run_test() if args.test else main())
