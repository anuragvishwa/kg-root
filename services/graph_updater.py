#!/usr/bin/env python3
"""
graph_updater.py – KGroot-style streaming graph builder (Vector.dev edition)

This version expects Vector to output structured JSON records to Kafka.
It still mirrors the KGroot pipeline:
• Data preprocessing → structured events (Vector does the heavy lift) :contentReference[oaicite:6]{index=6}
• Causal relation discovery via SVM over pair features (causal vs sequential) :contentReference[oaicite:7]{index=7}
• Online FPG construction (Alg. 1: window M, threshold θ, keep best candidate) :contentReference[oaicite:8]{index=8}
• FEKG construction from historical FPGs (Alg. 2) :contentReference[oaicite:9]{index=9}
• Graph similarity (RGCN→MLP; WL fallback) to pick best FEKG :contentReference[oaicite:10]{index=10}

Env:
EVENT_TOPIC=events
GROUP_ID=graph-updater
GRAPH_NAME=live_fpg
FAULT_GAP_SECS=60
MAX_ASSOCIATED=8
CORR_THRESHOLD=0.2
NEO4J_URI=bolt://neo4j:7687  NEO4J_USER=neo4j  NEO4J_PASSWORD=neo4j
KAFKA_BOOTSTRAP=kafka:9092
MODELS_DIR=/data/models
FPG_DUMP_DIR=/data/fpgs
FEKG_DIR=/data/fekg
USE_BERT=0
"""

from __future__ import annotations
import os, re, json, uuid, time, asyncio, argparse, glob, random
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timezone

import numpy as np
import networkx as nx
from neo4j import GraphDatabase, basic_auth
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Optional deps guarded
import joblib
from sklearn.svm import SVC
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.feature_extraction.text import TfidfVectorizer

try:
    from gensim.models import Word2Vec
except Exception:
    Word2Vec = None

try:
    from sentence_transformers import SentenceTransformer
except Exception:
    SentenceTransformer = None

TORCH_OK = True
PYG_OK = True
try:
    import torch
    from torch import nn
except Exception:
    TORCH_OK = False
try:
    from torch_geometric.nn import RGCNConv
    from torch_geometric.data import Data
except Exception:
    PYG_OK = False

# ───────── config & paths ─────────
TOPIC       = os.getenv("EVENT_TOPIC", "events")
GROUP_ID    = os.getenv("GROUP_ID", "graph-updater")
GRAPH_NAME  = os.getenv("GRAPH_NAME", "live_fpg")

MAX_ASSOCIATED  = int(os.getenv("MAX_ASSOCIATED", "8"))      # Alg.1 M
CORR_THRESHOLD  = float(os.getenv("CORR_THRESHOLD", "0.2"))   # Alg.1 θ
INACTIVITY_GAP  = int(os.getenv("FAULT_GAP_SECS", "60"))

MODELS_DIR  = os.getenv("MODELS_DIR", "/data/models")
FPG_DIR     = os.getenv("FPG_DUMP_DIR", "/data/fpgs")
FEKG_DIR    = os.getenv("FEKG_DIR", "/data/fekg")
USE_BERT    = bool(int(os.getenv("USE_BERT", "0")))
BERT_NAME   = os.getenv("BERT_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(FPG_DIR, exist_ok=True)
os.makedirs(FEKG_DIR, exist_ok=True)

SVM_PATH       = os.path.join(MODELS_DIR, "relation_svm.joblib")
W2V_PATH       = os.path.join(MODELS_DIR, "events.w2v")
RGCN_PATH      = os.path.join(MODELS_DIR, "rgcn_mlp.pt")
WL_MLP_PATH    = os.path.join(MODELS_DIR, "wl_mlp.joblib")

# ───────── utils ─────────
def _to_jsonable(x):
    if isinstance(x, np.ndarray): return x.tolist()
    if isinstance(x, (np.floating, np.integer)): return x.item()
    if isinstance(x, dict): return {k: _to_jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)): return [_to_jsonable(v) for v in x]
    return x

def coerce_ts(value) -> int:
    if isinstance(value, (int, float)):
        v = float(value)
        if v > 1e12: v /= 1000.0
        return int(v)
    if isinstance(value, str):
        s = value.strip()
        try:
            v = float(s)
            if v > 1e12: v /= 1000.0
            return int(v)
        except ValueError:
            pass
        try:
            if s.endswith("Z"): s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            return int(dt.timestamp())
        except Exception:
            pass
    raise ValueError(f"unsupported timestamp: {value!r}")

def now_utc_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())

# ───────── Neo4j ─────────
_NEODRV = None
def get_neo():
    global _NEODRV
    if _NEODRV is None:
        uri  = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        pwd  = os.getenv("NEO4J_PASSWORD", "neo4j")
        _NEODRV = GraphDatabase.driver(uri, auth=basic_auth(user, pwd))
    return _NEODRV

async def wait_for_neo4j() -> None:
    uri  = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd  = os.getenv("NEO4J_PASSWORD", "neo4j")
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

# ───────── types ─────────
@dataclass
class Event:
    ts: int
    eid: str
    service: str
    e_type: str
    attrs: Dict[str, Any]
    abstract_type: str = ""
    emb: Optional[np.ndarray] = None

# ───────── Eventizer (Vector-first) ─────────
class Eventizer:
    """
    Vector-first eventizer: expects Vector to have normalized most fields.
    Supported paths (any may exist):
      timestamp: .timestamp | .ts
      service:   .service.name | .service | .kubernetes.pod_labels."app.kubernetes.io/name"
      event_type: .event.type | .event_type | derived from http.status_code
      attributes: whole record (we'll keep compact)
    Falls back to simple regex only if nothing else is set.
    """

    LOG_PATTERNS = [
        (re.compile(r"OOMKilled|OutOfMemory"), "pod.memory.oomkill"),
        (re.compile(r"CPU throttle|cpu throttled"), "pod.cpu.throttle"),
        (re.compile(r"ReadTimeout|connect timeout|timed out|deadline exceeded"), "net.timeout"),
        (re.compile(r"\bHTTP\s*5\d\d\b|\b5\d\d\b"), "http.5xx"),
        (re.compile(r"CrashLoopBackOff"), "k8s.pod.crashloop"),
    ]

    def _pick(self, rec: Dict[str, Any], *paths: str, default=None):
        """
        Supports dotted paths AND bracketed segments for keys that contain dots:
          "kubernetes.pod_labels.[app.kubernetes.io/name]"
        """
        def walk(o: Any, path: str):
            segs: List[str] = []
            buf, in_br = [], False
            for ch in path:
                if ch == '[':
                    if in_br: return None  # malformed
                    if buf:
                        segs.append(''.join(buf)); buf = []
                    in_br = True
                elif ch == ']':
                    if not in_br: return None
                    segs.append(''.join(buf)); buf = []
                    in_br = False
                elif ch == '.' and not in_br:
                    segs.append(''.join(buf)); buf = []
                else:
                    buf.append(ch)
            if buf: segs.append(''.join(buf))

            cur = o
            for k in segs:
                if isinstance(cur, dict) and k in cur:
                    cur = cur[k]
                else:
                    return None
            return cur

        for p in paths:
            v = walk(rec, p)
            if v is not None:
                return v
        return default

    def from_raw(self, rec: Dict[str, Any]) -> Optional[Event]:
        base = rec
        attrs_blob = rec.get("attributes") if isinstance(rec.get("attributes"), dict) else {}

        # --- timestamp ---
        ts = (self._pick(base, "timestamp", "ts")
              or self._pick(attrs_blob, "timestamp", "ts")
              or now_utc_ts())
        ts = coerce_ts(ts)

        # --- service ---
        service = (self._pick(base, "service.name", "service",
                              "kubernetes.container_name",
                              "kubernetes.pod_labels.[app.kubernetes.io/name]")
                   or self._pick(attrs_blob, "service.name", "service",
                                  "kubernetes.container_name",
                                  "kubernetes.pod_labels.[app.kubernetes.io/name]")
                   or "UNKNOWN")

        # --- event type ---
        e_type = (self._pick(base, "event.type", "event_type")
                  or self._pick(attrs_blob, "event.type", "event_type"))

        # derive http.* if needed
        if not e_type:
            status = (self._pick(base, "http.status_code")
                      or self._pick(attrs_blob, "http.status_code"))
            try:
                if status is not None:
                    sc = int(status)
                    if 500 <= sc < 600 or 400 <= sc < 500:
                        e_type = f"http.{sc}"
            except Exception:
                pass

        # keep full context (but trim big strings)
        attrs = dict(rec)
        for k in ["message", "log", "stacktrace", "body"]:
            if k in attrs and isinstance(attrs[k], str):
                attrs[k] = attrs[k][:500]

        # final fallback classification (same as your original)
        if not e_type:
            msg = rec.get("message") or rec.get("log")
            if msg:
                e_type = "log.other"
                for pat, lab in self.LOG_PATTERNS:
                    if pat.search(msg):
                        e_type = lab; break
            else:
                metric = self._pick(base, "metric.name", "metric") or self._pick(attrs_blob, "metric.name", "metric")
                value  = self._pick(base, "metric.value", "value") or self._pick(attrs_blob, "metric.value", "value")
                thresh = self._pick(base, "metric.threshold", "threshold") or self._pick(attrs_blob, "metric.threshold", "threshold")
                if metric is not None and value is not None and thresh is not None:
                    try:
                        if float(value) >= float(thresh):
                            e_type = f"metric.threshold.{metric}"
                    except Exception:
                        pass
                if not e_type:
                    return None

        eid = rec.get("id") or attrs.get("uuid") or str(uuid.uuid4())
        return Event(ts=ts, eid=str(eid), service=str(service), e_type=str(e_type), attrs=attrs)

# ───────── Abstraction (abstract types + embeddings) ─────────
class Abstraction:
    def __init__(self):
        self.w2v = Word2Vec.load(W2V_PATH) if (Word2Vec and os.path.exists(W2V_PATH)) else None
        self.bert = None
        if USE_BERT and SentenceTransformer:
            try: self.bert = SentenceTransformer(BERT_NAME)
            except Exception: self.bert = None

    @staticmethod
    def to_abstract_type(ev: Event) -> str:
        et = ev.e_type.lower()

        # Vector often provides http status numerically → normalize
        if et.startswith("http."):
            code = et.split(".", 1)[1]
            try:
                code_i = int(code)
                if 500 <= code_i < 600: return "http.error.5xx"
                if 400 <= code_i < 500: return "http.error.4xx"
            except Exception:
                pass
            return "http.error.5xx" if "5" in code else "http.error.4xx"

        if et.startswith("metric.threshold"):
            m = ev.attrs.get("metric") or ev.attrs.get("metric.name") or "metric"
            return f"thresh.{m}"

        if "crashloop" in et: return "k8s.crashloop"
        if "oom" in et or "memory" in et: return "mem.pressure"
        if "timeout" in et: return "svc.timeout" if ev.attrs.get("service") else "net.timeout"
        if "cpu" in et and "throttle" in et: return "cpu.throttle"
        if et == "log.other": return "log.other"

        return et.replace(" ", "_")

    @staticmethod
    def as_sentence(ev: Event) -> str:
        svc = ev.service
        et  = ev.e_type
        # Use a few stable keys for text embedding
        http = ev.attrs.get("http", {})
        code = http.get("status_code") or ev.attrs.get("http.status_code")
        k8s  = ev.attrs.get("kubernetes", {})
        pod  = k8s.get("pod_name") or k8s.get("container_name")
        return f"{svc} {et} http={code} pod={pod}"

    def embed(self, evs: List[Event]) -> None:
        for ev in evs:
            ev.abstract_type = self.to_abstract_type(ev)
        if self.bert:
            texts = [self.as_sentence(e) for e in evs]
            vecs = self.bert.encode(texts, convert_to_numpy=True, show_progress_bar=False)
            for e, v in zip(evs, vecs): e.emb = v.astype(np.float32)
            return
        if self.w2v:
            for e in evs:
                tok = e.abstract_type
                if tok in self.w2v.wv:
                    e.emb = self.w2v.wv[tok]
                else:
                    e.emb = np.random.RandomState(hash(tok) & 0xFFFF).randn(self.w2v.vector_size).astype(np.float32)
            return
        # TF-IDF fallback (lightweight)
        corpus = [self.as_sentence(e) for e in evs]
        vec = TfidfVectorizer(max_features=256)
        X = vec.fit_transform(corpus).toarray().astype(np.float32)
        for e, row in zip(evs, X): e.emb = row

# ───────── RelationClassifier (SVM) ─────────
class RelationClassifier:
    """
    Binary: causal (1) vs sequential (0).
    Features: Δt, same_service, abstract_type match, Jaccard(attrs), cosine(emb).
    Mirrors the paper’s SVM-based relation decision. :contentReference[oaicite:11]{index=11}
    """
    def __init__(self):
        self.model = joblib.load(SVM_PATH) if os.path.exists(SVM_PATH) else None

    @staticmethod
    def _pair_features(src: Event, tgt: Event) -> Dict[str, Any]:
        dt = max(0, tgt.ts - src.ts)
        same_service = 1 if src.service == tgt.service else 0
        type_match = 1 if src.abstract_type == tgt.abstract_type else 0
        set_a = set(f"{k}:{v}" for k,v in src.attrs.items())
        set_b = set(f"{k}:{v}" for k,v in tgt.attrs.items())
        jacc = len(set_a & set_b) / max(1, len(set_a | set_b))
        cos = 0.0
        if src.emb is not None and tgt.emb is not None:
            a = src.emb / (np.linalg.norm(src.emb) + 1e-9)
            b = tgt.emb / (np.linalg.norm(tgt.emb) + 1e-9)
            cos = float(np.dot(a, b))
        return {"dt": dt, "same_service": same_service, "type_match": type_match, "jacc": jacc, "cos": cos}

    def predict(self, src: Event, tgt: Event) -> Tuple[str, float]:
        if self.model is None:
            # heuristic fallback: close in time → more causal-like
            dt = max(1, tgt.ts - src.ts)
            score = max(0.01, min(1.0, 1.0 / (dt / 1000.0 + 1.0)))
            return ("causal" if src.service != "" else "sequential", float(score))
        f = self._pair_features(src, tgt)
        X = np.array([[f["dt"], f["same_service"], f["type_match"], f["jacc"], f["cos"]]], dtype=np.float32)
        y = self.model.predict(X)[0]
        try: p = self.model.predict_proba(X)[0,1]
        except Exception: p = 0.7 if y==1 else 0.3
        return ("causal" if y==1 else "sequential", float(p))

    @staticmethod
    def _collect_pairs(fpg_paths: List[str]) -> Tuple[np.ndarray, np.ndarray]:
        Xs, ys = [], []
        absr = Abstraction()
        for p in fpg_paths:
            nodes: Dict[str, Event] = {}
            edges = []
            with open(p, "r") as fp:
                for line in fp:
                    rec = json.loads(line)
                    if "src" in rec and "dst" in rec: edges.append(rec)
                    else:
                        ev = Event(ts=int(rec["ts"]), eid=rec["id"],
                                   service=rec.get("service","UNKNOWN"),
                                   e_type=rec.get("e_type","unknown"),
                                   attrs=rec.get("attrs",{}))
                        nodes[ev.eid] = ev
            if not nodes: continue
            evs = list(nodes.values())
            absr.embed(evs)
            for e in edges:
                u, v = e["src"], e["dst"]
                if u not in nodes or v not in nodes: continue
                f = RelationClassifier._pair_features(nodes[u], nodes[v])
                Xs.append([f["dt"], f["same_service"], f["type_match"], f["jacc"], f["cos"]])
                ys.append(1 if e.get("r","causal") == "causal" else 0)
        return np.array(Xs, np.float32), np.array(ys, np.int64)

    @staticmethod
    def train(fpg_dir: str = FPG_DIR, out_path: str = SVM_PATH) -> None:
        paths = sorted(glob.glob(os.path.join(fpg_dir, "*.jsonl")))
        if not paths: raise RuntimeError(f"No FPG dumps in {fpg_dir}")
        X, y = RelationClassifier._collect_pairs(paths)
        if len(X) < 10: raise RuntimeError("Too few labeled pairs (need ≥10).")
        pipe = Pipeline([("scaler", StandardScaler()),
                         ("svc", SVC(kernel="rbf", probability=True, class_weight="balanced"))])
        pipe.fit(X, y)
        joblib.dump(pipe, out_path)
        print(f"[train-svm] ✅ saved {out_path} with {len(X)} pairs")

# ───────── Online FPG (Algorithm 1) ─────────
@dataclass
class FPGBuilder:
    M: int = MAX_ASSOCIATED
    theta: float = CORR_THRESHOLD
    graph: nx.MultiDiGraph = field(default_factory=nx.MultiDiGraph)
    rc: RelationClassifier = field(default_factory=RelationClassifier)
    abstr: Abstraction = field(default_factory=Abstraction)

    def add_event(self, ev: Event) -> None:
        self.abstr.embed([ev])
        if self.graph.number_of_nodes() == 0:
            self.graph.add_node(ev.eid, **ev.__dict__); return

        # consider last M nodes by timestamp
        last_nodes = list(sorted(self.graph.nodes(data=True), key=lambda n: n[1]["ts"]))[-self.M:]

        # ensure embeddings on sources
        src_events: List[Event] = []
        for _, data in last_nodes:
            src_events.append(Event(ts=int(data["ts"]), eid=data["eid"], service=data["service"],
                                    e_type=data["e_type"], attrs=data["attrs"],
                                    abstract_type=data.get("abstract_type",""),
                                    emb=np.array(data["emb"]) if data.get("emb") is not None else None))
        self.abstr.embed(src_events)

        candidates: List[nx.MultiDiGraph] = []
        for (src_id, _), src_ev in zip(last_nodes, src_events):
            r_type, score = self.rc.predict(src_ev, ev)
            if score < self.theta: continue
            g = self.graph.copy()
            g.add_node(ev.eid, **ev.__dict__)
            g.add_edge(src_id, ev.eid, r=r_type, score=float(score))
            candidates.append(g)

        if not candidates:
            self.graph.add_node(ev.eid, **ev.__dict__)
        else:
            # choose graph with max total edge score
            def tot(g): return sum(ed.get("score",0.0) for *_ , ed in g.edges(data=True))
            self.graph = max(candidates, key=tot)

# ───────── Persist + dump ─────────
async def persist_fpg(g: nx.MultiDiGraph, fault_id: str,
                      rca_summary: Optional[Dict[str, Any]] = None) -> None:
    if g.number_of_nodes() == 0: return

    # Persist nodes/edges to Neo4j
    driver = get_neo()
    with driver.session() as s:
        for n, data in g.nodes(data=True):
            s.run("""
            MERGE (e:Episodic {name:$name, group_id:$group})
            ON CREATE SET e.created_at = timestamp()
            SET e.body = $body, e.ts = $ts
            """, {
                "name": n,
                "group": GRAPH_NAME,
                "body": json.dumps(_to_jsonable({**data, "_rca": rca_summary})),
                "ts": int(data["ts"])
            }).consume()
        for src, dst, ed in g.edges(data=True):
            s.run("""
            MATCH (s:Episodic {name:$src,group_id:$group}),
                  (t:Episodic {name:$dst,group_id:$group})
            MERGE (s)-[r:RELATES_TO {group_id:$group}]->(t)
            ON CREATE SET r.created_at = timestamp()
            SET r.name=$rtype, r.score=$score
            """, {
                "src": src, "dst": dst, "group": GRAPH_NAME,
                "rtype": ed.get("r","causal"), "score": float(ed.get("score",1.0))
            }).consume()

    # Dump JSONL for offline training/FEKG building
    path = os.path.join(FPG_DIR, f"{fault_id}_{int(time.time())}.jsonl")
    with open(path, "w") as fp:
        for n, d in g.nodes(data=True):
            rec = {"id": n, **{k: (v.tolist() if isinstance(v, np.ndarray) else v) for k, v in d.items()}}
            fp.write(json.dumps(rec) + "\n")
        for u, v, d in g.edges(data=True):
            fp.write(json.dumps({"src": u, "dst": v, **d}) + "\n")
    print(f"[flush] 💾 {path} (nodes={g.number_of_nodes()} edges={g.number_of_edges()})", flush=True)

# ───────── FEKG (Algorithm 2, simplified) ─────────
def load_fpg_dump(path: str) -> nx.MultiDiGraph:
    G = nx.MultiDiGraph()
    with open(path, "r") as fp:
        for line in fp:
            rec = json.loads(line)
            if "src" in rec and "dst" in rec:
                G.add_edge(rec["src"], rec["dst"], r=rec.get("r","causal"), score=float(rec.get("score",1.0)))
            else:
                G.add_node(rec["id"], **rec)
    return G

def fpg_signature(G: nx.MultiDiGraph) -> set:
    sig = set()
    for u, v, d in G.edges(data=True):
        au = G.nodes[u].get("abstract_type","")
        av = G.nodes[v].get("abstract_type","")
        r  = d.get("r","causal")
        if au and av: sig.add((au, r, av))
    return sig

def cluster_fpgs(paths: List[str], mu: float=0.5) -> List[List[str]]:
    clusters: List[List[str]] = [[p] for p in paths]
    sigs = {p: fpg_signature(load_fpg_dump(p)) for p in paths}

    def linkage(a: List[str], b: List[str]) -> float:
        A = set().union(*[sigs[p] for p in a])
        B = set().union(*[sigs[p] for p in b])
        if not A and not B: return 1.0
        return len(A & B) / max(1, len(A | B))

    changed = True
    while changed and len(clusters) > 1:
        changed = False
        best = (0.0, -1, -1)
        for i in range(len(clusters)):
            for j in range(i+1, len(clusters)):
                l = linkage(clusters[i], clusters[j])
                if l > best[0]: best = (l, i, j)
        if best[0] >= mu:
            _, i, j = best
            clusters = [c for k,c in enumerate(clusters) if k not in (i,j)] + [clusters[i] + clusters[j]]
            changed = True
    return clusters

def build_fekg_from_cluster(paths: List[str], out_path: str) -> None:
    graphs = [load_fpg_dump(p) for p in paths]
    if not graphs: return
    sigs = [fpg_signature(G) for G in graphs]
    common = set.intersection(*sigs) if sigs else set()
    FE = nx.MultiDiGraph()
    for (au, r, av) in common:
        FE.add_node(au, node_type="abstract")
        FE.add_node(av, node_type="abstract")
        FE.add_edge(au, av, r=r)
    data = {
        "nodes": list(FE.nodes(data=True)),
        "edges": [(u,v,d) for u,v,d in FE.edges(data=True)],
        "label": {"fault_type": "UNKNOWN", "root_cause_types": list({u for (u,_,_) in common})}
    }
    with open(out_path, "w") as fp: json.dump(data, fp, indent=2)
    print(f"[build-fekg] ✅ {out_path}  |E|={FE.number_of_edges()}")

def cli_build_fekg(mu: float=0.5):
    paths = sorted(glob.glob(os.path.join(FPG_DIR, "*.jsonl")))
    if not paths: raise RuntimeError(f"No FPG dumps in {FPG_DIR}")
    clusters = cluster_fpgs(paths, mu=mu)
    for idx, group in enumerate(clusters):
        out = os.path.join(FEKG_DIR, f"fekg_{idx}.json")
        build_fekg_from_cluster(group, out)

# ───────── Graph similarity (RGCN → MLP; WL fallback) ─────────
def pg_data_from_graph(G: nx.MultiDiGraph, rel_map: Dict[str,int]) -> Tuple[Any, Dict[str,int]]:
    nodes = list(G.nodes())
    X = []
    for n in nodes:
        nd = G.nodes[n]
        emb = nd.get("emb")
        if emb is None:
            vec = np.zeros(64, dtype=np.float32)
            vec[hash(n) % 64] = 1.0
            X.append(vec)
        else:
            v = np.array(emb, dtype=np.float32).ravel()
            X.append(v)
    in_dim = max(len(v) for v in X)
    X = np.stack([np.pad(v, (0, in_dim - len(v))) for v in X]).astype(np.float32)
    idx = {n:i for i,n in enumerate(nodes)}
    eidx, etype = [], []
    for u, v, d in G.edges(data=True):
        eidx.append([idx[u], idx[v]])
        etype.append(rel_map.setdefault(d.get("r","causal"), len(rel_map)))
    import torch
    x = torch.tensor(X)
    ei = torch.tensor(eidx, dtype=torch.long).t().contiguous() if eidx else torch.empty((2,0), dtype=torch.long)
    et = torch.tensor(etype, dtype=torch.long) if etype else torch.empty((0,), dtype=torch.long)
    return Data(x=x, edge_index=ei, edge_type=et), idx

class RGCNSimModel(nn.Module):
    def __init__(self, in_dim: int, num_rels: int, hid: int = 64, out: int = 64):
        super().__init__()
        self.c1 = RGCNConv(in_dim, hid, num_relations=num_rels)
        self.c2 = RGCNConv(hid, out, num_relations=num_rels)
        self.mlp = nn.Sequential(nn.Linear(out*2, 64), nn.ReLU(), nn.Linear(64, 2))
    def forward(self, g1: Data, g2: Data):
        import torch
        h1 = torch.relu(self.c2(torch.relu(self.c1(g1.x, g1.edge_index, g1.edge_type)), g1.edge_index, g1.edge_type))
        h2 = torch.relu(self.c2(torch.relu(self.c1(g2.x, g2.edge_index, g2.edge_type)), g2.edge_index, g2.edge_type))
        g1v = h1.max(dim=0).values
        g2v = h2.max(dim=0).values
        z = torch.cat([g1v, g2v], dim=-1).unsqueeze(0)
        return torch.softmax(self.mlp(z), dim=-1)

def wl_graph_embed(G: nx.MultiDiGraph, iters: int = 2) -> Dict[str,int]:
    labels = {n: G.nodes[n].get("abstract_type") or n for n in G.nodes()}
    for _ in range(iters):
        new = {}
        for n in G.nodes():
            neigh = sorted([(G.nodes[v].get("abstract_type") or v, d.get("r","r")) for _,v,d in G.out_edges(n, data=True)])
            sig = labels[n] + "|" + ";".join(f"{t}:{r}" for t,r in neigh)
            new[n] = str(hash(sig) % (10**9))
        labels = new
    from collections import Counter
    return Counter(labels.values())

def wl_vectorize(c: Dict[str,int], vocab: List[str]) -> np.ndarray:
    return np.array([c.get(tok, 0) for tok in vocab], dtype=np.float32)

def rgcn_similarity_score(online: nx.MultiDiGraph, fekg_path: str) -> float:
    with open(fekg_path, "r") as fp: data = json.load(fp)
    K = nx.MultiDiGraph()
    for n, nd in data["nodes"]: K.add_node(n, **nd)
    for u,v,ed in data["edges"]: K.add_edge(u,v, **ed)

    if TORCH_OK and PYG_OK and os.path.exists(RGCN_PATH):
        rel_map = {}
        g1, _ = pg_data_from_graph(online, rel_map)
        g2, _ = pg_data_from_graph(K, rel_map)
        model = RGCNSimModel(in_dim=g1.x.shape[1], num_rels=max(1, len(rel_map)))
        model.load_state_dict(torch.load(RGCN_PATH, map_location="cpu"))
        model.eval()
        with torch.no_grad():
            p = model(g1, g2)[0,1].item()
        return float(p)

    # WL fallback
    vocab_path = WL_MLP_PATH + ".vocab.json"
    if os.path.exists(WL_MLP_PATH) and os.path.exists(vocab_path):
        vocab = json.load(open(vocab_path))
        c1 = wl_graph_embed(online); x1 = wl_vectorize(c1, vocab)
        c2 = wl_graph_embed(K);      x2 = wl_vectorize(c2, vocab)
        X = np.concatenate([x1, x2])[None, :]
        mlp = joblib.load(WL_MLP_PATH)
        try: return float(mlp.predict_proba(X)[0,1])
        except Exception: return float(mlp.predict(X)[0])

    # Last resort: Jaccard over abstract-edge triples
    s1 = fpg_signature(online)
    s2 = set((u,ed.get("r","r"),v) for u,v,ed in K.edges(data=True))
    return len(s1 & s2) / max(1, len(s1 | s2))

# Root-cause ranking (Eq. 3)
def rca_rank(online: nx.MultiDiGraph, alarm_node: Optional[str]=None, Wt: float=0.6, Wd: float=0.4, topn: int=3):
    nodes = list(online.nodes())
    if not nodes: return []
    alarm_node = alarm_node or max(nodes, key=lambda n: online.nodes[n].get("ts",0))
    tss = {n: online.nodes[n].get("ts",0) for n in nodes}
    sorted_by_time = sorted(nodes, key=lambda n: tss[n])    # earlier better
    Nt = {n:i for i,n in enumerate(sorted_by_time, start=1)}
    from collections import deque
    # distance: shortest path in undirected view
    dists = {}
    UG = online.to_undirected()
    for n in nodes:
        try: d = nx.shortest_path_length(UG, source=n, target=alarm_node)
        except Exception: d = 10**6
        dists[n] = d
    sorted_by_dist = sorted(nodes, key=lambda n: dists[n])
    Nd = {n:i for i,n in enumerate(sorted_by_dist, start=1)}
    scores = [(n, Wt*(1.0/Nt[n]) + Wd*(1.0/Nd[n])) for n in nodes]
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores[:topn]

# ───────── Kafka consume loop ─────────
async def consume_loop(bootstrap: str, do_rca: bool=False) -> None:
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

    eventizer = Eventizer()
    builders: Dict[str, Tuple[FPGBuilder, int]] = {}

    for msg in consumer:
        try:
            raw = msg.value or {}
            ev = eventizer.from_raw(raw)
            if not ev: continue

            fault_id = raw.get("fault_id") or ev.service  # window by service unless a fault_id is present
            builder, last_ts = builders.get(fault_id, (FPGBuilder(), 0))

            # Close a window on inactivity and flush the graph
            if last_ts and (ev.ts - last_ts) > INACTIVITY_GAP and builder.graph.number_of_nodes() > 0:
                rca_summary = None
                if do_rca:
                    fekgs = sorted(glob.glob(os.path.join(FEKG_DIR, "fekg_*.json")))
                    if fekgs:
                        scores = [(p, rgcn_similarity_score(builder.graph, p)) for p in fekgs]
                        scores.sort(key=lambda x: x[1], reverse=True)
                        best_path, best_p = scores[0]
                        top_events = rca_rank(builder.graph)
                        rca_summary = {"best_fekg": os.path.basename(best_path), "similarity": best_p, "top_events": top_events}
                await persist_fpg(builder.graph, fault_id, rca_summary=rca_summary)
                builders[fault_id] = (FPGBuilder(), 0)
                builder, last_ts = builders[fault_id]

            builder.add_event(ev)
            builders[fault_id] = (builder, ev.ts)

        except Exception as e:
            print(f"[warn] skipping message: {type(e).__name__}: {e}", flush=True)
            continue

# ───────── CLIs: embeddings, FEKG, RGCN/WL, SVM, eval ─────────
def cli_build_embeddings():
    if Word2Vec is None:
        print("[build-embeddings] gensim not installed; skipping Word2Vec"); return
    paths = sorted(glob.glob(os.path.join(FPG_DIR, "*.jsonl")))
    seqs = []
    for p in paths:
        nodes = []
        with open(p, "r") as fp:
            for line in fp:
                rec = json.loads(line)
                if "src" not in rec and "dst" not in rec:
                    nodes.append(rec)
        nodes.sort(key=lambda r: r.get("ts",0))
        seqs.append([Abstraction.to_abstract_type(Event(
            ts=int(r["ts"]), eid=r["id"], service=r.get("service",""), e_type=r.get("e_type",""), attrs=r.get("attrs",{})
        )) for r in nodes])
    if not seqs: raise RuntimeError(f"No FPG dumps in {FPG_DIR}")
    # Window=100 to reflect paper’s wide context window for events. :contentReference[oaicite:12]{index=12}
    w2v = Word2Vec(seqs, vector_size=64, window=100, min_count=1, workers=2, sg=1)
    w2v.save(W2V_PATH)
    print(f"[build-embeddings] ✅ {W2V_PATH}")

def cli_train_rgcn():
    if not (TORCH_OK and PYG_OK):
        print("[train-rgcn] torch/pyg not available; use WL fallback."); return cli_train_wl()
    fekgs = sorted(glob.glob(os.path.join(FEKG_DIR, "fekg_*.json")))
    fpgs  = sorted(glob.glob(os.path.join(FPG_DIR, "*.jsonl")))
    if not fekgs or not fpgs: raise RuntimeError("Need FEKGs and FPG dumps.")
    # Simple synthetic positives/negatives for training (replace with curated pairs for best results)
    rel_map = {}
    pairs = []
    for fp in fpgs:
        G = load_fpg_dump(fp)
        pos = random.choice(fekgs)
        pairs.append((G, pos, 1))
        neg = random.choice([p for p in fekgs if p != pos]) if len(fekgs) > 1 else pos
        pairs.append((G, neg, 0))

    datas = []
    for G, path, y in pairs:
        d1, _ = pg_data_from_graph(G, rel_map)
        with open(path, "r") as fp:
            dat = json.load(fp)
        K = nx.MultiDiGraph()
        for n, nd in dat["nodes"]: K.add_node(n, **nd)
        for u,v,ed in dat["edges"]: K.add_edge(u,v, **ed)
        d2, _ = pg_data_from_graph(K, rel_map)
        datas.append((d1, d2, y))
    in_dim = datas[0][0].x.shape[1]
    model = RGCNSimModel(in_dim, num_rels=max(1,len(rel_map)))
    opt = torch.optim.Adam(model.parameters(), lr=1e-3)
    loss_fn = nn.CrossEntropyLoss()
    for epoch in range(5):
        tot = 0.0
        for g1, g2, y in datas:
            opt.zero_grad()
            pred = model(g1, g2)
            yv = torch.tensor([y], dtype=torch.long)
            loss = loss_fn(pred, yv)
            loss.backward(); opt.step()
            tot += loss.item()
        print(f"[train-rgcn] epoch {epoch+1} loss={tot/len(datas):.4f}")
    torch.save(model.state_dict(), RGCN_PATH)
    print(f"[train-rgcn] ✅ {RGCN_PATH}")

def cli_train_wl():
    fekgs = sorted(glob.glob(os.path.join(FEKG_DIR, "fekg_*.json")))
    fpgs  = sorted(glob.glob(os.path.join(FPG_DIR, "*.jsonl")))
    if not fekgs or not fpgs: raise RuntimeError("Need FEKGs and FPG dumps.")
    vocab = set()
    X, y = [], []
    for fp in fpgs:
        G = load_fpg_dump(fp)
        c1 = wl_graph_embed(G)
        # Positive/mismatch pairs (rough)
        pos = random.choice(fekgs)
        with open(pos,"r") as bf:
            dat = json.load(bf)
        K = nx.MultiDiGraph()
        for n, nd in dat["nodes"]: K.add_node(n, **nd)
        for u,v,ed in dat["edges"]: K.add_edge(u,v, **ed)
        c2 = wl_graph_embed(K)
        vocab.update(c1.keys()); vocab.update(c2.keys())
        X.append((c1,c2)); y.append(1)
        neg = random.choice([p for p in fekgs if p != pos]) if len(fekgs) > 1 else pos
        with open(neg,"r") as nf:
            dat2 = json.load(nf)
        K2 = nx.MultiDiGraph()
        for n, nd in dat2["nodes"]: K2.add_node(n, **nd)
        for u,v,ed in dat2["edges"]: K2.add_edge(u,v, **ed)
        c3 = wl_graph_embed(K2)
        vocab.update(c3.keys())
        X.append((c1,c3)); y.append(0)
    vocab = sorted(vocab)
    feats = [np.concatenate([wl_vectorize(cA, vocab), wl_vectorize(cB, vocab)], axis=0) for cA,cB in X]
    from sklearn.neural_network import MLPClassifier
    clf = MLPClassifier(hidden_layer_sizes=(64,), max_iter=200)
    clf.fit(np.array(feats), np.array(y))
    joblib.dump(clf, WL_MLP_PATH)
    json.dump(vocab, open(WL_MLP_PATH + ".vocab.json","w"))
    print(f"[train-wl] ✅ {WL_MLP_PATH}")

def cli_eval(k_list: List[int] = [1,2,3,5]):
    fekgs = sorted(glob.glob(os.path.join(FEKG_DIR, "fekg_*.json")))
    fpgs  = sorted(glob.glob(os.path.join(FPG_DIR, "*.jsonl")))
    if not fekgs or not fpgs: raise RuntimeError("Need FEKGs and FPG dumps.")
    import time as _t
    ranks, total_ms = [], []
    for fp in fpgs:
        G = load_fpg_dump(fp)
        t0 = _t.time()
        scores = [(p, rgcn_similarity_score(G, p)) for p in fekgs]
        scores.sort(key=lambda x: x[1], reverse=True)
        total_ms.append((_t.time()-t0)*1000.0)
        # if FEKG filenames encode fault types, you can compute true ranks here
        ranks.append(1)  # placeholder
    a_at_k = {k: np.mean([1.0 if r<=k else 0.0 for r in ranks]) for k in k_list}
    mar = float(np.mean(ranks)) if ranks else float("nan")
    for k,v in a_at_k.items(): print(f"[eval] A@{k}={v:.2%}")
    print(f"[eval] MAR={mar:.2f}  Inference(ms) avg={np.mean(total_ms):.1f}±{np.std(total_ms):.1f}")

# ───────── main ─────────
async def main(do_rca: bool=False) -> None:
    await wait_for_neo4j()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    await consume_loop(bootstrap, do_rca=do_rca)

async def run_test() -> None:
    await wait_for_neo4j()
    builder = FPGBuilder()
    t0 = int(time.time())
    for i, et in enumerate(["heartbeat","cpu.throttle","svc.timeout","http.503","k8s.pod.crashloop"]):
        builder.add_event(Event(ts=t0 + i*2, eid=f"selftest-{i}", service="SELFTEST", e_type=et, attrs={"seq": i}))
    await persist_fpg(builder.graph, "selftest")
    print("[test] ✅ wrote selftest FPG", flush=True)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")
    ap.add_argument("--test", action="store_true")
    ap.add_argument("--rca", action="store_true")

    s1 = sub.add_parser("train-svm");          s1.add_argument("--fpg-dir", default=FPG_DIR)
    s2 = sub.add_parser("build-embeddings");   s2.add_argument("--fpg-dir", default=FPG_DIR)
    s3 = sub.add_parser("build-fekg");         s3.add_argument("--mu", type=float, default=0.5)
    s4 = sub.add_parser("train-rgcn")
    s5 = sub.add_parser("eval")

    args = ap.parse_args()
    if args.cmd == "train-svm":
        RelationClassifier.train(fpg_dir=args.fpg_dir, out_path=SVM_PATH)
    elif args.cmd == "build-embeddings":
        cli_build_embeddings()
    elif args.cmd == "build-fekg":
        cli_build_fekg(mu=args.mu)
    elif args.cmd == "train-rgcn":
        cli_train_rgcn()
    elif args.cmd == "eval":
        cli_eval()
    else:
        asyncio.run(run_test() if args.test else main(do_rca=args.rca))
