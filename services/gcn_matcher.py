# services/gcn_matcher.py
import os, networkx as nx, torch, torch.nn.functional as F
from fastapi import FastAPI
from pydantic import BaseModel
from neo4j import GraphDatabase
from torch_geometric.nn import RGCNConv

driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
)
app = FastAPI()
rgcn = RGCNConv(32, 32, num_relations=2)

class Req(BaseModel):
    incident_id: str  # we’re actually using 'service' here
    k: int = 3

def to_graph(rows):
    G = nx.MultiDiGraph()
    for r in rows:
        G.add_edge(r["s"], r["o"], key=r["rel"])
    return G

def graph_tensors(G):
    nodes = {n:i for i,n in enumerate(G.nodes())}
    rels  = {"CAUSAL":0}
    edge_index, edge_type = [], []
    for u,v,k in G.edges(keys=True):
        edge_index.append([nodes[u], nodes[v]])
        edge_type.append(rels.get(k,0))
    return torch.tensor(edge_index).T, torch.tensor(edge_type), len(nodes)

@app.post("/match")
def match(req: Req):
    # 1) fetch the FPG for this service
    fpg_rows = driver.session().run(
        """
        MATCH (e1:Episode {service:$svc})-[r:CAUSAL]->(e2)
        RETURN e1.event_type AS s, r.score AS rel, e2.event_type AS o
        """,
        svc=req.incident_id
    ).data()

    # 2) fetch all FEKGs
    fekg_rows = driver.session().run(
        "MATCH (f:FEKG) RETURN f.fault_type AS ft, f.triples AS t"
    ).data()

    fpg = to_graph(fpg_rows)
    f_e_idx, f_e_type, f_n = graph_tensors(fpg)
    f_emb = rgcn(None, f_e_idx, f_e_type, num_nodes=f_n).mean(0)

    scores = []
    for row in fekg_rows:
        g = to_graph(row["t"])
        g_idx, g_type, g_n = graph_tensors(g)
        g_emb = rgcn(None, g_idx, g_type, num_nodes=g_n).mean(0)
        sim = float(F.cosine_similarity(f_emb, g_emb, dim=0))
        scores.append((row["ft"], sim))

    top = sorted(scores, key=lambda x: x[1], reverse=True)[:req.k]
    return {"top_k":[{"fekg":ft,"score":round(sc,3)} for ft,sc in top]}
