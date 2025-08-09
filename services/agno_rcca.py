import os, json, requests, openai
from neo4j import GraphDatabase

openai.api_key = os.getenv("OPENAI_API_KEY")
MATCHER  = os.getenv("MATCHER_ENDPOINT", "http://gcn-matcher:8000")

drv = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
)
with drv.session() as s:
    rec = s.run(
        "MATCH (e:Episode) RETURN e.service AS id ORDER BY e.ts DESC LIMIT 1"
    ).single()
    if not rec:
        print("⏳ No incidents yet; wait for events.")
        exit(0)
    incident = rec["id"]

matches = requests.post(
    f"{MATCHER}/match", json={"incident_id": incident, "k": 3}, timeout=10
).json()["top_k"]

prompt = f"""You are an SRE assistant.

Incident ID: {incident}
Matcher output:
{json.dumps(matches, indent=2)}

Write:
1) Probable root cause
2) Evidence
3) Step-by-step remediation
"""
reply = openai.ChatCompletion.create(
    model="o3-gpt",
    messages=[{"role": "user", "content": prompt}],
    temperature=0.2
).choices[0].message.content

print("\n=== RCA REPORT ===\n")
print(reply)
print("\n===================\n")
