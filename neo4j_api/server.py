import os
import re
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")
API_KEY = os.getenv("API_KEY")

# Connect to Neo4j Aura
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

app = FastAPI()

# Example schema (customize to your graph model)
schema = {
    "nodes": [
        {"label": "Vehicle", "properties": ["vin","make","model","year","trim","bodyStyle"]},
        {"label": "Fact", "properties": ["odometer","price","msrp","engineHP","mpgCity","mpgHwy"]},
        {"label": "Brand", "properties": ["name","country"]},
        {"label": "Dealer", "properties": ["name","region"]}
    ],
    "rels": [
        {"type": "HAS_FACT", "from": "Vehicle", "to": "Fact"},
        {"type": "MADE_BY", "from": "Vehicle", "to": "Brand"},
        {"type": "SOLD_BY", "from": "Vehicle", "to": "Dealer"}
    ]
}

# 🔐 Simple API-key check
def check_api_key(x_api_key: str):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.get("/schema")
async def get_schema(x_api_key: str = Header(None)):
    check_api_key(x_api_key)
    return schema

class CypherRequest(BaseModel):
    query: str
    params: dict = {}
    maxRows: int = 200

@app.post("/cypher")
async def run_cypher(req: CypherRequest, x_api_key: str = Header(None)):
    check_api_key(x_api_key)

    # Reject write operations
    forbidden = re.compile(r"\b(create|merge|delete|set|remove|load\s+csv)\b", re.IGNORECASE)
    if forbidden.search(req.query):
        raise HTTPException(status_code=400, detail="Only read queries allowed")

    # Add LIMIT if missing and not aggregate
    final_query = req.query
    if "limit" not in req.query.lower() and not re.search(r"\b(count|avg|min|max|sum)\s*\(", req.query, re.I):
        final_query = f"{req.query}\nLIMIT {min(req.maxRows, 200)}"

    rows = []
    with driver.session(database="neo4j") as session:
        result = session.run(final_query, req.params)
        for record in result:
            rows.append(record.data())

    return {"rows": rows, "summary": {"keys": result.keys(), "rowCount": len(rows)}}
