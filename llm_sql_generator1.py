"""
⚡ Production SQL Generator for Incident Chatbot
Optimized for MySQL + Ollama local LLM
"""

from __future__ import annotations
import os
import re
from functools import lru_cache
from typing import Optional
import requests

# -------------------------
# CONFIG
# -------------------------

OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")
OLLAMA_URL = "http://localhost:11434/api/generate"
TABLE_NAME = "ticketdetails"
VALID_COLUMNS = ["IncidentID", "TicketStatus", "AssignPerson", "TicketSubmittedDateTime"]
STATUS_MAP = {
    "new incident": "New",
    "new case": "New",
    "pending": "New",
    "open": "New",
    "in progress": "In Progress",
    "assign to group": "Assign to Group"
}

# -------------------------
# OLLAMA CLIENT
# -------------------------

session = requests.Session()

def call_llm(prompt: str) -> str:
    try:
        r = session.post(OLLAMA_URL, json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
            "temperature": 0,
            "keep_alive": "10m"
        }, timeout=60)
        r.raise_for_status()
        return r.json().get("response", "").strip()
    except Exception:
        return ""

# -------------------------
# FAST INTENT ROUTING
# -------------------------

def fast_intent(question: str) -> Optional[str]:
    q = question.lower()
    if ("pending" in q or "belum tutup" in q) and any(x in q for x in ["how many","berapa","count","jumlah"]):
        return f"SELECT COUNT(*) AS total_pending FROM {TABLE_NAME} WHERE TicketStatus='Pending';"
    if any(x in q for x in ["open","belum tutup","masih buka"]):
        return f"SELECT IncidentID, TicketStatus, AssignPerson FROM {TABLE_NAME} WHERE TicketStatus IN ('Open','In Progress','Assign to Group') ORDER BY id DESC LIMIT 50;"
    if any(x in q for x in ["latest","terkini","baru"]):
        return f"SELECT IncidentID, TicketStatus, TicketSubmittedDateTime FROM {TABLE_NAME} ORDER BY id DESC LIMIT 50;"
    if any(x in q for x in ["new case", "new cases", "kes baru"]):
        return f"SELECT IncidentID, TicketStatus, TicketSubmittedDateTime FROM {TABLE_NAME} WHERE TicketStatus='New' ORDER BY id DESC LIMIT 50;"
    match = re.search(r'inc[\-\s]?(\d+)', q)
    if match:
        inc = match.group(1)
        return f"SELECT TicketStatus FROM {TABLE_NAME} WHERE IncidentID='{inc}';"
    return None

# -------------------------
# SQL CLEANING
# -------------------------

def clean_sql(raw: str) -> str:
    if not raw:
        return ""
    raw = re.sub(r"```(sql)?", "", raw, flags=re.IGNORECASE).strip()
    match = re.search(r"(SELECT[\s\S]+?;)", raw, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"(SELECT[\s\S]+)", raw, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""

# -------------------------
# SQL VALIDATION
# -------------------------

def valid_sql(sql: str) -> bool:
    if not sql:
        return False
    s = sql.upper().strip()
    if not s.startswith("SELECT"):
        return False
    forbidden = ["UPDATE","DELETE","INSERT","DROP","ALTER","TRUNCATE","--"]
    if any(x in s for x in forbidden):
        return False
    return True

# -------------------------
# SQL REPAIR ENGINE
# -------------------------

def repair_sql(sql: str) -> str:
    """
    Minimal safe repair:
    - Ensure correct table name
    - Fix only TicketStatus in WHERE clause
    - Fix status values
    - Ensure LIMIT 50 if missing
    - Ensure semicolon
    """
    if not sql:
        return sql

    # Ensure FROM correct table
    sql = re.sub(r"FROM\s+\w+", f"FROM {TABLE_NAME}", sql, flags=re.IGNORECASE)

    # Fix WHERE clause column names to TicketStatus
    sql = re.sub(r"WHERE\s+\w+", "WHERE TicketStatus", sql, flags=re.IGNORECASE)

    # Fix status values using STATUS_MAP
    for k, v in STATUS_MAP.items():
        sql = re.sub(rf"['\"]{k}['\"]", f"'{v}'", sql, flags=re.IGNORECASE)

    # Ensure LIMIT 50 if no COUNT and no LIMIT
    if "count(" not in sql.lower() and "limit" not in sql.lower():
        sql = sql.rstrip(";") + " LIMIT 50"

    # Ensure semicolon
    if not sql.strip().endswith(";"):
        sql += ";"

    return sql

# -------------------------
# PROMPT BUILDER
# -------------------------

def build_prompt(question: str) -> str:
    return f"""
You are a MySQL expert.
Return ONLY ONE valid SELECT query.
STRICT RULES:
- SELECT only
- No explanation
- No comments
- No markdown
- Use table {TABLE_NAME}
- Use LIMIT 50 unless COUNT

User Question:
{question}

SQL:
"""

# -------------------------
# MAIN GENERATOR
# -------------------------

@lru_cache(maxsize=200)
def generate_sql(question: str) -> Optional[str]:
    # Fast intent routing first
    fast = fast_intent(question)
    if fast:
        return fast.strip()

    # LLM generation
    prompt = build_prompt(question)
    raw = call_llm(prompt)
    if not raw:
        return None

    sql = clean_sql(raw)
    if not valid_sql(sql):
        return None

    sql = repair_sql(sql)
    return sql.strip()