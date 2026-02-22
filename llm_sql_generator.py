import ollama
import re
from config import LLM_MODEL

SCHEMA = """
Table: ticketdetails

Columns:
- title (varchar)
- tat (int)
- incidentid (varchar)
- ticketstatus (varchar)
- ticketsubmitteddatetime (datetime)
- ticketcloseddatetime (datetime)
- reqname (varchar)
- reqemail (varchar)
- typeofticket (varchar)
- category (varchar)
- priorityseverity (varchar)
- product (varchar)
- reportedby (varchar)
- subcategory (varchar)
- subsubcategory (varchar)
- assignperson (varchar)
- assignpersonemail (varchar)
- assigngroup (varchar)
"""

def clean_sql(text: str) -> str:
    """
    Extract SQL from LLM response and clean formatting.
    """

    # remove markdown
    text = re.sub(r"```sql|```", "", text, flags=re.IGNORECASE).strip()

    # remove explanations before SELECT
    select_index = text.lower().find("select")
    if select_index != -1:
        text = text[select_index:]

    # remove trailing semicolons & spaces
    text = text.strip().rstrip(";")

    return text


def generate_sql(question: str) -> str | None:
    """
    - Generate SAFE SELECT SQL from user question.
    - Output MUST be a single SELECT statement.
    - Must include LIMIT 50 (unless aggregate).
    - MUST NOT modify data (INSERT/UPDATE/DELETE/DDL).
    - Return ONLY SQL (no prose).
    """
    prompt = f"""
You are an expert MySQL query generator.

Convert the user question into a SAFE SQL query.

IMPORTANT RULES:
- Output exactly one SQL statement.
- SELECT only. Do not modify data.
- Use LIMIT 50 for non-aggregations.
- Prefer IS NOT NULL over != '' when checking presence.
- Use DATE/DATETIME functions when the question uses ranges like "today", "yesterday", "last 7 days".
- RETURN ONLY SQL (no explanations).

Database Schema:
{SCHEMA}

Examples:

User: how many pending tickets
SQL:
SELECT COUNT(*) AS total
FROM ticketdetails
WHERE ticketstatus <> 'Closed';

User: show the new tickets
SQL:
SELECT *
FROM ticketdetails
WHERE ticketstatus LIKE '%New%'
LIMIT 50;

User: status of incident id INC-002
SQL:
SELECT ticketstatus
FROM ticketdetails
WHERE incidentid = 'INC-002'
LIMIT 50;

User: ticket submitted on 21/02/2026
SQL:
SELECT *
FROM ticketdetails
WHERE STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y') = STR_TO_DATE('21/02/2026', '%d/%m/%Y')
LIMIT 50;

User: berapa banyak tiket low
SQL:
SELECT COUNT(*) AS total
FROM ticketdetails
WHERE priorityseverity LIKE '%Low%';

User: senaraikan tiket di bawah kategori Infra
SQL:
SELECT *
FROM ticketdetails
WHERE category LIKE '%Infra%'
LIMIT 50;

User: tiket dibuka semalam
SQL:
SELECT *
FROM ticketdetails
WHERE STR_TO_DATE(ticketsubmitteddatetime, '%e/%c/%Y') = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
LIMIT 50;

User Question:
{question}

SQL:
"""

    try:
        response = ollama.chat(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0}
        )

        raw_output = response["message"]["content"]

        sql = clean_sql(raw_output)

        # safety validation
        if not sql.lower().startswith("select"):
            return None

        forbidden = ["update", "delete", "insert", "drop", "alter", "truncate"]
        if any(word in sql.lower() for word in forbidden):
            return None

        return sql

    except Exception as e:
        print("LLM SQL generation error:", e)
        return None