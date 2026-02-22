from llm_sql_generator import generate_sql
from sql_guard import validate_sql
from db import run_query
from formatter import format_response

def chatbot(question):
    sql = generate_sql(question)

    if not sql:
        return "⚠️ No SQL generated."

    if not validate_sql(sql):
        return f"⚠️ Invalid query generated.\n\nSQL:\n{sql}"

    try:
        data = run_query(sql)
    except Exception as e:
        return f"❌ Database error while running SQL:\n{e}\n\nSQL:\n{sql}"

    return format_response(data)
