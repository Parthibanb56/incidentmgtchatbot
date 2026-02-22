FORBIDDEN = ["delete","update","drop","insert","alter"]

def validate_sql(sql):
    s = sql.lower()

    if not s.strip().startswith("select"):
        return False

    for word in FORBIDDEN:
        if word in s:
            return False

    return True