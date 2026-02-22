import pandas as pd

def format_response(data: pd.DataFrame) -> str:
    """
    Formats the DataFrame returned from the DB into a user-friendly string for chat.
    """

    # Step 1: Check for empty DataFrame
    if data is None or data.empty:
        return "⚠️ No records found."

    # Step 2: Single row → display in one line
    if len(data) == 1:
        #row = data.iloc[0]
        #row_str = " | ".join([f"**{col}**: {row[col]}" for col in data.columns])
        #return f"✅ Record found:\n{row_str}"
        count = len(data)
        columns = ", ".join(data.columns)
        table_head = data.head(5).to_string(index=False)
        return (
            f"⚠️ Found {count} records.\n"# Columns: {columns}\n"
            f"```\n{table_head}\n```"
        )

    # Step 3: Multiple rows (<=5) → display as table
    elif len(data) <= 5:
        #table_str = data.to_string(index=False)
        #return f"📋 Found {len(data)} records:\n```\n{table_str}\n```"
        count = len(data)
        columns = ", ".join(data.columns)
        table_head = data.head(5).to_string(index=False)
        return (
            f"⚠️ Found {count} records.\n"# Columns: {columns}\n"
            f"```\n{table_head}\n```"
        )

    # Step 4: Many rows (>5) → show summary + top 5
    else:
        count = len(data)
        columns = ", ".join(data.columns)
        table_head = data.head(5).to_string(index=False)
        return (
            f"⚠️ Found {count} records.\n"# Columns: {columns}\n"
            f"Showing top 5 records:\n```\n{table_head}\n```"
        )