import pandas as pd

def format_response(data: pd.DataFrame) -> str:
    """
    Formats the DataFrame returned from the DB into a user-friendly string for chat.
    """

    # Step 1: Check for empty DataFrame
    if data is None or data.empty:
        return "⚠️ No records found."

    # Step 2: Single row
    if len(data) == 1:
        # convert single row to readable string
        row = data.iloc[0]
        row_str = ", ".join([f"{col}: {row[col]}" for col in data.columns])
        return row_str

    # Step 3: Multiple rows → show top 5 rows in a table format
    elif len(data) <= 5:
        # Convert to string table
        return data.to_string(index=False)
    
    # Step 4: Many rows → show summary with count
    else:
        count = len(data)
        columns = ", ".join(data.columns)
        return f"⚠️ Found {count} records. Columns: {columns}. Showing top 5 rows:\n\n{data.head(5).to_string(index=False)}"
        