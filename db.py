from sqlalchemy import create_engine, text
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME
import pandas as pd

engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
)

#def run_query(sql):
 #   with engine.connect() as conn:
  #      result = conn.execute(text(sql))
   #     return [dict(row) for row in result]
    
def run_query(query):
    with engine.connect() as conn:
        print(query)
        df = pd.read_sql(query, conn)
    return df