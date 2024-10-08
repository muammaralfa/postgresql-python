import pandas as pd
from connection import Connection

if __name__ == '__main__':
    df = pd.DataFrame([{'id': 1, 'nama': "alfa", "hobi": "makan"}])
    df.to_sql(
        "table name",
        con=Connection().initialize_postgre_url(),
        if_exists="append",
        index=False
    )