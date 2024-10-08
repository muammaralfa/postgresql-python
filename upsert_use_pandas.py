from sqlalchemy.dialects.postgresql import insert
from connection import Connection
import pandas as pd


class Func:
    """
        upsert bisa dilakukan ketika table sudah ada di db, dan syarat nya harus sudah memiliki primary keys
    """
    def __init__(self):
        self.conn = Connection()

    def insert_on_conflict_nothing(self, table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        insert_statement = insert(table.table).values(data)
        conflict_update = insert_statement.on_conflict_do_update(
            constraint=f"{table.table.name}_pkey",
            set_={column.key: column for column in insert_statement.excluded},
        )
        result = conn.execute(conflict_update)
        return result.rowcount

    def insert_table(self, df: pd.DataFrame, table_name: str, schema='public'):
        conn = self.conn.initialize_postgre_url()
        df.to_sql(
            table_name,
            schema=schema,
            con=conn,
            if_exists='append',
            index=False,
            method=self.insert_on_conflict_nothing
        )


if __name__ == '__main__':
    df = pd.DataFrame([{'id': 1, 'nama': "alfa", "hobi": "makan"}])

    func = Func()
    func.insert_table(df, "table name", "schema")