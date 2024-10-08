import hashlib
import numpy as np
import psycopg2
import pandas as pd
from connection import Connection


file = "source/file.xlsx"
df = pd.read_excel(file)
df = df.replace({np.nan: None})

province_name = df['province_name'].tolist()
city_name = df['city_name'].tolist()
district_name = df['district_name'].tolist()
subdistrict_name = df['subdistrict_name'].tolist()
constituency_name = df['constituency_name'].tolist()
party_number = df['party_number'].tolist()
party = df['party'].tolist()
candidate_name = df['candidate_name'].tolist()
candidate_number = df['candidate_number'].tolist()
vote = df['vote'].tolist()
time = df['time'].tolist()


if __name__ == "__main__":
    try:
        conn = Connection().connect()
        for num, province in enumerate(province_name):
            cur = conn.cursor()
            table_target = "table name"
            query = f"""INSERT INTO {table_target} (province_name, city_name, district_name, subdistrict_name, 
            constituency_name, party_number, party, candidate_name, candidate_number, vote, time) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

            params = (
                province,
                city_name[num],
                district_name[num],
                subdistrict_name[num],
                constituency_name[num],
                party_number[num],
                party[num],
                candidate_name[num],
                candidate_number[num],
                vote[num],
                time[num]
            )

            print(params)
            cur.execute(query, params)
            conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

