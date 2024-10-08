import pandas as pd
import psycopg2
from connection import Connection


file = "source/file.xlsx"
df = pd.read_excel(file)

province_name = df['province_name'].tolist()
city_name = df['city_name'].tolist()
district_name = df['district_name'].tolist()
subdistrict_name = df['subdistrict_name'].tolist()
constituency_name_2024_list = df['Constituency_2024'].tolist()


if __name__ == "__main__":
    try:
        conn = Connection().connect()

        for num, province in enumerate(province_name):
            cur = conn.cursor()
            city = city_name[num]
            district = district_name[num]
            subdistrict = subdistrict_name[num]
            constituency_name_2024 = constituency_name_2024_list[num]
            dbname = "table name"
            query = f"""
                UPDATE {dbname} SET constituency_name_2024=%(constituency_name_2024)s
                WHERE province_name=%(province_name)s AND city_name=%(city_name)s AND district_name=%(district_name)s
                AND subdistrict_name=%(subdistrict_name)s
            """
            params = {
                "constituency_name_2024": constituency_name_2024,
                "province_name": province,
                "city_name": city,
                "district_name": district,
                "subdistrict_name": subdistrict
            }
            cur.execute(query, params)
            conn.commit()
            print(city, "updated")

    except Exception as e:
        print(e)