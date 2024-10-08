from sqlalchemy import URL, create_engine
import psycopg2


class Connection:
    def __init__(self):
        pass

    def initialize_postgre_url(self):
        """
        create connection use an sqlalchemy for pandas
        :return:
        """
        url_object = URL.create(
            "postgresql",
            host=host,
            port=port,
            username=username,
            password=password,
            database=db,
        )
        return create_engine(url_object).connect()

    def connect(self):
        """
        create connection use psycopg2
        :return:
        """
        conn = psycopg2.connect(
            host="host",
            port="port",
            database="db name",
            user="postgres",
            password="password"
        )
