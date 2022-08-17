import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copy data from S3 to staging table in Redshift.
    Execute SQL queries in copy_table_queries list.

    Parameter
    ---------
    cur: psycopg2 Cursor
        Python engine to execute SQL command
    conn: pssycopg2 Connection
        Connection engine
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables to data warehouse.
    Execute SQL query from insert_table_queries

    Parameter
    ---------
    cur: psycopg2 Cursor
        Python engine to execute SQL command
    conn: pssycopg2 Connection
        Connection engine
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn_str = "host={} dbname={} user={} password={} port={}"

    conn = psycopg2.connect(conn_str.format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
