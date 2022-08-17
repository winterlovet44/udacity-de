import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop tables in AWS Redshift if it exists.
    Execute SQL query from drop_table_queries

    Parameter
    ---------
    cur: psycopg2 Cursor
        Python engine to execute SQL command
    conn: pssycopg2 Connection
        Connection engine
    """
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create tables in AWS Redshift if it exists.
    Execute SQL query from create_table_queries

    Parameter
    ---------
    cur: psycopg2 Cursor
        Python engine to execute SQL command
    conn: pssycopg2 Connection
        Connection engine
    """
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print(dict(config.items('CLUSTER')))
    conn_str = "host={} dbname={} user={} password={} port={}"

    conn = psycopg2.connect(conn_str.format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Done connect to AWS!!!")

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
