import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Copy data to staging tables.
    Takes as input connection to DB in Redshift
    and cursor. 
    Executes queries looping through a list
    of those.    
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Inserts data into tables.
    Takes as input connection to DB in Redshift
    and cursor. 
    Executes queries looping through a list
    of those.    
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Loading staging tables...')
    load_staging_tables(cur, conn)
    
    print('Inserting data in dimension and fact tables...')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()