import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):   
    """
        Description: Drops each table using the queries in `drop_table_queries` list.
        
        Arguments: 
           cur: the cursor object
           conn: connection to the DB
    
        Returns:
            None
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
        Description: Creates each table using the queries in `create_table_queries` list.
        
        Arguments: 
           cur: the cursor object
           conn: connection to the DB
    
        Returns:
            None
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def main():
    
    """
    Description: 
        - Drops (if exists) and Creates the sparkify database. 
    
        - Establishes connection with the sparkify database and gets
          cursor to it.  
    
        - Drops all the tables.  
    
        - Creates all tables needed. 
    
        - Finally, closes the connection.
    
    Arguments: 
       None
    
    Returns:
       None
        
    """
    
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)


    conn.close()


if __name__ == "__main__":
    main()