import psycopg2
from config import load_config

def connect(config):
    try:
        with psycopg2.connect(**config) as conn:
            print("Successfully connected to the Postgres server...")
            return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

if __name__ == '__main__':
    config = load_config()
    conn = connect(config)
    cur = conn.cursor()
    print("Connection successful...")

    #Executing queries
    cur.execute(''' CREATE TABLE IF NOT EXISTS source_table (
    sale_id SERIAL PRIMARY KEY,
    gamer_name VARCHAR(50),
    game_title VARCHAR(100),
    purchase_date DATE,
    price DECIMAL(10,2)
    );
                ''')
    #print("Successfully created source table....")

    cur.execute(''' CREATE TABLE IF NOT EXISTS target_table (
    sale_id SERIAL PRIMARY KEY,
    gamer_name VARCHAR(50),
    game_title VARCHAR(100),
    price DECIMAL(10,2)
    ); 
                ''')
    
    cur.execute(''' INSERT INTO source_table (gamer_name, game_title, purchase_date, price) VALUES 
    ('Aaron Stark', 'Final Fantasy XVI', '2023-01-15', 59.99),
    ('Sophia Turner', 'God of War: Ragnarok', '2023-01-18', 69.99),
    ('Michael White', 'The Last of Us Part III', '2021-12-25', 49.99),
    ('Linda Green', 'Ratchet & Clank: Rift Apart', '2023-01-19', 59.99);
                ''')
    
    #cur.execute(''' SELECT * FROM source_table''')
    #print(cur.fetchall())

    cur.execute('''SELECT * FROM source_table WHERE purchase_date > '2023-01-01';''')

    #Inserting the data extraced from source table to target table
    cur.execute('''INSERT INTO target_table (gamer_name, game_title, price)
                SELECT gamer_name, game_title, price
                FROM source_table 
                WHERE purchase_date > '2023-01-01' AND price > 60;''')
    cur.execute('''SELECT * FROM target_table;''')
    print(cur.fetchone())

    


