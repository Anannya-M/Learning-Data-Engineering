import mysql.connector
import pandas as pd
from datetime import datetime
from decimal import Decimal

'''
IN THIS I AM ATTEMPTING TO EXTRACT A TABLE (source_table) FROM THE MySQL SERVER AND CONVERT IT INTO
A PANDAS DATAFRAME  
'''

# CONNECTING TO THE MySQL SERVER
conn = mysql.connector.connect(user = 'root', password = 'root')
if (conn.is_connected()):
    print("Connection successful....")
else:
    print("Connection failed....")
curr = conn.cursor()

curr.execute('''CREATE DATABASE IF NOT EXISTS practice;''')
curr.execute("USE practice")
curr.execute(''' CREATE TABLE IF NOT EXISTS source_table (
    sale_id SERIAL PRIMARY KEY,
    gamer_name VARCHAR(50),
    game_title VARCHAR(100),
    purchase_date DATE,
    price DECIMAL(10,2)
    );
                ''')

curr.execute(''' INSERT INTO source_table (gamer_name, game_title, purchase_date, price) VALUES 
    ('Aaron Stark', 'Final Fantasy XVI', '2023-01-15', 59.99),
    ('Sophia Turner', 'God of War: Ragnarok', '2023-01-18', 69.99),
    ('Michael White', 'The Last of Us Part III', '2021-12-25', 49.99),
    ('Linda Green', 'Ratchet & Clank: Rift Apart', '2023-01-19', 59.99);
                ''')
curr.execute("SELECT * FROM source_table")
# print(curr.fetchall())

#NOW EXTRACTING THE DATA FROM MYSQL TABLE AND APPENDING IT IN A DATAFRAME
df = pd.DataFrame(columns=['sale_id','gamer_name','game_title','purchase_date','price'])
for row in curr.fetchall():
    
    df.loc[len(df)] = list(row)

# print(df)

conn.commit()
conn.close()

# NOW MAKING SOME TRANSFORMATIONS

df['purchase_date'] = pd.to_datetime(df['purchase_date'])
filter_date = pd.Timestamp('2022-01-23')
#GIVING A DISCOUNT OF 5% TO THE PRICE OF GAMES BEFORE 2022 AND A DISCOUNT OF 2% AFTER 2022
df['price'] = df.apply(lambda row: row['price'] - Decimal('0.5') * row['price'] if row['purchase_date'] < filter_date 
                       else row['price'] - Decimal(0.2) * row['price'], axis=1)

#print(df)

