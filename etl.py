import requests
import json
import pandas as pd
import psycopg2
import os



def extract_data():
    # API endpoint and parameters
    url = 'https://finnhub.io/api/v1/stock/insider-transactions'
    api_key = os.getenv('STOCK_API_KEY')

    params = {
        'symbol': '',  # stock symbol or empty for all
        'token': api_key  
    }

    # Get data from API
    response = requests.get(url, params=params)
    data = json.loads(response.text)

    return data



def transform_data(data):
    # Convert JSON data to Pandas dataframe
    df = pd.json_normalize(data)

    # # Drop unnecessary columns if they exist
    columns_to_drop = ['transactionShares', 'transactionPrice', 'transactionValue']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], axis=1)

    # Sort by index (transaction date) in descending order
    df = df.sort_index(ascending=False)

    return df



def load_data(df):
    # Connect to Postgres database
    conn = psycopg2.connect(
    host = os.getenv('DATABASE_HOST'), 
    database = os.getenv('DATABASE_DB'), 
    user = os.getenv('DATABASE_USER'), 
    password = os.getenv('DATABASE_PASSWORD'), 
    port = os.getenv('DATABASE_PORT')
    )

    cur = conn.cursor()

    # Create the table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS insider_transactions (
            transactionDate DATE,
            filingDate DATE,
            change NUMERIC,
            id TEXT,
            name TEXT,
            share NUMERIC,
            symbol TEXT,
            transactionCode VARCHAR(5),
            PRIMARY KEY (id, transactionDate)
        )
    """)
    conn.commit()

    # Insert data into the table
    for i, row in df.iterrows():
        cur.execute("""
            SELECT COUNT(*) FROM insider_transactions WHERE id=%s AND transactionDate=%s
        """, (row['id'], row['transactionDate']))
        count = cur.fetchone()[0]
        if count == 0:
            cur.execute("""
                INSERT INTO insider_transactions (
                    transactionDate, filingDate, change, id, name, share, symbol, transactionCode
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (row['transactionDate'], 
                  row['filingDate'], 
                  row['change'], 
                  row['id'], 
                  row['name'], 
                  row['share'], 
                  row['symbol'], 
                  row['transactionCode']))


    # Commit the changes and close the cursor and connection
    conn.commit()
    cur.close()
    conn.close()

 
 
if __name__ == '__main__':
    data = extract_data()
    df = transform_data(data["data"])
    load_data(df)