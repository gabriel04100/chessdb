import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database configuration
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# Configure logging
logging.basicConfig(filename='insert_new_games.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect() -> psycopg2.extensions.connection:
    """
    Establish a connection to the PostgreSQL database.
    """
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def fetch_new_games() -> pd.DataFrame:
    """
    Fetch games from the last two months that are not already in the database.
    """
    conn = connect()
    two_months_ago = datetime.now() - timedelta(days=60)  # Approximate 2 months
    query = f"""
    WITH recent_games AS (
        SELECT * FROM chess_games
        WHERE date >= '{two_months_ago.strftime('%Y-%m-%d')}'
    )
    SELECT r.*
    FROM recent_games r
    LEFT JOIN chess_games c
    ON r.id = c.id
    WHERE c.id IS NULL
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def insert_game(data: tuple) -> None:
    """
    Insert a single game record into the chess_games table.
    """
    conn = connect()
    cur = conn.cursor()
    query = """
    INSERT INTO chess_games (
        event, site, date, round, white_player, black_player, result, 
        white_elo, black_elo, time_control, end_time, termination, moves
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cur.execute(query, data)
        conn.commit()
    except Exception as e:
        logging.error(f"Error inserting game: {e}")
    finally:
        cur.close()
        conn.close()

def main():
    """
    Main function to fetch and insert new games.
    """
    try:
        logging.info("Starting the script.")
        new_games = fetch_new_games()
        for index, row in new_games.iterrows():
            data = (
                row['event'], row['site'], row['date'], row['round'], 
                row['white_player'], row['black_player'], row['result'], 
                row['white_elo'], row['black_elo'], row['time_control'], 
                row['end_time'], row['termination'], row['moves']
            )
            insert_game(data)
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()


