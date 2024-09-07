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
    Fetch games from the last month that are not already in the database.
    """
    conn = connect()
    query = """
    WITH recent_games AS (
        SELECT * FROM chess_games
        WHERE date >= (CURRENT_DATE - INTERVAL '1 month')
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
import sys
import os
import random
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests
from typing import Tuple, Any, List
from src.database import insert_game, connect 
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get email address from environment variables
EMAIL = os.getenv('CHESSCOM_EMAIL')


def get_game_archives(username: str, max_retries: int = 5) -> List[str]:
    """
    Fetch the list of available game archives for a Chess.com player, with retries and rate-limiting handling.
    
    :param username: Chess.com username of the player
    :type username: str
    :param max_retries: Maximum number of retries in case of failure (default is 5)
    :type max_retries: int
    :return: List of archive URLs
    :rtype: list
    """
    url = f"https://api.chess.com/pub/player/{username}/games/archives"
    headers = {'User-Agent': EMAIL}

    for attempt in range(max_retries):
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()["archives"]
        elif response.status_code == 429:
            # Rate limit error, apply exponential backoff
            wait_time = 2 ** attempt + random.uniform(0, 1)  # Random jitter
            print(f"Rate limited. Waiting for {wait_time:.2f} seconds before retrying...")
            time.sleep(wait_time)
        elif response.status_code == 403:
            raise Exception(f"Access forbidden for {username}. Status code: 403")
        else:
            raise Exception(f"Failed to fetch archives for {username}. Status code: {response.status_code}")

    raise Exception(f"Max retries reached. Failed to fetch archives for {username}")



def get_games_for_month(username: str, year: str, month: str):
    """
    Fetch the games for a specific player for a given month.
    """
    url = f'https://api.chess.com/pub/player/{username}/games/{year}/{month}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['games']
    else:
        raise Exception(f"Failed to fetch games for {username} in {year}/{month}. Status code: {response.status_code}")


def prepare_game_data(game: dict) -> Tuple[Any, ...]:
    """
    Prepare a single game tuple to be inserted into the database.
    """
    return (
        game.get('tournament', 'N/A'),  # Event
        game.get('url', 'N/A'),  # Site
        time.strftime('%Y-%m-%d', time.gmtime(game['end_time'])),  # Date (converted from UNIX timestamp)
        '1',  # Round (use '1' as placeholder)
        game['white']['username'],  # White player
        game['black']['username'],  # Black player
        game['result'],  # Result
        game['white']['rating'],  # White Elo
        game['black']['rating'],  # Black Elo
        game.get('time_control', 'N/A'),  # Time control
        time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(game['end_time'])),  # End time
        game.get('termination', 'N/A'),  # Termination
        game.get('pgn', 'N/A')  # PGN moves
    )


def fetch_and_insert_games(username: str):
    """
    Fetch games for a player and insert them into the database.
    """
    # Get the game archives (months where games exist)
    archives = get_game_archives(username)
    
    # For each archive, fetch games and insert them
    for archive in archives:
        year, month = archive.split('/')[-2:]
        games = get_games_for_month(username, year, month)

        for game in games:
            game_data = prepare_game_data(game)
            insert_game(game_data)


if __name__ == '__main__':
    username = str(os.getenv('PLAYER_NAME'))
    fetch_and_insert_games(username)

