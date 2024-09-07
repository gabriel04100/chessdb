import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests
import time
import random
from typing import Tuple, Any, List, Dict
from io import StringIO
import chess.pgn
from src.utils import read_pgn_file
from src.pgn_parser import parse_pgn

# Load environment variables
load_dotenv()

# Database configuration
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# Email for Chess.com API header
EMAIL = os.getenv('CHESSCOM_EMAIL')

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


def get_games_for_month(username: str, year: str, month: str) -> List[str]:
    """
    Fetch the PGN data for games of a specific player for a given month.
    """
    url = f'https://api.chess.com/pub/player/{username}/games/{year}/{month}'
    headers = {'User-Agent': EMAIL}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json().get('games', [])
    elif response.status_code == 429:
        # Rate limit error, apply exponential backoff
        wait_time = 2 ** (attempt + 1) + random.uniform(0, 1)  # Random jitter
        logging.warning(f"Rate limited. Waiting for {wait_time:.2f} seconds before retrying...")
        time.sleep(wait_time)
        return get_games_for_month(username, year, month)  # Retry
    elif response.status_code == 403:
        raise Exception(f"Access forbidden for {username}. Status code: 403")
    else:
        raise Exception(f"Failed to fetch games for {username} in {year}/{month}. Status code: {response.status_code}")


def fetch_games_for_last_two_months(username: str) -> List[Tuple[Any, ...]]:
    """
    Fetch and parse games for the player from the last two months.
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=60)
    
    # Convert dates to the format needed for the Chess.com API
    end_date_str = end_date.strftime('%Y/%m')
    start_date_str = start_date.strftime('%Y/%m')

    all_games = []

    current_date = start_date
    while current_date <= end_date:
        year_month = current_date.strftime('%Y/%m')
        if start_date_str <= year_month <= end_date_str:
            games = get_games_for_month(username, current_date.strftime('%Y'), current_date.strftime('%m'))
            for game in games:
                pgn_data = game.get('pgn', '')
                all_games.extend(parse_pgn(pgn_data))
        current_date += timedelta(days=31)  # Move to the next month
    
    return all_games


def insert_game(data: Tuple[Any, ...]) -> None:
    """
    Insert a chess game record into the chess_games table.

    :param data: A tuple containing the game data to be inserted. The order of the elements should match the columns in the chess_games table.
    :type data: Tuple[Any, ...]
    :return: None
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
    username = os.getenv('PLAYER_NAME')
    if not username:
        logging.error("PLAYER_NAME environment variable not set.")
        return
    
    try:
        logging.info("Starting the script.")
        new_games = fetch_games_for_last_two_months(username)

        conn = connect()
        cur = conn.cursor()
        cur.execute("SELECT id FROM chess_games")
        existing_ids = set(row[0] for row in cur.fetchall())
        conn.close()

        for game_data in new_games:
            if game_data[0] not in existing_ids:
                insert_game(game_data)
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
