import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import os
import requests
import time
import random

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

def fetch_new_games(username: str) -> pd.DataFrame:
    """
    Fetch games from Chess.com API for the last two months.
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=60)
    
    # Convert dates to the format needed for the Chess.com API
    end_date_str = end_date.strftime('%Y/%m')
    start_date_str = start_date.strftime('%Y/%m')

    # Get archives for the player
    def get_game_archives(username: str) -> list:
        url = f"https://api.chess.com/pub/player/{username}/games/archives"
        headers = {'User-Agent': EMAIL}
        
        for attempt in range(5):  # Retry logic
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()["archives"]
            elif response.status_code == 429:
                wait_time = 2 ** attempt + random.uniform(0, 1)
                time.sleep(wait_time)
            else:
                raise Exception(f"Failed to fetch archives: {response.status_code}")
        raise Exception("Max retries reached")

    def get_games_for_month(username: str, year: str, month: str) -> list:
        url = f"https://api.chess.com/pub/player/{username}/games/{year}/{month}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json().get('games', [])
        else:
            raise Exception(f"Failed to fetch games: {response.status_code}")

    # Fetch game archives
    archives = get_game_archives(username)
    games = []

    for archive in archives:
        year, month = archive.split('/')[-2:]
        if (year + '/' + month) >= start_date_str and (year + '/' + month) <= end_date_str:
            games.extend(get_games_for_month(username, year, month))

    # Convert games to DataFrame
    game_records = []
    for game in games:
        game_records.append({
            'event': game.get('tournament', 'N/A'),
            'site': game.get('url', 'N/A'),
            'date': datetime.utcfromtimestamp(game['end_time']).strftime('%Y-%m-%d'),
            'round': '1',  # Placeholder
            'white_player': game['white']['username'],
            'black_player': game['black']['username'],
            'result': game['result'],
            'white_elo': game['white']['rating'],
            'black_elo': game['black']['rating'],
            'time_control': game.get('time_control', 'N/A'),
            'end_time': datetime.utcfromtimestamp(game['end_time']).strftime('%Y-%m-%d %H:%M:%S'),
            'termination': game.get('termination', 'N/A'),
            'moves': game.get('pgn', 'N/A')
        })

    df = pd.DataFrame(game_records)
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
    username = os.getenv('PLAYER_NAME')
    if not username:
        logging.error("PLAYER_NAME environment variable not set.")
        return
    
    try:
        logging.info("Starting the script.")
        new_games = fetch_new_games(username)
        conn = connect()
        cur = conn.cursor()
        cur.execute("SELECT id FROM chess_games")
        existing_ids = set(row[0] for row in cur.fetchall())
        conn.close()

        for index, row in new_games.iterrows():
            if row['id'] not in existing_ids:
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
