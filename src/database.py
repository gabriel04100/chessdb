import psycopg2
from typing import Tuple, Any, Optional
from .config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def connect() -> psycopg2.extensions.connection:
    """
    Establish a connection to the PostgreSQL database.

    :return: A connection object to the PostgreSQL database.
    :rtype: psycopg2.extensions.connection
    """
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn


def execute_script(script_path: str) -> None:
    """
    Execute a SQL script from a file.

    :param script_path: The file path to the SQL script.
    :type script_path: str
    :return: None
    """
    conn = connect()
    cur = conn.cursor()
    with open(script_path, 'r') as f:
        cur.execute(f.read())
    conn.commit()
    cur.close()
    conn.close()


def create_player_views(player_name: Optional[str] = None) -> None:
    """
    Create SQL views for recent games,
    recent white games, and recent black games
    for a specified player or for all players.

    :param player_name: The name of the player to filter the views. If None, 
    views for all players are created.
    :type player_name: Optional[str]
    :return: None
    """
    # Path to the existing SQL script template
    script_path = 'recent_games.sql'
    # Read the script content
    with open(script_path, 'r') as file:
        script_content = file.read()

    # Replace placeholder with actual player name if provided
    if player_name:
        script_content = script_content.format(player_name=player_name)
    else:
        script_content = script_content.format(player_name="''")

    # Write the modified script to a temporary file
    temp_script_path = 'temp_recent_games.sql'
    with open(temp_script_path, 'w') as file:
        file.write(script_content)
    # Execute the modified script from the temporary file
    execute_script(temp_script_path)


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
    cur.execute(query, data)
    conn.commit()
    cur.close()
    conn.close()
