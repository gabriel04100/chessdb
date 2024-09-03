import psycopg2
from typing import Tuple, Any
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
