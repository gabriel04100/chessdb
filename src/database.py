import psycopg2
from .config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def connect():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn


def execute_script(script_path):
    conn = connect()
    cur = conn.cursor()
    with open(script_path, 'r') as f:
        cur.execute(f.read())
    conn.commit()
    cur.close()
    conn.close()


def insert_game(data):
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
