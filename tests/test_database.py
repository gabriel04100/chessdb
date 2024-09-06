import pytest
from unittest.mock import patch, mock_open
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from src.database import connect, execute_script, create_player_views, insert_game, load_data, get_recent_playergames_query
import os

load_dotenv()
DB_NAME=os.getenv("DB_NAME"),
DB_USER=os.getenv("DB_USER"),
DB_PASSWORD=os.getenv("DB_PASSWORD"),
DB_HOST=os.getenv("DB_HOST"),
DB_PORT=os.getenv("DB_PORT")


### Tests pour la fonction `execute_script()`
@patch("psycopg2.connect")
def test_execute_script(mock_connect):
    """
    Teste que la fonction execute_script lit et exécute un script SQL correctement.
    """
    mock_conn = mock_connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    # Mock open pour simuler la lecture d'un fichier SQL
    with patch("builtins.open", mock_open(read_data="SELECT 1;")):
        execute_script("fake_path.sql")

    mock_cursor.execute.assert_called_once_with("SELECT 1;")
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


### Tests pour la fonction `insert_game()`
@patch("psycopg2.connect")
def test_insert_game(mock_connect):
    """
    Teste que la fonction insert_game insère correctement des données dans la table chess_games.
    """
    mock_conn = mock_connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    test_data = (
        'Event', 'Site', '2024-01-01', '1', 'WhitePlayer', 'BlackPlayer', '1-0', 
        2800, 2700, '90+30', '2024-01-01T18:00:00', 'Checkmate', 'e4 e5 Nf3 Nc6'
    )

    insert_game(test_data)

    query = """
    INSERT INTO chess_games (
        event, site, date, round, white_player, black_player, result, 
        white_elo, black_elo, time_control, end_time, termination, moves
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    mock_cursor.execute.assert_called_once_with(query, test_data)
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()

### Tests pour la fonction `load_data()`
@patch("psycopg2.connect")
@patch("pandas.read_sql_query")
def test_load_data(mock_read_sql, mock_connect):
    """
    Teste que la fonction load_data charge correctement les données dans un DataFrame.
    """
    mock_conn = mock_connect.return_value
    mock_df = pd.DataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]})
    mock_read_sql.return_value = mock_df

    query = "SELECT * FROM chess_games"
    df = load_data(query)

    mock_read_sql.assert_called_once_with(query, mock_conn)
    assert df.equals(mock_df)
    mock_conn.close.assert_called_once()

### Tests pour la fonction `get_recent_playergames_query()`
def test_get_recent_playergames_query():
    """
    Teste que la fonction get_recent_playergames_query génère correctement la requête SQL.
    """
    player_name = "test_player"
    expected_query = f"""
    SELECT * FROM chess_games
    WHERE date >= (CURRENT_DATE - INTERVAL '6 months')
    AND (white_player = '{player_name}' OR black_player = '{player_name}');
    """

    query = get_recent_playergames_query(player_name)
    assert query.strip() == expected_query.strip()
