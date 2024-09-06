import chess
import chess.svg
import base64


# Afficher l'échiquier sous forme d'image SVG
def render_chessboard(board: chess.Board) -> str:
    """chess board to SVG and encode it as base64 to display in Streamlit."""
    svg = chess.svg.board(board=board)
    b64_svg = base64.b64encode(svg.encode('utf-8')).decode('utf-8')
    return f"data:image/svg+xml;base64,{b64_svg}"


def parse_moves(moves):
    move_list = moves.split()
    num_moves = len(move_list)
    first_n_moves = ' '.join(move_list[:3]) if num_moves >= 3 else ''
    return num_moves, first_n_moves


# Column to indicate if the player won or drew the game
def determine_result(row, player_name):
    if row['white_player'] == player_name and row['result'] == "1-0":
        return "Win"
    elif row['black_player'] == player_name and row['result'] == "0-1":
        return "Win"
    elif row['result'] == "1/2-1/2":
        return "Draw"
    else:
        return "Loss"
