import chess.pgn
from io import StringIO


def parse_pgn(pgn_data):
    pgn_io = StringIO(pgn_data)
    games = []
    while True:
        game = chess.pgn.read_game(pgn_io)
        if game is None:
            break
        headers = game.headers
        moves = " ".join([str(move) for move in game.mainline_moves()])
        game_data = (
            headers.get("Event"),
            headers.get("Site"),
            headers.get("Date"),
            headers.get("Round"),
            headers.get("White"),
            headers.get("Black"),
            headers.get("Result"),
            headers.get("WhiteElo"),
            headers.get("BlackElo"),
            headers.get("TimeControl"),
            headers.get("EndTime"),
            headers.get("Termination"),
            moves
        )
        games.append(game_data)
    return games


def parse_moves(moves):
    move_list = moves.split()
    num_moves = len(move_list)
    first_5_moves = ' '.join(move_list[:5]) if num_moves >= 5 else ''
    return num_moves, first_5_moves
