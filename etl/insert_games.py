from src.utils import read_pgn_file
from src.pgn_parser import parse_pgn
from src.database import insert_game


def main():
    pgn_data = read_pgn_file('../data/games.pgn')
    games = parse_pgn(pgn_data)
    for game in games:
        insert_game(game)


if __name__ == '__main__':
    main()
