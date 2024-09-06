import os
from src.database import create_player_views

if __name__ == "__main__":
    # Get the player name from the environment variable
    player_name = os.getenv("PLAYER_NAME")
    if player_name:
        create_player_views(player_name)
    else:
        print("PLAYER_NAME is not set in the .env file.")
