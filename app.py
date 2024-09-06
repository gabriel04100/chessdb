import streamlit as st
import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os
import chess
import chess.svg
from src.chessfunc import render_chessboard, parse_moves, determine_result
from src.database import get_query, load_data
from src.visuals import *

# Charger les variables d'environnement
load_dotenv()


# Connexion à la base de données
def connect():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )


# Application Streamlit
def main():
    st.title("Analyse des Résultats d'Échecs au cours des 6 derniers mois")

    # Saisie du pseudonyme du joueur
    player_name = st.text_input("Entrez le pseudonyme du joueur:", value="gabrielpizzo") 
    if not player_name:
        st.warning("Veuillez entrer un pseudonyme de joueur.")
        return

    # Charger les données
    query = get_query(player_name)
    df = load_data(query) 
    if df.empty:
        st.warning(f"Aucune donnée trouvée pour le joueur {player_name}.")
        return

    df['player_result'] = df.apply(lambda row: determine_result(row, player_name), axis=1)
    df[['num_moves', 'first_three_moves']] = df['moves'].apply(parse_moves).apply(pd.Series)
    df['player_color'] = df.apply(lambda row: 'White' if row['white_player'] == player_name else 'Black', axis=1)
    st.write(f"Affichage des parties pour: {player_name}")
    # Afficher les données
    st.write(df)
    # Sélectionner une partie pour afficher l'échiquier
    st.subheader("Visualiser une partie")
    game_index = st.selectbox("Sélectionnez une partie", df.index)

    if game_index is not None:
        selected_game = df.loc[game_index]
        st.write(f"Partie sélectionnée: {selected_game['white_player']} vs {selected_game['black_player']}")  
        # Initialiser le board et les mouvements
        moves = selected_game['moves'].split()
        board = chess.Board() 
        move_num = st.slider("Déplacez-vous dans les coups de la partie", 0, len(moves), 0)   
        # Appliquer les mouvements jusqu'à la position actuelle
        for move in moves[:move_num]:
            board.push(chess.Move.from_uci(move))

        # Afficher l'échiquier
        st.image(render_chessboard(board), use_column_width=True) 
    # Visualisation des résultats
    st.subheader("Distribution des résultats")
    if not df.empty:
        # Histogramme des résultats
        fig = histogram_player_results(player_name,df)
        st.pyplot(fig)  
        # Nombre de coups
        fig = mooves_number_hist(df)
        st.pyplot(fig)      
        # Premier coup et trois premiers coups
        fig= first_three_mooves_count(df)
        st.pyplot(fig)   
        # Pourcentage des résultats par les trois premiers coups
        st.subheader("Pourcentage des résultats par les trois premiers coups") 
        fig = most_frequent_first_mooves(df)
        st.pyplot(fig)


if __name__ == "__main__":
    main()
