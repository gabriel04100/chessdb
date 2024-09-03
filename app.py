import streamlit as st
import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

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


# Charger les données depuis la base de données
def load_data(query: str) -> pd.DataFrame:
    conn = connect()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


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


# Définir les requêtes SQL avec le pseudonyme du joueur
def get_query(player_name: str) -> str:
    return f"""
    SELECT * FROM chess_games
    WHERE date >= (CURRENT_DATE - INTERVAL '6 months')
    AND (white_player = '{player_name}' OR black_player = '{player_name}');
    """


# Application Streamlit
def main():
    st.title("Analyse des Résultats d'Échecs")

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

    df['gabrielpizzo_result'] = df.apply(lambda row: determine_result(row, player_name), axis=1)
    df[['num_moves', 'first_three_moves']] = df['moves'].apply(parse_moves).apply(pd.Series)
    df['gabrielpizzo_color'] = df.apply(lambda row: 'White' if row['white_player'] == player_name else 'Black', axis=1)
  
    st.write(f"Affichage des parties pour: {player_name}")
  
    # Afficher les données
    st.write(df)
   
    # Visualisation des résultats
    st.subheader("Distribution des résultats")
  
    if not df.empty:
        # Histogramme des résultats
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.countplot(data=df, x='gabrielpizzo_result', ax=ax, palette="Set2")
        ax.set_title(f'Distribution des résultats pour {player_name}')
        plt.xticks(rotation=45)
        st.pyplot(fig)
      
        # Nombre de coups
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.histplot(df['num_moves'], bins=30, kde=True, ax=ax)
        ax.set_title('Distribution du nombre de coups')
        st.pyplot(fig)
        
        # Premier coup et trois premiers coups
        fig, ax = plt.subplots(figsize=(12, 6))
        sns.countplot(data=df, x='first_three_moves', ax=ax, palette="Set2")
        ax.set_title('Distribution des trois premiers coups')
        plt.xticks(rotation=90)
        st.pyplot(fig)
        
        # Pourcentage des résultats par les trois premiers coups
        st.subheader("Pourcentage des résultats par les trois premiers coups")
        
        # Filtrer pour inclure seulement les trois premiers coups les plus courants
        top_3_moves = df['first_three_moves'].value_counts().nlargest(3).index
        df_top_3 = df[df['first_three_moves'].isin(top_3_moves)]

        # Calculer les pourcentages
        df_top_3['count'] = df_top_3.groupby(['first_three_moves', 'gabrielpizzo_result'])['first_three_moves'].transform('count')
        df_top_3['percentage'] = df_top_3['count'] / df_top_3.groupby('first_three_moves')['first_three_moves'].transform('count') * 100

        # Préparer le graphique
        fig, ax = plt.subplots(figsize=(10, 10))
        sns.barplot(data=df_top_3, x="first_three_moves", y="percentage", hue="gabrielpizzo_result", palette="viridis", ax=ax)

        # Ajouter les pourcentages au-dessus des barres
        for p in ax.patches:
            height = p.get_height()
            ax.annotate(f'{height:.1f}%', (p.get_x() + p.get_width() / 2., height),
                        ha='center', va='bottom', fontsize=12)

        ax.set_title("Pourcentage des Résultats par les Trois Premiers Coups")
        ax.set_ylabel("Pourcentage (%)")
        ax.set_xlabel("Trois Premiers Coups")
        ax.grid()
        st.pyplot(fig)


if __name__ == "__main__":
    main()
