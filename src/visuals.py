import matplotlib.pyplot as plt
import seaborn as sns


def histogram_player_results(player_name, df):
    # Histogramme des résultats
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.countplot(data=df, x='player_result', ax=ax)
    ax.set_title(f'Distribution des résultats pour {player_name}')
    plt.xticks(rotation=45)
    plt.grid()
    return fig


def mooves_number_hist(df):
    # Nombre de coups
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.histplot(df['num_moves'], bins=30, ax=ax)
    ax.set_title('Distribution du nombre de coups')
    plt.grid()
    return fig


def first_three_mooves_count(df):
    # Premier coup et trois premiers coups
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.countplot(data=df, x='first_three_moves', ax=ax)
    ax.set_title('Distribution des trois premiers coups')
    plt.xticks(rotation=90)
    plt.grid()
    return fig


def most_frequent_first_mooves(df):
    # Filtrer pour inclure seulement les trois premiers coups les plus courants
    top_3_moves = df['first_three_moves'].value_counts().nlargest(3).index
    df_top_3 = df[df['first_three_moves'].isin(top_3_moves)]

    # Calculer les pourcentages
    df_top_3['count'] = df_top_3.groupby(['first_three_moves', 'player_result'])['first_three_moves'].transform('count')
    count = df_top_3.groupby('first_three_moves')['first_three_moves'].transform('count') * 100
    df_top_3['percentage'] = df_top_3['count'] / count

    # Préparer le graphique
    fig, ax = plt.subplots(figsize=(10, 10))
    sns.barplot(data=df_top_3, x="first_three_moves",
                y="percentage", hue="player_result", palette="viridis", ax=ax)

    # Ajouter les pourcentages au-dessus des barres
    for p in ax.patches:
        height = p.get_height()
        ax.annotate(f'{height:.1f}%', (p.get_x() + p.get_width() / 2., height),
                    ha='center', va='bottom', fontsize=12)

    ax.set_title("Pourcentage des Résultats par les Trois Premiers Coups")
    ax.set_ylabel("Pourcentage (%)")
    ax.set_xlabel("Trois Premiers Coups")
    ax.grid()
    return fig

