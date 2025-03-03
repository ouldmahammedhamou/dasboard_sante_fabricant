"""
Module principal du tableau de bord - Création d'un tableau de bord interactif avec Streamlit
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
import os

# Ajoute le répertoire src au chemin Python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importe les modules personnalisés
from data_fetcher import DataFetcher
from data_processor import DataProcessor

# Configuration de la page
st.set_page_config(
    page_title="Tableau de Bord de Santé des Fabricants sur le Marché",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Ajoute un titre et une description
st.title("Tableau de Bord de Santé des Fabricants sur le Marché")
st.markdown("""
Ce tableau de bord présente les indicateurs de santé des fabricants sur le marché pour différentes catégories de produits.
En analysant les indicateurs de performance clés (KPIs), vous pouvez comprendre la position de leadership des fabricants sur le marché.
""")

# Initialise le récupérateur de données et le processeur
@st.cache_resource
def initialize_data_processor():
    fetcher = DataFetcher()
    processor = DataProcessor()
    return fetcher, processor

fetcher, processor = initialize_data_processor()

# Filtres de la barre latérale
st.sidebar.header("Filtres")

# Charge des données d'exemple (dans une application réelle, elles seraient récupérées depuis l'API)
@st.cache_data
def load_sample_data():
    # Données d'exemple pour les produits
    product_data = {
        'logID': list(range(1, 101)),
        'prodID': list(range(101, 201)),
        'catID': np.random.choice([5, 10, 15, 20], 100),
        'fabID': np.random.choice(list(range(1, 21)), 100),
        'dateID': np.random.choice(list(range(1, 366)), 100)  # Supposons que les ID de date 2022 vont de 1 à 366
    }
    
    # Données d'exemple pour les accords de vente
    sale_data = {
        'logID': list(range(1, 101)),
        'prodID': list(range(101, 201)),
        'catID': np.random.choice([5, 10, 15, 20], 100),
        'fabID': np.random.choice(list(range(1, 21)), 100),
        'magID': np.random.choice(list(range(1, 31)), 100),  # 30 magasins
        'dateID': np.random.choice(list(range(1, 366)), 100)  # Supposons que les ID de date 2022 vont de 1 à 366
    }
    
    product_df = pd.DataFrame(product_data)
    sale_df = pd.DataFrame(sale_data)
    
    return product_df, sale_df

product_df, sale_df = load_sample_data()
processor.set_dataframes(product_df, sale_df)

# Ajoute un sélecteur d'ID de fabricant dans la barre latérale
manufacturer_id = st.sidebar.number_input("ID du Fabricant", min_value=1, max_value=20, value=1664)

# Ajoute un sélecteur de catégorie
available_categories = sorted(product_df['catID'].unique())
category_id = st.sidebar.selectbox("Catégorie de Produit", options=available_categories, index=0 if 5 in available_categories else 0)

# Ajoute un sélecteur de plage de dates
start_date = datetime(2022, 1, 1)
end_date = datetime(2022, 12, 31)
date_range = st.sidebar.date_input(
    "Plage de Dates",
    value=(start_date, end_date),
    min_value=start_date,
    max_value=end_date
)

# Si l'utilisateur a sélectionné une date, la convertit en un tuple contenant les dates de début et de fin
if isinstance(date_range, tuple) and len(date_range) == 2:
    selected_start_date, selected_end_date = date_range
else:
    selected_start_date, selected_end_date = start_date, end_date

# Indicateurs KPI principaux
st.header("Indicateurs de Performance Clés (KPIs)")

# Crée une mise en page à trois colonnes pour afficher les KPI
col1, col2, col3 = st.columns(3)

with col1:
    # Calcule le nombre d'acteurs du marché pour la catégorie 5
    actor_count = processor.count_market_actors_by_category(category_id)
    st.metric(
        label=f"Nombre d'Acteurs du Marché - Catégorie {category_id}",
        value=actor_count,
        delta="+2 par rapport au mois dernier",  # Dans une application réelle, cela serait calculé
        delta_color="normal"
    )

with col2:
    # Calcule le nombre moyen de produits par fabricant pour la catégorie 5
    avg_products = processor.avg_products_per_manufacturer_by_category(category_id)
    st.metric(
        label=f"Nombre Moyen de Produits/Fabricant - Catégorie {category_id}",
        value=f"{avg_products:.2f}",
        delta="-0.5 par rapport au mois dernier",  # Dans une application réelle, cela serait calculé
        delta_color="normal"
    )

with col3:
    # Calcule le score de santé du fabricant
    health_score = processor.manufacturer_health_score(manufacturer_id, category_id)
    st.metric(
        label=f"Score de Santé du Fabricant {manufacturer_id}",
        value=f"{health_score:.2%}",
        delta="+1.2% par rapport au mois dernier",  # Dans une application réelle, cela serait calculé
        delta_color="normal"
    )

# Affiche les 10 premiers magasins
st.header(f"Top 10 des Magasins")
top_stores = processor.top_stores(10)


# Crée un graphique à barres
fig_stores = px.bar(
    top_stores, 
    x='magID', 
    y='agreement_count',
    title=f"Top 10 des Magasins (par nombre d'accords de vente)",
    labels={"magID": "ID du Magasin", "agreement_count": "Nombre d'Accords de Vente"},
    color='agreement_count',
    color_continuous_scale='Viridis'
)
st.plotly_chart(fig_stores, use_container_width=True)

# Crée une mise en page à deux colonnes
col1, col2 = st.columns(2)

with col1:
    # Crée un graphique d'évolution du nombre d'acteurs du marché au fil du temps (données simulées)
    st.subheader(f"Évolution du Nombre d'Acteurs du Marché - Catégorie {category_id}")
    
    # Données mensuelles simulées
    months = ['Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin', 
              'Juillet', 'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre']
    # Données simulées, seraient obtenues du processeur dans une application réelle
    actor_counts = np.random.randint(15, 25, size=12)
    
    # Crée un graphique linéaire
    fig_actors = px.line(
        x=months, 
        y=actor_counts,
        markers=True,
        title=f"Tendance du Nombre d'Acteurs du Marché - Catégorie {category_id} (2022)",
        labels={"x": "Mois", "y": "Nombre d'Acteurs"}
    )
    st.plotly_chart(fig_actors, use_container_width=True)

with col2:
    # Crée un graphique d'évolution du score de santé du fabricant au fil du temps (données simulées)
    st.subheader(f"Évolution du Score de Santé du Fabricant {manufacturer_id}")
    
    # Données simulées, seraient obtenues du processeur dans une application réelle
    health_scores = np.random.uniform(0.1, 0.3, size=12)
    
    # Crée un graphique linéaire
    fig_health = px.line(
        x=months, 
        y=health_scores,
        markers=True,
        title=f"Tendance du Score de Santé du Fabricant {manufacturer_id} (2022)",
        labels={"x": "Mois", "y": "Score de Santé"}
    )
    fig_health.update_layout(yaxis_tickformat='.1%')
    st.plotly_chart(fig_health, use_container_width=True)

# Affiche des tableaux de données (repliables)
with st.expander("Voir les Données Brutes"):
    tab1, tab2 = st.tabs(["Données des Produits", "Données des Accords de Vente"])
    
    with tab1:
        st.dataframe(product_df.head(50))
    
    with tab2:
        st.dataframe(sale_df.head(50))

# Ajoute des annotations et des explications
st.markdown("""
### Explication des KPI

1. **Nombre d'Acteurs du Marché**: Nombre de fabricants actifs sur le marché pour une catégorie de produit spécifique.
2. **Nombre Moyen de Produits/Fabricant**: Nombre moyen de produits qu'un fabricant propose dans une catégorie spécifique.
3. **Score de Santé du Fabricant**: Proportion moyenne des produits d'un fabricant parmi tous les produits d'une catégorie dans les 10 premiers magasins.

### Source des Données
- Données provenant de l'API distante: http://51.255.166.155:1353/
- Période d'analyse: 1er janvier 2022 au 31 décembre 2022
""")

# Ajoute un pied de page
st.markdown("---")
st.markdown("© 2025 Projet d'Analyse de la Santé des Fabricants sur le Marché | Auteur: XXX") 