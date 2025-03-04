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
import time

# Ajoute le répertoire src au chemin Python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importe les modules personnalisés
from data_fetcher import DataFetcher
from data_processor import DataProcessor

# Configuration de la page
st.set_page_config(
    page_title="Tableau de Bord - Santé des Fabricants",
    page_icon="📊",
    layout="wide"
)

# Titre du tableau de bord
st.title("📊 Tableau de Bord - Santé des Fabricants sur le Marché")

# Description du tableau de bord
st.markdown("""
Ce tableau de bord présente les indicateurs de santé des fabricants sur le marché pour différentes catégories de produits.
En analysant les indicateurs de performance clés (KPIs), vous pouvez comprendre la position de leadership des fabricants sur le marché.
""")

# Initialise le récupérateur de données et le processeur
@st.cache_resource
def initialize_data_processor():
    """Initialise le processeur de données et le récupérateur"""
    # Crée le récupérateur et le processeur
    fetcher = DataFetcher()
    processor = DataProcessor()
    return fetcher, processor

fetcher, processor = initialize_data_processor()

# Crée le répertoire cache s'il n'existe pas
if not os.path.exists("cache"):
    os.makedirs("cache")

# Filtres de la barre latérale
st.sidebar.header("Filtres")

# Ajoute un sélecteur de source de données
data_source = st.sidebar.radio(
    "Source de données",
    options=["Données en cache", "Données des fichiers de test", "Données API en temps réel"],
    index=0
)

# Ajoute un bouton de rafraîchissement des données
if st.sidebar.button("🔄 Rafraîchir les données"):
    # Efface le cache et redémarre l'application
    st.cache_data.clear()
    st.rerun()

# Fonction pour charger les données depuis l'API
def load_data_from_api():
    """
    Charge les données directement depuis l'API
    
    Retourne:
        Tuple contenant les DataFrames des produits et des accords de vente
    """
    try:
        st.sidebar.info("⏳ Chargement des données depuis l'API en cours...")
        
        # Récupérer un nombre limité de logs pour la démonstration
        product_logs = fetcher.get_multiple_product_logs(1, 1000000)
        sale_logs = fetcher.get_multiple_sale_logs(1, 1000000)
        
        if not product_logs or not sale_logs:
            st.sidebar.warning("⚠️ Aucune donnée trouvée dans l'API, utilisation des données d'exemple.")
            return None, None
            
        # Convertir les logs en DataFrames
        product_df = fetcher.convert_logs_to_dataframe(product_logs, 'product')
        sale_df = fetcher.convert_logs_to_dataframe(sale_logs, 'sale')
        
        st.sidebar.success("✅ Données chargées depuis l'API")
        return product_df, sale_df
    except Exception as e:
        st.sidebar.error(f"❌ Erreur lors du chargement depuis l'API: {e}")
        return None, None

# Charge des données avec stratégie de cache
@st.cache_data(ttl=3600)  # Cache pendant une heure
def load_data_with_cache():
    """
    Charge les données avec une stratégie de cache multi-niveaux:
    1. Essaie de charger depuis l'API
    2. Essaie de charger depuis le cache fichier
    3. Essaie de charger depuis les fichiers de test
    4. Génère des données d'exemple en cas d'échec
    
    Retourne:
        Tuple contenant les DataFrames des produits et des accords de vente
    """
    # 1. Essaie de charger depuis l'API
    try:
        st.sidebar.info("⏳ Tentative de chargement depuis l'API...")
        
        # Récupérer des logs (augmenter la plage pour plus de données)
        product_logs = fetcher.get_multiple_product_logs(1, 1000000)
        sale_logs = fetcher.get_multiple_sale_logs(1, 1000000)
        
        if product_logs and sale_logs:
            # Convertir les logs en DataFrames
            product_df = fetcher.convert_logs_to_dataframe(product_logs, 'product')
            sale_df = fetcher.convert_logs_to_dataframe(sale_logs, 'sale')
            
            # Sauvegarder dans le cache pour une utilisation future
            cache_dir = "cache"
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir)
                
            product_cache = f"{cache_dir}/product_data.csv"
            sale_cache = f"{cache_dir}/sale_data.csv"
            
            fetcher.save_data_to_cache(product_df, product_cache)
            fetcher.save_data_to_cache(sale_df, sale_cache)
            
            st.sidebar.success("✅ Données chargées depuis l'API et mises en cache")
            return product_df, sale_df
    except Exception as e:
        st.sidebar.warning(f"⚠️ Impossible de charger depuis l'API: {e}")
    
    # 2. Essaie de charger depuis le cache fichier
    try:
        cache_dir = "cache"
        product_cache = f"{cache_dir}/product_data.csv"
        sale_cache = f"{cache_dir}/sale_data.csv"
        
        product_df = fetcher.load_data_from_cache(product_cache)
        sale_df = fetcher.load_data_from_cache(sale_cache)
        
        if product_df is not None and sale_df is not None:
            st.sidebar.success("✅ Données chargées depuis le cache")
            return product_df, sale_df
    except Exception as e:
        st.sidebar.info(f"⚠️ Impossible de charger depuis le cache: {e}")
    
    # 3. Essaie de charger depuis les fichiers de test
    try:
        product_file_path = "data_test/produits-tous/produits-tous.orig"
        sale_file_path = "data_test/pointsDeVente-tous/pointsDeVente-tous"
        
        if os.path.exists(product_file_path) and os.path.exists(sale_file_path):
            product_df = fetcher.load_test_data_from_text_file(product_file_path, 'product')
            sale_df = fetcher.load_test_data_from_text_file(sale_file_path, 'sale')
            
            # Sauvegarde dans le cache
            cache_dir = "cache"
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir)
                
            product_cache = f"{cache_dir}/product_data.csv"
            sale_cache = f"{cache_dir}/sale_data.csv"
            
            fetcher.save_data_to_cache(product_df, product_cache)
            fetcher.save_data_to_cache(sale_df, sale_cache)
            
            st.sidebar.success("✅ Données chargées depuis les fichiers de test et mises en cache")
            return product_df, sale_df
    except Exception as e:
        st.sidebar.info(f"⚠️ Impossible de charger depuis les fichiers de test: {e}")
    
    # 4. Génère des données d'exemple
    st.sidebar.warning("⚠️ Utilisation des données d'exemple générées")
    
    # Données d'exemple générées
    product_data = {
        'logID': list(range(1, 101)),
        'prodID': list(range(101, 201)),
        'catID': np.random.choice([5, 10, 15, 20], 100),
        'fabID': np.random.choice(list(range(1, 21)), 100),
        'dateID': np.random.choice(list(range(1, 366)), 100)
    }
    
    # Données d'exemple pour les accords de vente
    sale_data = {
        'logID': list(range(1, 101)),
        'prodID': list(range(101, 201)),
        'catID': np.random.choice([5, 10, 15, 20], 100),
        'fabID': np.random.choice(list(range(1, 21)), 100),
        'magID': np.random.choice(list(range(1, 31)), 100),  # 30 magasins
        'dateID': np.random.choice(list(range(1, 366)), 100)
    }
    
    product_df = pd.DataFrame(product_data)
    sale_df = pd.DataFrame(sale_data)
    
    return product_df, sale_df

# Charge les données en fonction de la source sélectionnée
if data_source == "Données API en temps réel":
    product_df, sale_df = load_data_from_api()
    if product_df is None or sale_df is None:
        # Si l'API échoue, utiliser les données mises en cache
        product_df, sale_df = load_data_with_cache()
elif data_source == "Données des fichiers de test":
    try:
        product_file_path = "data_test/produits-tous/produits-tous.orig"
        sale_file_path = "data_test/pointsDeVente-tous/pointsDeVente-tous"
        
        if os.path.exists(product_file_path) and os.path.exists(sale_file_path):
            product_df = fetcher.load_test_data_from_text_file(product_file_path, 'product')
            sale_df = fetcher.load_test_data_from_text_file(sale_file_path, 'sale')
            st.sidebar.success("✅ Données chargées depuis les fichiers de test")
        else:
            st.sidebar.warning("⚠️ Fichiers de test non trouvés, utilisation des données en cache")
            product_df, sale_df = load_data_with_cache()
    except Exception as e:
        st.sidebar.error(f"❌ Erreur lors du chargement depuis les fichiers de test: {e}")
        product_df, sale_df = load_data_with_cache()
else:
    # Par défaut, charger les données mises en cache
    product_df, sale_df = load_data_with_cache()

# Mettre à jour le processeur avec les nouvelles données
processor.set_dataframes(product_df, sale_df)

# Ajoute un sélecteur d'ID de fabricant dans la barre latérale
manufacturer_id = st.sidebar.number_input("ID du Fabricant", min_value=1, max_value=1000000, value=1664)

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
    # Ajouter des informations de débogage
    st.sidebar.write(f"**Débogage moyenne produits (catégorie {category_id}):**")
    category_products = processor.product_df[processor.product_df['catID'] == category_id]
    st.sidebar.write(f"Nombre de produits dans catégorie {category_id}: {len(category_products)}")
    products_per_manufacturer = category_products.groupby('fabID')['prodID'].nunique()
    st.sidebar.write(f"Nombre de fabricants: {len(products_per_manufacturer)}")
    st.sidebar.write(f"Nombre moyen calculé: {products_per_manufacturer.mean()}")
    
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