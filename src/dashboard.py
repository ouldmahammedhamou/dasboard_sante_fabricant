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
import requests
import logging
from typing import Optional, Tuple

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# Initialisation
@st.cache_resource
def initialize_data_processor():
    fetcher = DataFetcher()
    processor = DataProcessor()
    return fetcher, processor

# Fonctions de chargement des données
@st.cache_data
def load_sample_data():
    """Charge des données d'exemple"""
    product_data = {
        'logID': list(range(1, 101)),
        'prodID': list(range(101, 201)),
        'catID': np.random.choice([5, 10, 15, 20], 100),
        'fabID': np.random.choice(list(range(1, 21)), 100),
        'dateID': np.random.choice(list(range(1, 366)), 100)
    }
    
    sale_data = {
        'logID': list(range(1, 101)),
        'prodID': list(range(101, 201)),
        'catID': np.random.choice([5, 10, 15, 20], 100),
        'fabID': np.random.choice(list(range(1, 21)), 100),
        'magID': np.random.choice(list(range(1, 31)), 100),
        'dateID': np.random.choice(list(range(1, 366)), 100)
    }
    
    return pd.DataFrame(product_data), pd.DataFrame(sale_data)

@st.cache_data(ttl=3600)
def load_real_data():
    """Charge les données réelles depuis l'API"""
    try:
        fetcher = DataFetcher(base_url="http://51.255.166.155:1353")
        product_df, sale_df = fetcher.fetch_real_data()
        
        if product_df is None or sale_df is None or product_df.empty or sale_df.empty:
            st.warning("Données vides reçues de l'API. Utilisation des données de démonstration.")
            return load_sample_data()
            
        return product_df, sale_df
        
    except Exception as e:
        st.error(f"Erreur lors du chargement des données: {str(e)}")
        return load_sample_data()

# Interface utilisateur
st.title("Tableau de Bord de Santé des Fabricants sur le Marché")
st.markdown("""
Ce tableau de bord présente les indicateurs de santé des fabricants sur le marché pour différentes catégories de produits.
En analysant les indicateurs de performance clés (KPIs), vous pouvez comprendre la position de leadership des fabricants sur le marché.
""")

# Initialisation des composants
fetcher, processor = initialize_data_processor()

# Barre latérale
st.sidebar.header("Filtres")

# Section de contrôle des données
st.sidebar.markdown("---")
st.sidebar.subheader("Contrôle des données")

# Bouton de rafraîchissement avec état
if st.sidebar.button("🔄 Rafraîchir les données"):
    with st.sidebar.status("Mise à jour des données...", expanded=True) as status:
        st.cache_data.clear()
        try:
            product_df, sale_df = load_real_data()
            processor.set_dataframes(product_df, sale_df)
            status.update(label="Données mises à jour !", state="complete")
            st.session_state.last_update = datetime.now()
        except Exception as e:
            status.update(label=f"Erreur: {str(e)}", state="error")

# Affichage dernière mise à jour
if 'last_update' in st.session_state:
    st.sidebar.markdown(f"""
        **Dernière mise à jour**: {st.session_state.last_update.strftime('%H:%M:%S')}
    """)

# Option de rafraîchissement automatique
auto_refresh = st.sidebar.checkbox("Rafraîchissement automatique", value=False)
if auto_refresh:
    refresh_interval = st.sidebar.slider(
        "Intervalle de rafraîchissement (minutes)",
        min_value=1,
        max_value=60,
        value=5
    )
    if 'last_update' in st.session_state:
        time_since_update = (datetime.now() - st.session_state.last_update).seconds / 60
        if time_since_update >= refresh_interval:
            st.experimental_rerun()

st.sidebar.markdown("---")

# Chargement initial des données
try:
    product_df, sale_df = load_real_data()
    processor.set_dataframes(product_df, sale_df)
except Exception as e:
    st.error(f"Erreur lors du chargement initial des données: {e}")
    product_df, sale_df = load_sample_data()
    processor.set_dataframes(product_df, sale_df)

# Filtres
manufacturer_id = st.sidebar.number_input("ID du Fabricant", min_value=1, max_value=2000, value=1664)
available_categories = sorted(product_df['catID'].unique())
category_id = st.sidebar.selectbox("Catégorie de Produit", options=available_categories, index=0 if 5 in available_categories else 0)

# Sélecteur de dates
start_date = datetime(2022, 1, 1)
end_date = datetime(2022, 12, 31)
date_range = st.sidebar.date_input(
    "Plage de Dates",
    value=(start_date, end_date),
    min_value=start_date,
    max_value=end_date
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    selected_start_date, selected_end_date = date_range
else:
    selected_start_date, selected_end_date = start_date, end_date

# Période de comparaison
period_days = st.sidebar.slider(
    "Période de comparaison (jours)", 
    min_value=7,
    max_value=90,
    value=30
)

# KPIs
st.header("Indicateurs de Performance Clés (KPIs)")
col1, col2, col3 = st.columns(3)

with col1:
    actor_count, delta_value = processor.calculate_market_actors_delta(category_id, period_days)
    st.metric(
        label=f"Nombre d'Acteurs du Marché - Catégorie {category_id}",
        value=actor_count,
        delta=delta_value,
        delta_color="normal" if actor_count >= 0 else "inverse"
    )

with col2:
    avg_products = processor.avg_products_per_manufacturer_by_category(category_id)
    current_avg, delta_text = processor.calculate_avg_products_delta(category_id, period_days)
    st.metric(
        label=f"Nombre Moyen de Produits/Fabricant - Catégorie {category_id}",
        value=f"{current_avg:.2f}",
        delta=delta_text,
        delta_color="normal"
    )

with col3:
    try:
        current_score, delta_text = processor.calculate_health_score_delta(
            manufacturer_id, 
            category_id, 
            period_days
        )
        st.metric(
            label=f"Score de Santé du Fabricant {manufacturer_id}",
            value=f"{current_score:.1%}",
            delta=delta_text,
            delta_color="normal" if float(delta_text.split('%')[0]) >= 0 else "inverse"
        )
    except Exception as e:
        st.metric(
            label=f"Score de Santé du Fabricant {manufacturer_id}",
            value="N/A",
            delta="Erreur de calcul",
            delta_color="normal"
        )

# Graphiques
st.header("Top 10 des Magasins")
top_stores = processor.top_stores(10)
fig_stores = px.bar(
    top_stores, 
    x='magID', 
    y='agreement_count',
    title="Top 10 des Magasins (par nombre d'accords de vente)",
    labels={"magID": "ID du Magasin", "agreement_count": "Nombre d'Accords de Vente"},
    color='agreement_count',
    color_continuous_scale='Viridis'
)
st.plotly_chart(fig_stores, use_container_width=True)

# Graphiques d'évolution
col1, col2 = st.columns(2)

with col1:
    st.subheader(f"Évolution du Nombre d'Acteurs du Marché - Catégorie {category_id}")
    market_evolution = processor.market_actors_over_time(
        category_id,
        selected_start_date,
        selected_end_date,
        'M'
    )
    fig_actors = px.line(
        market_evolution,
        x='period_start',
        y='actor_count',
        markers=True,
        title=f"Tendance du Nombre d'Acteurs du Marché - Catégorie {category_id}",
        labels={"period_start": "Période", "actor_count": "Nombre d'Acteurs"}
    )
    st.plotly_chart(fig_actors, use_container_width=True)

with col2:
    st.subheader(f"Évolution du Score de Santé du Fabricant {manufacturer_id}")
    health_evolution = processor.calculate_health_score_evolution(
        manufacturer_id,
        category_id,
        selected_start_date,
        selected_end_date
    )
    fig_health = px.line(
        health_evolution,
        x='period',
        y='score',
        markers=True,
        title=f"Tendance du Score de Santé du Fabricant {manufacturer_id}",
        labels={"period": "Période", "score": "Score de Santé"}
    )
    fig_health.update_layout(yaxis_tickformat='.1%')
    st.plotly_chart(fig_health, use_container_width=True)

# Données brutes
with st.expander("Voir les Données Brutes"):
    tab1, tab2 = st.tabs(["Données des Produits", "Données des Accords de Vente"])
    with tab1:
        st.dataframe(product_df.head(50))
    with tab2:
        st.dataframe(sale_df.head(50))

# Documentation
st.markdown("""
### Explication des KPI
1. **Nombre d'Acteurs du Marché**: Nombre de fabricants actifs sur le marché pour une catégorie de produit spécifique.
2. **Nombre Moyen de Produits/Fabricant**: Nombre moyen de produits qu'un fabricant propose dans une catégorie spécifique.
3. **Score de Santé du Fabricant**: Proportion moyenne des produits d'un fabricant parmi tous les produits d'une catégorie dans les 10 premiers magasins.

### Source des Données
- Données provenant de l'API distante: http://51.255.166.155:1353/
- Période d'analyse: 1er janvier 2022 au 31 décembre 2022
""")

# Pied de page
st.markdown("---")
st.markdown("© 2025 Projet d'Analyse de la Santé des Fabricants sur le Marché | Auteur: XXX")