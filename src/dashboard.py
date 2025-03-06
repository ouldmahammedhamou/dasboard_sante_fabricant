"""
Module principal du tableau de bord - Création d'un tableau de bord interactif avec Streamlit
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
import sys
import os
import json
from typing import Dict, List, Tuple, Optional, Any

# Ajoute le répertoire src au chemin Python
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importe les modules personnalisés
from db_handler import PostgresHandler
from data_processor import DataProcessor

# Configuration de la page
st.set_page_config(
    page_title="Tableau de Bord - Santé des Fabricants",
    page_icon="📊",
    layout="wide"
)

# Définir le style global
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E88E5;
        text-align: center;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #0277BD;
    }
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
    }
    .success-metric {
        color: #00C853;
        font-weight: bold;
    }
    .warning-metric {
        color: #FF9800;
        font-weight: bold;
    }
    .danger-metric {
        color: #F44336;
        font-weight: bold;
    }
    .info-box {
        background-color: #E3F2FD;
        border-left: 5px solid #2196F3;
        padding: 10px;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

def initialize_data_processor():
    """Initialise le processeur de données"""
    return DataProcessor()

def initialize_db_handler():
    """Initialise la connexion à la base de données PostgreSQL"""
    db = PostgresHandler()
    try:
        db.connect()
        return db
    except Exception as e:
        st.sidebar.error(f"❌ Erreur de connexion à la base de données: {e}")
        return None

def load_data_from_test_file(file_path: str, data_type: str) -> pd.DataFrame:
    """
    Charge les données à partir d'un fichier de test
    
    Paramètres:
        file_path: Chemin du fichier
        data_type: Type de données ('product' ou 'sale')
        
    Retourne:
        DataFrame avec les données chargées
    """
    try:
        # Vérifier si le fichier existe
        if not os.path.exists(file_path):
            st.sidebar.error(f"❌ Fichier non trouvé: {file_path}")
            return pd.DataFrame()
            
        # Déterminer le format de fichier en fonction de l'extension
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.csv':
            # Lire le fichier CSV
            df = pd.read_csv(file_path)
            
            # Vérifier si les données ont été chargées
            if df.empty:
                st.sidebar.warning(f"⚠️ Aucune donnée trouvée dans le fichier CSV: {file_path}")
                return pd.DataFrame()
                
            # S'assurer que les colonnes date_formatted sont au format datetime
            if 'date_formatted' in df.columns:
                df['date_formatted'] = pd.to_datetime(df['date_formatted'])
                
            # Informations sur les données chargées
            st.sidebar.success(f"✅ Données CSV chargées avec succès: {len(df)} enregistrements")
            return df
            
        elif file_ext in ['.jsonl', '.json', '.orig']:
            # Lire le fichier JSON ligne par ligne
            records = []
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:  # Ignorer les lignes vides
                        try:
                            record = json.loads(line)
                            records.append(record)
                        except json.JSONDecodeError:
                            # Essayer de parser comme des valeurs séparées par des espaces
                            parts = line.split()
                            if data_type == 'product' and len(parts) >= 4:
                                record = {
                                    'date_id': parts[0],
                                    'prod_id': parts[1],
                                    'cat_id': parts[2],
                                    'fab_id': parts[3]
                                }
                                records.append(record)
                            elif data_type == 'sale' and len(parts) >= 5:
                                record = {
                                    'date_id': parts[0],
                                    'prod_id': parts[1],
                                    'cat_id': parts[2],
                                    'fab_id': parts[3],
                                    'mag_id': parts[4]
                                }
                                records.append(record)
                            else:
                                st.sidebar.warning(f"⚠️ Ligne ignorée dans {file_path}: {line[:50]}...")
            
            if not records:
                st.sidebar.warning(f"⚠️ Aucun enregistrement valide trouvé dans {file_path}")
                return pd.DataFrame()
                
            df = pd.DataFrame(records)
            
            # Renommer les colonnes si nécessaire
            if data_type == 'product':
                column_map = {
                    'logID': 'log_id',
                    'prodID': 'prod_id',
                    'catID': 'cat_id',
                    'fabID': 'fab_id',
                    'dateID': 'date_id'
                }
            else:  # sale
                column_map = {
                    'logID': 'log_id',
                    'prodID': 'prod_id',
                    'catID': 'cat_id',
                    'fabID': 'fab_id',
                    'magID': 'mag_id',
                    'dateID': 'date_id'
                }
            
            # Renommer uniquement les colonnes qui existent
            for old_col, new_col in column_map.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})
            
            # Convertir les colonnes numériques
            for col in df.columns:
                if col != 'date_formatted':
                    try:
                        df[col] = pd.to_numeric(df[col], errors='ignore')
                    except:
                        pass
            
            # Ajouter la colonne de date formatée si elle n'existe pas
            if 'date_id' in df.columns and 'date_formatted' not in df.columns:
                try:
                    df['date_formatted'] = pd.to_datetime(df['date_id'].astype(str), format='%Y%m%d')
                except Exception as e:
                    st.sidebar.warning(f"⚠️ Impossible de créer la colonne date_formatted: {e}")
            
            st.sidebar.success(f"✅ Données JSON/texte chargées avec succès: {len(df)} enregistrements")
            return df
        else:
            st.sidebar.error(f"❌ Format de fichier non pris en charge: {file_ext}")
            return pd.DataFrame()
        
    except Exception as e:
        st.sidebar.error(f"❌ Erreur lors du chargement du fichier {file_path}: {e}")
        import traceback
        st.sidebar.error(traceback.format_exc())
        return pd.DataFrame()

def load_data_from_database():
    """Charge les données directement depuis la base de données sans mise en cache"""
    try:
        # Initialiser la connexion à la base de données
        db = initialize_db_handler()
        if db is None:
            return pd.DataFrame(), pd.DataFrame()
        
        # Récupérer les données
        product_df = db.get_products_dataframe()
        sale_df = db.get_sales_dataframe()
        
        st.sidebar.success(f"✅ Données chargées depuis la base de données")
        
        return product_df, sale_df
    except Exception as e:
        st.sidebar.error(f"❌ Erreur lors du chargement des données: {e}")
        return pd.DataFrame(), pd.DataFrame()

def main():
    """Fonction principale qui gère l'interface du tableau de bord"""
    
    # Titre principal
    st.markdown('<h1 class="main-header">Tableau de Bord de l\'analyse de Marché</h1>', unsafe_allow_html=True)
    
    # Barre latérale pour les contrôles
    st.sidebar.title("Contrôles")
    
    # Section de sélection de source de données
    st.sidebar.header("Source de données")
    data_source = st.sidebar.radio(
        "Sélectionner la source de données",
        options=["Base de données", "Fichier de test"],
        index=0
    )
    
    # Initialiser le processeur de données
    processor = initialize_data_processor()
    
    # Charger les données selon la source sélectionnée
    if data_source == "Base de données":
        product_df, sale_df = load_data_from_database()
        if product_df.empty or sale_df.empty:
            st.error("❌ Impossible de charger les données depuis la base de données. Veuillez vérifier la connexion.")
            return
    else:  # Fichier de test
        # Sélecteur de fichiers de test
        test_product_file = st.sidebar.text_input("Fichier de données produits", "data/test_products.csv")
        test_sale_file = st.sidebar.text_input("Fichier de données ventes", "data/test_sales.csv")
        
        # Charger les données depuis les fichiers de test
        product_df = load_data_from_test_file(test_product_file, "product")
        sale_df = load_data_from_test_file(test_sale_file, "sale")
        
        if product_df.empty or sale_df.empty:
            st.error("❌ Impossible de charger les données depuis les fichiers de test. Veuillez vérifier les chemins des fichiers.")
            return
            
        st.sidebar.info("ℹ️ Utilisation de données depuis les fichiers de test.")
    
    # Mettre à jour le processeur de données avec les données chargées
    processor.set_dataframes(product_df, sale_df)
    
    # Information sur les données
    st.sidebar.subheader("Information sur les données")
    info_text = f"📊 Produits: {len(product_df)} enregistrements\n"
    info_text += f"🛒 Accords de vente: {len(sale_df)} enregistrements"
    st.sidebar.info(info_text)
    
    # Obtenir les valeurs uniques pour les filtres
    if not product_df.empty:
        categories = sorted(product_df['cat_id'].unique())
        manufacturers = sorted(product_df['fab_id'].unique())
        
        # Filtres dans la barre latérale
        st.sidebar.header("Filtres")
        selected_category = st.sidebar.selectbox("Catégorie", categories, index=0)
        selected_manufacturer = st.sidebar.selectbox("Fabricant", manufacturers, index=0)
        
        # Filtrer par date
        if 'date_formatted' in product_df.columns and len(product_df['date_formatted']) > 0 and not product_df['date_formatted'].isna().all():
            try:
                date_min = product_df['date_formatted'].min()
                date_max = product_df['date_formatted'].max()
                 
                if hasattr(date_min, 'date'):
                    min_date = date_min.date()
                else:
                    min_date = date_min
                    
                if hasattr(date_max, 'date'):
                    max_date = date_max.date()
                else:
                    max_date = date_max
            except (AttributeError, ValueError, TypeError) as e:
                st.sidebar.warning(f"Erreur lors du traitement des dates: {e}. Utilisation des dates par défaut.")
                min_date = date(2022, 1, 1)
                max_date = date(2022, 12, 31)
        else:
            min_date = date(2022, 1, 1)
            max_date = date(2022, 12, 31)

        date_range = st.sidebar.date_input(
            "Plage de dates",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        # S'assurer que la plage de dates est valide
        if len(date_range) == 2:
            start_date, end_date = date_range
            if start_date > end_date:
                st.sidebar.error("⚠️ La date de début doit être antérieure à la date de fin.")
                return
        else:
            st.sidebar.error("⚠️ Veuillez sélectionner une plage de dates complète.")
            return
            
        # Afficher des informations sur les données filtrées
        # Ajout d'un contrôle de types pour le filtrage des dates
        try:
            # Vérifier le type de date_formatted
            if 'date_formatted' in product_df.columns and len(product_df) > 0:
                # Convertir date_formatted en datetime si ce n'est pas déjà le cas
                if not pd.api.types.is_datetime64_any_dtype(product_df['date_formatted']):
                    product_df['date_formatted'] = pd.to_datetime(product_df['date_formatted'])
                if not pd.api.types.is_datetime64_any_dtype(sale_df['date_formatted']):
                    sale_df['date_formatted'] = pd.to_datetime(sale_df['date_formatted'])
            
            # Maintenant nous pouvons filtrer en toute sécurité
            filtered_products = product_df[
                (product_df['cat_id'] == selected_category) &
                (product_df['date_formatted'] >= pd.Timestamp(start_date)) &
                (product_df['date_formatted'] <= pd.Timestamp(end_date))
            ]
            
            filtered_sales = sale_df[
                (sale_df['cat_id'] == selected_category) &
                (sale_df['date_formatted'] >= pd.Timestamp(start_date)) &
                (sale_df['date_formatted'] <= pd.Timestamp(end_date))
            ]
        except Exception as e:
            st.error(f"Erreur lors du filtrage des données par date: {e}")
            # Fallback: filtrer uniquement par catégorie si le filtrage par date échoue
            filtered_products = product_df[product_df['cat_id'] == selected_category]
            filtered_sales = sale_df[sale_df['cat_id'] == selected_category]
        
        # Statistiques de base
        st.sidebar.subheader("Statistiques de base")
        filtered_info = f"📊 Produits filtrés: {len(filtered_products)}\n🛒 Accords de vente filtrés: {len(filtered_sales)}"
        st.sidebar.info(filtered_info)
        
        # Afficher le top 10 des magasins dans la barre latérale
        st.sidebar.subheader("Top 10 des magasins par nombre d'accords")
        top_stores = processor.top_stores(10)
        if not top_stores.empty:
            # Créer une version abrégée pour la barre latérale
            st.sidebar.dataframe(top_stores[['mag_id', 'agreement_count']], use_container_width=True)
        else:
            st.sidebar.warning("⚠️ Aucune donnée de magasin disponible.")
            
        # Corps principal
        # Pour la catégorie et le fabricant sélectionnés
        st.subheader(f"Analyse pour la catégorie {selected_category} et le fabricant {selected_manufacturer}")
        
        # Vérifier si nous avons des données pour ce fabricant et cette catégorie
        manufacturer_sales = filtered_sales[filtered_sales['fab_id'] == selected_manufacturer]
        
        if manufacturer_sales.empty:
            st.warning(f"⚠️ Aucun accord de vente trouvé pour le fabricant {selected_manufacturer} dans la catégorie {selected_category}.")
        
        # Créer les onglets pour organiser le contenu
        tab1, tab2, tab3, tab4 = st.tabs(["Graphiques KPI", "Top Magasins", "Évolution Temporelle", "Données Brutes"])
        
        with tab1:
            # KPI 1: Nombre d'acteurs dans la catégorie
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.info("Acteurs dans cette catégorie")
                market_actors = processor.count_market_actors_by_category(selected_category)
                st.metric("Nombre de fabricants", market_actors)
                
                # Évaluation de la concurrence
                if market_actors <= 3:
                    st.markdown("<p class='success-metric'>✅ Marché concentré</p>", unsafe_allow_html=True)
                elif market_actors <= 7:
                    st.markdown("<p class='warning-metric'>⚠️ Marché modérément compétitif</p>", unsafe_allow_html=True)
                else:
                    st.markdown("<p class='danger-metric'>❗ Marché très compétitif</p>", unsafe_allow_html=True)
                    
            with col2:
                st.info("Produits par fabricant")
                avg_products = processor.avg_products_per_manufacturer_by_category(selected_category)
                st.metric("Moyenne de produits", f"{avg_products:.1f}")
                
                # Évaluation de la diversité des produits
                if avg_products <= 2:
                    st.markdown("<p class='danger-metric'>❗ Faible diversité de produits</p>", unsafe_allow_html=True)
                elif avg_products <= 5:
                    st.markdown("<p class='warning-metric'>⚠️ Diversité de produits moyenne</p>", unsafe_allow_html=True)
                else:
                    st.markdown("<p class='success-metric'>✅ Bonne diversité de produits</p>", unsafe_allow_html=True)
                
            with col3:
                st.info("Santé du fabricant")
                health_score = processor.manufacturer_health_score(selected_manufacturer, selected_category)
                st.metric("Score de santé", f"{health_score:.2f}")
                
                # Évaluation du score de santé
                if health_score <= 0.3:
                    st.markdown("<p class='danger-metric'>❗ Fabricant en difficulté</p>", unsafe_allow_html=True)
                elif health_score <= 0.7:
                    st.markdown("<p class='warning-metric'>⚠️ Fabricant stable</p>", unsafe_allow_html=True)
                else:
                    st.markdown("<p class='success-metric'>✅ Fabricant performant</p>", unsafe_allow_html=True)

            # Graphique de la part de marché
            st.subheader("Part de marché dans la catégorie")
            market_share = processor.manufacturer_share_in_category(selected_manufacturer, selected_category)
            
            # Créer un graphique en anneau pour la part de marché
            fig = go.Figure(go.Pie(
                values=[market_share * 100, (1 - market_share) * 100],
                labels=[f"Fabricant {selected_manufacturer}", "Autres fabricants"],
                hole=0.6,
                marker_colors=['#1E88E5', '#E0E0E0']
            ))
            
            fig.update_layout(
                title=f"Part de marché du fabricant {selected_manufacturer} dans la catégorie {selected_category}",
                annotations=[dict(text=f"{market_share:.1%}", x=0.5, y=0.5, font_size=20, showarrow=False)]
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
        with tab2:
            st.subheader("Présence dans les principaux magasins")
            
            # Top 10 des magasins
            top_stores_df = processor.top_stores(10)
            
            if not top_stores_df.empty:
                # Obtenir les magasins où ce fabricant a des produits
                manufacturer_stores = set(filtered_sales[filtered_sales['fab_id'] == selected_manufacturer]['mag_id'].unique())
                top_store_ids = set(top_stores_df['mag_id'].unique())
                
                # Calculer le pourcentage de présence dans les top magasins
                presence_percentage = len(manufacturer_stores.intersection(top_store_ids)) / len(top_store_ids) if top_store_ids else 0
                
                # Afficher le pourcentage
                st.metric("Présence dans les top 10 magasins", f"{presence_percentage:.0%}")
                
                # Indicateur de santé
                if presence_percentage <= 0.3:
                    st.markdown("<p class='danger-metric'>❗ Faible présence dans les magasins clés</p>", unsafe_allow_html=True)
                elif presence_percentage <= 0.7:
                    st.markdown("<p class='warning-metric'>⚠️ Présence moyenne dans les magasins clés</p>", unsafe_allow_html=True)
                else:
                    st.markdown("<p class='success-metric'>✅ Forte présence dans les magasins clés</p>", unsafe_allow_html=True)
                
                # Créer des données pour le graphique
                presence_data = []
                for _, row in top_stores_df.iterrows():
                    store_id = row['mag_id']
                    is_present = store_id in manufacturer_stores
                    presence_data.append({
                        'mag_id': store_id,
                        'agreement_count': row['agreement_count'],
                        'present': "Oui" if is_present else "Non"
                    })
                
                presence_df = pd.DataFrame(presence_data)
                
                # Graphique de présence
                fig = px.bar(
                    presence_df,
                    x='mag_id',
                    y='agreement_count',
                    color='present',
                    labels={'mag_id': 'ID du magasin', 'agreement_count': "Nombre d'accords", 'present': 'Présence du fabricant'},
                    title=f"Présence du fabricant {selected_manufacturer} dans les top 10 magasins",
                    color_discrete_map={"Oui": "#00C853", "Non": "#F44336"}
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Afficher le tableau de données
                st.dataframe(presence_df, use_container_width=True)
            else:
                st.warning("⚠️ Aucune donnée de magasin disponible.")
                
        with tab3:
            st.subheader("Évolution temporelle")
            
            # Créer un sélecteur de granularité temporelle
            time_granularity = st.selectbox(
                "Granularité temporelle",
                options=["Jour", "Semaine", "Mois", "Trimestre", "Année"],
                index=2  # Mois par défaut
            )
            
            # Mapper la sélection à la fréquence pandas
            freq_map = {
                "Jour": "D",
                "Semaine": "W",
                "Mois": "M",
                "Trimestre": "Q",
                "Année": "Y"
            }
            
            selected_freq = freq_map[time_granularity]
            
            # Obtenir l'évolution des acteurs du marché au fil du temps
            try:
                # Vérifier que les dates sont au bon format pour le traitement
                start_date_obj = start_date
                end_date_obj = end_date
                
                # Convertir en datetime.datetime si nécessaire pour le traitement
                if isinstance(start_date, date) and not isinstance(start_date, datetime):
                    start_date_obj = datetime.combine(start_date, datetime.min.time())
                if isinstance(end_date, date) and not isinstance(end_date, datetime):
                    end_date_obj = datetime.combine(end_date, datetime.min.time())
                
                # Enlever le timezone si présent
                start_date_obj = start_date_obj.replace(tzinfo=None) if hasattr(start_date_obj, 'tzinfo') else start_date_obj
                end_date_obj = end_date_obj.replace(tzinfo=None) if hasattr(end_date_obj, 'tzinfo') else end_date_obj
                
                evolution_df = processor.market_actors_over_time(
                    selected_category,
                    start_date_obj,
                    end_date_obj,
                    selected_freq
                )
            except Exception as e:
                st.error(f"Erreur lors du calcul de l'évolution des acteurs: {e}")
                evolution_df = pd.DataFrame(columns=['period_start', 'active_manufacturers'])
            
            if not evolution_df.empty:
                # Créer un graphique d'évolution
                fig = px.line(
                    evolution_df,
                    x='period_start',
                    y='active_manufacturers',
                    labels={
                        'period_start': 'Période',
                        'active_manufacturers': 'Fabricants actifs'
                    },
                    title=f"Évolution du nombre de fabricants actifs dans la catégorie {selected_category}"
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Afficher si le fabricant sélectionné était présent dans chaque période
                st.subheader(f"Activité du fabricant {selected_manufacturer} au fil du temps")
                
                # Filtrer les ventes du fabricant sélectionné
                manufacturer_sales_over_time = filtered_sales[filtered_sales['fab_id'] == selected_manufacturer]
                
                if not manufacturer_sales_over_time.empty:
                    try:
                        # S'assurer que date_formatted est au format datetime pour le groupement
                        if not pd.api.types.is_datetime64_any_dtype(manufacturer_sales_over_time['date_formatted']):
                            manufacturer_sales_over_time = manufacturer_sales_over_time.copy()
                            manufacturer_sales_over_time['date_formatted'] = pd.to_datetime(manufacturer_sales_over_time['date_formatted'])
                        
                        # Regrouper par période
                        manufacturer_activity = manufacturer_sales_over_time.groupby(
                            pd.Grouper(key='date_formatted', freq=selected_freq)
                        ).size().reset_index()
                        manufacturer_activity.columns = ['period_start', 'sales_count']
                        
                        # Créer un graphique d'activité
                        fig = px.bar(
                            manufacturer_activity,
                            x='period_start',
                            y='sales_count',
                            labels={
                                'period_start': 'Période',
                                'sales_count': 'Nombre d\'accords'
                            },
                            title=f"Activité du fabricant {selected_manufacturer} par {time_granularity.lower()}"
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    except Exception as e:
                        st.error(f"Erreur lors de l'analyse de l'activité du fabricant: {e}")
                else:
                    st.warning(f"⚠️ Aucune activité trouvée pour le fabricant {selected_manufacturer} dans cette période.")
            else:
                st.warning("⚠️ Aucune donnée d'évolution disponible.")
                
        with tab4:
            st.subheader("Données brutes")
            
            # Créer des onglets pour les données brutes
            raw_tab1, raw_tab2 = st.tabs(["Produits", "Accords de vente"])
            
            with raw_tab1:
                st.subheader(f"Produits dans la catégorie {selected_category}")
                st.dataframe(filtered_products, use_container_width=True)
                
            with raw_tab2:
                st.subheader(f"Accords de vente dans la catégorie {selected_category}")
                st.dataframe(filtered_sales, use_container_width=True)

# Exécuter l'application lorsque le script est exécuté directement
if __name__ == "__main__":
    main() 