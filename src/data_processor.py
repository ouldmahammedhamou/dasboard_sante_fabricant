"""
Module de traitement des données - Contient les fonctions de traitement et d'analyse des données
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple, Optional, Union
from datetime import datetime, timedelta, date
import random
import json
import os

class DataProcessor:
    """Classe pour traiter les données de produits et d'accords de vente et calculer les KPI"""
    
    def __init__(self, product_df: Optional[pd.DataFrame] = None, sale_df: Optional[pd.DataFrame] = None):
        """
        Initialise le processeur de données
        
        Paramètres:
            product_df: DataFrame des journaux de produits
            sale_df: DataFrame des journaux d'accords de vente
        """
        self.product_df = product_df
        self.sale_df = sale_df
        
    def set_dataframes(self, product_df: pd.DataFrame, sale_df: pd.DataFrame) -> None:
        """
        Définit les DataFrames utilisés par le processeur
        
        Paramètres:
            product_df: DataFrame des journaux de produits
            sale_df: DataFrame des journaux d'accords de vente
        """
        self.product_df = product_df
        self.sale_df = sale_df
    
    def get_date_from_id(self, date_id: int) -> datetime:
        """
        Convertit un ID de date en objet datetime
        
        Paramètres:
            date_id: ID de date au format YYYYMMDD
            
        Retourne:
            Objet datetime
        """
        date_str = str(date_id)
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
        return datetime(year, month, day)
    
    def add_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ajoute une colonne 'date' au DataFrame à partir de la colonne 'date_id'
        
        Paramètres:
            df: DataFrame à modifier
            
        Retourne:
            DataFrame avec la colonne 'date' ajoutée
        """
        if df is None or df.empty:
            return df
            
        # Vérifier si la colonne 'date' existe déjà
        if 'date' in df.columns:
            return df
            
        # Créer une copie pour éviter les warnings de SettingWithCopyWarning
        result_df = df.copy()
        
        # Si date_id est au format YYYYMMDD (ex: 20220101)
        if 'date_id' in result_df.columns and isinstance(result_df['date_id'].iloc[0], (str, int)) and str(result_df['date_id'].iloc[0]).isdigit():
            try:
                date_strings = result_df['date_id'].astype(str)
                
                # Vérifier si nous avons un format YYYYMMDD (8 chiffres)
                if date_strings.str.len().max() == 8:
                    result_df['date'] = pd.to_datetime(date_strings, format='%Y%m%d')
                    result_df['month'] = result_df['date'].dt.month
                    return result_df
            except Exception as e:
                print(f"Erreur lors de la conversion du format YYYYMMDD: {e}")
        
        # Si date_id est une chaîne de caractères au format YYYY-MM-DD
        if 'date_id' in result_df.columns and isinstance(result_df['date_id'].iloc[0], str):
            try:
                result_df['date'] = pd.to_datetime(result_df['date_id'])
                result_df['month'] = result_df['date'].dt.month
                return result_df
            except Exception as e:
                print(f"Erreur lors de la conversion de date_id en date: {e}")
        
        # Si date_id est un entier représentant un jour de l'année
        if 'date_id' in result_df.columns and (isinstance(result_df['date_id'].iloc[0], int) or 
                                             isinstance(result_df['date_id'].iloc[0], float)):
            try:
                # Convertir les date_id (jour de l'année) en dates complètes pour 2022
                # Exemple: date_id=1 -> 2022-01-01, date_id=32 -> 2022-02-01
                base_date = datetime(2022, 1, 1)
                result_df['date'] = result_df['date_id'].apply(
                    lambda x: base_date + pd.Timedelta(days=int(x)-1)
                )
                result_df['month'] = result_df['date'].dt.month
                return result_df
            except Exception as e:
                print(f"Erreur lors de la conversion de date_id (entier) en date: {e}")
                
        # Si aucune conversion n'a fonctionné, retourner le DataFrame original
        print("Avertissement: Impossible d'ajouter une colonne de date.")
        return df
    
    def count_market_actors_by_category(self, category_id: int, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> int:
        """
        Calcule le nombre d'acteurs du marché pour une catégorie spécifique
        
        Paramètres:
            category_id: ID de la catégorie de produit
            start_date: Date de début pour le filtrage (optionnel)
            end_date: Date de fin pour le filtrage (optionnel)
            
        Retourne:
            Nombre de fabricants différents dans la catégorie
        """
        if self.product_df is None:
            return 0
            
        # Ajouter des colonnes de date si nécessaire
        prod_df_with_date = self.add_date_column(self.product_df.copy())
        
        # Filtrer les produits par catégorie
        category_filter = prod_df_with_date['cat_id'] == category_id
        filtered_products = prod_df_with_date[category_filter]
        
        # Appliquer le filtre de date si spécifié
        if start_date and end_date and 'date_formatted' in prod_df_with_date.columns:
            date_filter = (
                (filtered_products['date_formatted'] >= start_date) &
                (filtered_products['date_formatted'] <= end_date)
            )
            filtered_products = filtered_products[date_filter]
        
        # Compter les fabricants uniques
        return filtered_products['fab_id'].nunique()
    
    def avg_products_per_manufacturer_by_category(self, category_id: int, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> float:
        """
        Calcule le nombre moyen de produits par fabricant dans une catégorie spécifique
        
        Paramètres:
            category_id: ID de la catégorie de produit
            start_date: Date de début pour le filtrage (optionnel)
            end_date: Date de fin pour le filtrage (optionnel)
            
        Retourne:
            Nombre moyen de produits par fabricant
        """
        if self.product_df is None:
            return 0.0
            
        # Ajouter des colonnes de date si nécessaire
        prod_df_with_date = self.add_date_column(self.product_df.copy())
        
        # Filtrer les produits par catégorie
        category_filter = prod_df_with_date['cat_id'] == category_id
        filtered_products = prod_df_with_date[category_filter]
        
        # Appliquer le filtre de date si spécifié
        if start_date and end_date and 'date_formatted' in prod_df_with_date.columns:
            date_filter = (
                (filtered_products['date_formatted'] >= start_date) &
                (filtered_products['date_formatted'] <= end_date)
            )
            filtered_products = filtered_products[date_filter]
            
        # Si aucun produit après filtrage, retourner 0
        if filtered_products.empty:
            return 0.0
            
        # Compter les produits par fabricant
        products_per_manufacturer = filtered_products.groupby('fab_id')['prod_id'].nunique()
        
        # Calculer la moyenne des produits par fabricant
        if len(products_per_manufacturer) == 0:
            return 0.0
            
        return products_per_manufacturer.mean()
    
    def top_stores(self, n: int = 10) -> pd.DataFrame:
        """
        Identifie les N premiers magasins
        Ici, nous définissons "top N magasins" comme ceux ayant le plus grand nombre d'accords de vente
        
        Paramètres:
            n: Nombre de magasins à retourner
            
        Retourne:
            DataFrame contenant les N premiers magasins
        """
        if self.sale_df is None:
            raise ValueError("DataFrame d'accords de vente non défini")
        
        # Calcule le nombre d'accords de vente pour chaque magasin
        store_counts = self.sale_df['mag_id'].value_counts().reset_index()
        store_counts.columns = ['mag_id', 'agreement_count']
        
        # Retourne les N premiers magasins
        return store_counts.head(n)
    
    def manufacturer_health_score(self, manufacturer_id: int, category_id: int, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, top_n_stores: int = 10) -> float:
        """
        Calcule le score de santé d'un fabricant pour une catégorie spécifique et les N premiers magasins
        
        Paramètres:
            manufacturer_id: ID du fabricant
            category_id: ID de la catégorie de produit
            start_date: Date de début pour le filtrage (optionnel)
            end_date: Date de fin pour le filtrage (optionnel)
            top_n_stores: Nombre des premiers magasins à considérer
            
        Retourne:
            Score de santé (proportion moyenne de produits du fabricant parmi tous les produits)
        """
        if self.sale_df is None or self.product_df is None:
            return 0.0
        
        # Ajouter des colonnes de date si nécessaire
        sale_df_with_date = self.add_date_column(self.sale_df.copy())
        
        # Filtrer par date si spécifié
        filtered_sales = sale_df_with_date
        if start_date and end_date and 'date_formatted' in sale_df_with_date.columns:
            filtered_sales = sale_df_with_date[
                (sale_df_with_date['date_formatted'] >= start_date) &
                (sale_df_with_date['date_formatted'] <= end_date)
            ]
        
        # Si aucune vente après filtrage, retourner 0
        if filtered_sales.empty:
            return 0.0
            
        # Vérifier si le fabricant a des produits dans cette catégorie
        manufacturer_products = self.product_df[
            (self.product_df['cat_id'] == category_id) & 
            (self.product_df['fab_id'] == manufacturer_id)
        ]
        
        if manufacturer_products.empty:
            return 0.0
        
        # Obtenir les identifiants des N premiers magasins
        top_stores = filtered_sales['mag_id'].value_counts().head(top_n_stores).index.tolist()
        
        # Si aucun magasin trouvé, retourner 0
        if not top_stores:
            return 0.0
        
        # Calculer le score pour chaque magasin top
        store_scores = []
        
        for store_id in top_stores:
            # Filtrer les ventes pour le magasin et la catégorie
            store_sales = filtered_sales[
                (filtered_sales['mag_id'] == store_id) & 
                (filtered_sales['cat_id'] == category_id)
            ]
            
            if store_sales.empty:
                store_scores.append(0.0)
                continue
                
            # Produits uniques vendus dans ce magasin pour cette catégorie
            unique_products = store_sales['prod_id'].nunique()
            
            # Produits du fabricant vendus dans ce magasin pour cette catégorie
            manufacturer_sold = store_sales[
                store_sales['fab_id'] == manufacturer_id
            ]['prod_id'].nunique()
            
            # Calculer le score pour ce magasin
            store_score = manufacturer_sold / unique_products if unique_products > 0 else 0.0
            store_scores.append(store_score)
        
        # Moyenne des scores de tous les magasins
        return sum(store_scores) / len(store_scores) if store_scores else 0.0
    
    def market_actors_over_time(self, category_id: int, start_date: datetime, end_date: datetime, freq: str = 'M') -> pd.DataFrame:
        """
        Calcule l'évolution du nombre d'acteurs du marché pour une catégorie spécifique
        """
        if self.product_df is None:
            raise ValueError("DataFrame de produits non défini")
        
        # Convertir les dates au format YYYYMMDD en datetime
        df_with_dates = self.product_df.copy()
        df_with_dates['date'] = pd.to_datetime(df_with_dates['date_id'].astype(str), format='%Y%m%d')
        
        # Filtre les produits pour la catégorie spécifique
        filtered_df = df_with_dates[
            (df_with_dates['cat_id'] == category_id) & 
            (df_with_dates['date'] >= start_date) & 
            (df_with_dates['date'] <= end_date)
        ]
        
        # Crée des périodes de temps
        time_periods = pd.date_range(start=start_date, end=end_date, freq=freq)
        
        # Calcule le nombre d'acteurs pour chaque période
        results = []
        for i in range(len(time_periods) - 1):
            period_start = time_periods[i]
            period_end = time_periods[i+1]
            
            period_data = filtered_df[
                (filtered_df['date'] >= period_start) & 
                (filtered_df['date'] < period_end)
            ]
            
            actor_count = period_data['fab_id'].nunique()
            
            results.append({
                'period_start': period_start,
                'actor_count': actor_count
            })
        
        return pd.DataFrame(results)
    
    def manufacturer_share_in_category(self, manufacturer_id: int, category_id: int, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> float:
        """
        Calcule la part de marché d'un fabricant dans une catégorie spécifique
        
        Paramètres:
            manufacturer_id: ID du fabricant
            category_id: ID de la catégorie de produit
            start_date: Date de début pour le filtrage (optionnel)
            end_date: Date de fin pour le filtrage (optionnel)
            
        Retourne:
            Proportion des produits du fabricant parmi tous les produits de la catégorie
        """
        if self.product_df is None:
            return 0.0
            
        # Ajouter des colonnes de date si nécessaire
        prod_df_with_date = self.add_date_column(self.product_df.copy())
        
        # Filtrer les produits par catégorie
        category_filter = prod_df_with_date['cat_id'] == category_id
        category_products = prod_df_with_date[category_filter]
        
        # Appliquer le filtre de date si spécifié
        if start_date and end_date and 'date_formatted' in prod_df_with_date.columns:
            date_filter = (
                (category_products['date_formatted'] >= start_date) &
                (category_products['date_formatted'] <= end_date)
            )
            category_products = category_products[date_filter]
        
        if category_products.empty:
            return 0.0
            
        # Compter les produits totaux et les produits du fabricant
        total_products = category_products['prod_id'].nunique()
        manufacturer_products = category_products[
            category_products['fab_id'] == manufacturer_id
        ]['prod_id'].nunique()
        
        return manufacturer_products / total_products if total_products > 0 else 0.0
    
    def manufacturer_products_in_category(self, manufacturer_id: int, category_id: int, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> int:
        """
        Compte le nombre de produits d'un fabricant dans une catégorie spécifique
        
        Paramètres:
            manufacturer_id: ID du fabricant
            category_id: ID de la catégorie
            start_date: Date de début pour le filtrage (optionnel)
            end_date: Date de fin pour le filtrage (optionnel)
            
        Retourne:
            Nombre de produits
        """
        if self.product_df is None:
            return 0
        
        # Ajouter des colonnes de date si nécessaire
        prod_df_with_date = self.add_date_column(self.product_df.copy())
        
        # Filtre les produits du fabricant dans la catégorie spécifique
        base_filter = (
            (prod_df_with_date['cat_id'] == category_id) & 
            (prod_df_with_date['fab_id'] == manufacturer_id)
        )
        filtered_products = prod_df_with_date[base_filter]
        
        # Appliquer le filtre de date si spécifié
        if start_date and end_date and 'date_formatted' in prod_df_with_date.columns:
            date_filter = (
                (filtered_products['date_formatted'] >= start_date) &
                (filtered_products['date_formatted'] <= end_date)
            )
            filtered_products = filtered_products[date_filter]
        
        # Retourner le nombre de produits uniques
        return filtered_products['prod_id'].nunique()
    
    def get_winter_discount_period(self, year: int = 2022) -> Tuple[datetime, datetime]:
        """
        Retourne la période de soldes d'hiver pour une année donnée
        
        Paramètres:
            year: Année pour laquelle calculer la période de soldes
            
        Retourne:
            Tuple (date de début, date de fin)
        """
        # Période de soldes d'hiver: 12 janvier - 8 février
        return (datetime(year, 1, 12), datetime(year, 2, 8))
    
    def get_summer_discount_period(self, year: int = 2022) -> Tuple[datetime, datetime]:
        """
        Retourne la période de soldes d'été pour une année donnée
        
        Paramètres:
            year: Année pour laquelle calculer la période de soldes
            
        Retourne:
            Tuple (date de début, date de fin)
        """
        # Période de soldes d'été: 22 juin - 19 juillet
        return (datetime(year, 6, 22), datetime(year, 7, 19))
    
    def avg_products_in_discount_period(self, category_id: int, is_winter: bool = True, year: int = 2022) -> float:
        """
        Calcule le nombre moyen de produits par fabricant dans une catégorie pendant une période de soldes
        
        Paramètres:
            category_id: ID de la catégorie de produit
            is_winter: Si True, calcule pour les soldes d'hiver, sinon pour les soldes d'été
            year: Année pour laquelle calculer
            
        Retourne:
            Nombre moyen de produits
        """
        if self.product_df is None:
            return 0.0
        
        # Obtenir la période de soldes
        if is_winter:
            start_date, end_date = self.get_winter_discount_period(year)
        else:
            start_date, end_date = self.get_summer_discount_period(year)
        
        # S'assurer que nous avons une colonne de date
        product_df_with_date = self.add_date_column(self.product_df)
        
        # Vérifier si la colonne date existe, sinon utiliser date_formatted
        date_col = 'date' if 'date' in product_df_with_date.columns else 'date_formatted'
        
        # Vérifier si la colonne existe
        if date_col not in product_df_with_date.columns:
            print(f"Avertissement: Colonne {date_col} introuvable dans le DataFrame des produits.")
            return 0.0
        
        # Filtrer par catégorie et plage de dates
        filtered_products = product_df_with_date[
            (product_df_with_date['cat_id'] == category_id) &
            (product_df_with_date[date_col] >= start_date) &
            (product_df_with_date[date_col] <= end_date)
        ]
        
        if filtered_products.empty:
            return 0.0
        
        # Pour chaque fabricant, calculer le nombre de produits
        products_per_manufacturer = filtered_products.groupby('fab_id')['prod_id'].nunique()
        
        # Si aucun fabricant n'est trouvé
        if len(products_per_manufacturer) == 0:
            return 0.0
            
        # Calculer la moyenne
        return products_per_manufacturer.mean()
    
    def top_stores_in_discount_period(self, category_id: int = None, n: int = 10, is_winter: bool = True, year: int = 2022) -> pd.DataFrame:
        """
        Identifie les N premiers magasins pendant une période de soldes
        
        Paramètres:
            category_id: ID de la catégorie de produit (si None, considère toutes les catégories)
            n: Nombre de magasins à retourner
            is_winter: Si True, calcule pour les soldes d'hiver, sinon pour les soldes d'été
            year: Année pour laquelle calculer
            
        Retourne:
            DataFrame contenant les N premiers magasins
        """
        if self.sale_df is None:
            return pd.DataFrame(columns=['mag_id', 'agreement_count'])
        
        # Obtenir la période de soldes
        if is_winter:
            start_date, end_date = self.get_winter_discount_period(year)
        else:
            start_date, end_date = self.get_summer_discount_period(year)
        
        # S'assurer que nous avons une colonne de date
        sale_df_with_date = self.add_date_column(self.sale_df)
        
        # Vérifier si la colonne date existe, sinon utiliser date_formatted
        date_col = 'date' if 'date' in sale_df_with_date.columns else 'date_formatted'
        
        # Vérifier si la colonne existe
        if date_col not in sale_df_with_date.columns:
            print(f"Avertissement: Colonne {date_col} introuvable dans le DataFrame des ventes.")
            return pd.DataFrame(columns=['mag_id', 'agreement_count'])
        
        # Filtrer par plage de dates
        filtered_sales = sale_df_with_date[
            (sale_df_with_date[date_col] >= start_date) &
            (sale_df_with_date[date_col] <= end_date)
        ]
        
        # Si une catégorie est spécifiée, filtrer par catégorie
        if category_id is not None:
            filtered_sales = filtered_sales[filtered_sales['cat_id'] == category_id]
        
        if filtered_sales.empty:
            return pd.DataFrame(columns=['mag_id', 'agreement_count'])
        
        # Calculer le nombre d'accords de vente pour chaque magasin
        store_counts = filtered_sales['mag_id'].value_counts().reset_index()
        store_counts.columns = ['mag_id', 'agreement_count']
        
        # Retourner les N premiers magasins
        return store_counts.head(n)
    
    def manufacturer_health_score_over_time(self, manufacturer_id: int, category_id: int, 
                                           start_date: datetime, end_date: datetime,
                                           top_n_stores: int = 10, freq: str = 'M') -> pd.DataFrame:
        """
        Calcule l'évolution du score de santé d'un fabricant
        """
        print(f"\nDébut du calcul pour fabricant {manufacturer_id}, catégorie {category_id}")
        print(f"Période: {start_date} à {end_date}")
        
        # Vérifier et convertir les dates
        sale_df = self.sale_df.copy()
        if 'date_formatted' not in sale_df.columns:
            sale_df['date_formatted'] = pd.to_datetime(sale_df['date_id'].astype(str), format='%Y%m%d')
        
        # Créer les périodes mensuelles
        periods = pd.date_range(start=start_date, end=end_date, freq=freq)
        results = []
        
        for period_start in periods:
            # Définir la fin de la période
            if freq == 'M':
                period_end = period_start + pd.offsets.MonthEnd(1)
            elif freq == 'W':
                period_end = period_start + pd.Timedelta(days=6)
            else:
                period_end = period_start + pd.Timedelta(days=1)
            
            print(f"\nAnalyse période: {period_start.strftime('%Y-%m-%d')} à {period_end.strftime('%Y-%m-%d')}")
            
            # Filtrer les ventes pour cette période et catégorie
            period_sales = sale_df[
                (sale_df['date_formatted'] >= period_start) &
                (sale_df['date_formatted'] <= period_end) &
                (sale_df['cat_id'] == category_id)
            ]
            
            if len(period_sales) == 0:
                print(f"Aucune vente pour cette période")
                results.append({
                    'period': period_start,
                    'health_score': 0.0
                })
                continue
            
            # Trouver les top magasins pour cette période
            top_stores = period_sales['mag_id'].value_counts().head(top_n_stores).index
            
            store_scores = []
            for store_id in top_stores:
                store_sales = period_sales[period_sales['mag_id'] == store_id]
                
                # Nombre total de produits uniques dans ce magasin
                total_products = store_sales['prod_id'].nunique()
                
                # Nombre de produits du fabricant
                manufacturer_products = store_sales[
                    store_sales['fab_id'] == manufacturer_id
                ]['prod_id'].nunique()
                
                if total_products > 0:
                    store_score = manufacturer_products / total_products
                    store_scores.append(store_score)
                    print(f"Magasin {store_id}: {manufacturer_products}/{total_products} = {store_score:.2f}")
            
            # Calculer le score moyen pour la période
            period_score = np.mean(store_scores) if store_scores else 0.0
            print(f"Score période: {period_score:.2f}")
            
            results.append({
                'period': period_start,
                'health_score': period_score
            })
        
        result_df = pd.DataFrame(results)
        print("\nRésultats finaux:")
        print(result_df)
        return result_df

# Code de test
if __name__ == "__main__":
    # Crée quelques données de test
    product_data = {
        'logID': list(range(1, 11)),
        'prodID': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
        'catID': [5, 5, 5, 5, 10, 10, 5, 5, 5, 10],
        'fabID': [1, 1, 2, 2, 3, 3, 1, 4, 4, 5],
        'dateID': [1, 5, 10, 15, 20, 25, 30, 35, 40, 45]
    }
    
    sale_data = {
        'logID': list(range(1, 11)),
        'prodID': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
        'catID': [5, 5, 5, 5, 10, 10, 5, 5, 5, 10],
        'fabID': [1, 1, 2, 2, 3, 3, 1, 4, 4, 5],
        'magID': [1, 2, 1, 3, 2, 4, 5, 1, 2, 3],
        'dateID': [1, 5, 10, 15, 20, 25, 30, 35, 40, 45]    
    }
    
    product_df = pd.DataFrame(product_data)
    sale_df = pd.DataFrame(sale_data)
    
    processor = DataProcessor(product_df, sale_df)
    
    # Test des calculs de KPI
    print(f"Nombre d'acteurs du marché pour la catégorie 5: {processor.count_market_actors_by_category(5)}")
    print(f"Nombre moyen de produits par fabricant pour la catégorie 5: {processor.avg_products_per_manufacturer_by_category(5)}")
    print(f"Top 3 des magasins:\n{processor.top_stores(3)}")
    
    # Calcule le score de santé du fabricant 1 pour la catégorie 5 et les 3 premiers magasins
    health_score = processor.manufacturer_health_score(1, 5, 3)
    print(f"Score de santé du fabricant 1 pour la catégorie 5 et les 3 premiers magasins: {health_score:.2f}") 