"""
Module de traitement des données - Responsable de l'analyse des données et du calcul des KPI
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime
import random

# Définition des périodes de soldes
SOLDES_HIVER = {
    'debut': pd.to_datetime('2022-01-12'),  # 12 janvier 2022
    'fin': pd.to_datetime('2022-02-08')     # 8 février 2022
}

SOLDES_ETE = {
    'debut': pd.to_datetime('2022-06-22'),  # 22 juin 2022
    'fin': pd.to_datetime('2022-07-19')     # 19 juillet 2022
}

class DataProcessor:
    """Classe pour traiter les données de produits et d'accords de vente et calculer les KPI"""
    
    # Définition des périodes comme variables de classe
    SOLDES_HIVER = {
        'debut': pd.to_datetime('2022-01-12'),
        'fin': pd.to_datetime('2022-02-08')
    }
    
    SOLDES_ETE = {
        'debut': pd.to_datetime('2022-06-22'),
        'fin': pd.to_datetime('2022-07-19')
    }
    
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
    
    def count_market_actors_by_category(self, category_id: int) -> int:
        """
        Calcule le nombre d'acteurs du marché pour une catégorie spécifique
        
        Paramètres:
            category_id: ID de la catégorie de produit
            
        Retourne:
            Nombre d'acteurs
        """
        if self.product_df is None:
            raise ValueError("DataFrame de produits non défini")
        
        # Filtre les produits de la catégorie spécifique
        category_products = self.product_df[self.product_df['cat_id'] == category_id]
        
        # Calcule le nombre de fabricants distincts
        return category_products['fab_id'].nunique()
    
    def avg_products_per_manufacturer_by_category(self, category_id: int) -> float:
        """
        Calcule le nombre moyen de produits par fabricant pour une catégorie spécifique
        
        Paramètres:
            category_id: ID de la catégorie de produit
            
        Retourne:
            Nombre moyen de produits
        """
        if self.product_df is None:
            raise ValueError("DataFrame de produits non défini")
        
        # Filtre les produits de la catégorie spécifique
        category_products = self.product_df[self.product_df['cat_id'] == category_id]
        
        if category_products.empty:
            return 0.0
            
        # Pour chaque fabricant, calcule le nombre de produits
        products_per_manufacturer = category_products.groupby('fab_id').size()
        
        # Si aucun fabricant n'est trouvé
        if len(products_per_manufacturer) == 0:
            return 0.0
            
        # Calcule la moyenne
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
    
    def manufacturer_health_score(self, manufacturer_id: int, category_id: int, top_n_stores: int = 10) -> float:
        """
        Calcule le score de santé d'un fabricant pour une catégorie spécifique et les N premiers magasins
        
        Paramètres:
            manufacturer_id: ID du fabricant
            category_id: ID de la catégorie de produit
            top_n_stores: Nombre des premiers magasins à considérer
            
        Retourne:
            Score de santé (proportion moyenne de produits du fabricant parmi tous les produits)
        """
        if self.sale_df is None or self.product_df is None:
            return 0.0
            
        # Vérifier si le fabricant a des produits dans cette catégorie
        manufacturer_products = self.product_df[
            (self.product_df['cat_id'] == category_id) & 
            (self.product_df['fab_id'] == manufacturer_id)
        ]
        
        if len(manufacturer_products) == 0:
            return 0.0
            
        # Obtient les N premiers magasins basés sur le nombre total d'accords de vente
        top_stores_df = self.top_stores(top_n_stores)
        top_store_ids = top_stores_df['mag_id'].tolist()
        
        # Filtre les accords de vente pour la catégorie spécifique et ces magasins
        category_sales = self.sale_df[
            (self.sale_df['cat_id'] == category_id) & 
            (self.sale_df['mag_id'].isin(top_store_ids))
        ]
        
        if len(category_sales) == 0:
            return 0.0
        
        # Calcule le score pour chaque magasin
        store_scores = []
        for store_id in top_store_ids:
            store_sales = category_sales[category_sales['mag_id'] == store_id]
            
            if len(store_sales) == 0:
                store_scores.append(0.0)
                continue
                
            # Nombre total de produits uniques dans ce magasin pour cette catégorie
            total_products = store_sales['prod_id'].nunique()
            
            # Nombre de produits uniques de ce fabricant dans ce magasin pour cette catégorie
            manufacturer_products = store_sales[
                store_sales['fab_id'] == manufacturer_id
            ]['prod_id'].nunique()
            
            # Calcule le score pour ce magasin
            store_score = manufacturer_products / total_products if total_products > 0 else 0.0
            store_scores.append(store_score)
        
        # Retourne la moyenne des scores de tous les magasins
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
    
    def manufacturer_share_in_category(self, manufacturer_id: int, category_id: int) -> float:
        """
        Calcule la part de marché d'un fabricant dans une catégorie spécifique
        
        Paramètres:
            manufacturer_id: ID du fabricant
            category_id: ID de la catégorie de produit
            
        Retourne:
            Proportion des produits du fabricant parmi tous les produits de la catégorie
        """
        if self.product_df is None:
            return 0.0
            
        # Filtrer les produits de la catégorie
        category_products = self.product_df[self.product_df['cat_id'] == category_id]
        
        if category_products.empty:
            return 0.0
            
        # Compter les produits totaux et les produits du fabricant
        total_products = category_products['prod_id'].nunique()
        manufacturer_products = category_products[
            category_products['fab_id'] == manufacturer_id
        ]['prod_id'].nunique()
        
        # Calculer la proportion
        return manufacturer_products / total_products if total_products > 0 else 0.0
    
    def manufacturer_products_in_category(self, manufacturer_id: int, category_id: int) -> int:
        """
        Calcule le nombre de produits d'un fabricant dans une catégorie spécifique
        
        Paramètres:
            manufacturer_id: ID du fabricant
            category_id: ID de la catégorie de produit
            
        Retourne:
            Nombre de produits
        """
        if self.product_df is None:
            raise ValueError("DataFrame de produits non défini")
        
        # Filtre les produits du fabricant dans la catégorie spécifique
        products = self.product_df[
            (self.product_df['cat_id'] == category_id) & 
            (self.product_df['fab_id'] == manufacturer_id)
        ]
        
        return len(products)

    def avg_products_per_manufacturer_by_category_soldes(self, category_id: int, periode: str = 'hiver') -> float:
        """
        Calcule le nombre moyen de produits par fabricant pendant les soldes
        
        Paramètres:
            category_id: ID de la catégorie
            periode: 'hiver' ou 'ete'
        """
        if self.product_df is None:
            return 0.0
        
        # Définir la période de soldes
        if periode == 'hiver':
            debut = self.SOLDES_HIVER['debut']
            fin = self.SOLDES_HIVER['fin']
        else:
            debut = self.SOLDES_ETE['debut']
            fin = self.SOLDES_ETE['fin']
        
        # Filtrer les produits pour la période de soldes
        soldes_products = self.product_df[
            (self.product_df['date_formatted'] >= debut) &
            (self.product_df['date_formatted'] <= fin) &
            (self.product_df['cat_id'] == category_id)
        ]
        
        if soldes_products.empty:
            return 0.0
        
        # Calculer la moyenne
        products_per_manufacturer = soldes_products.groupby('fab_id')['prod_id'].nunique()
        return products_per_manufacturer.mean()

    def top_stores_soldes(self, n: int = 10, periode: str = 'both') -> pd.DataFrame:
        """
        Identifie les N premiers magasins pendant les soldes
        
        Paramètres:
            n: Nombre de magasins à retourner
            periode: 'hiver', 'ete' ou 'both' pour les deux
        """
        if self.sale_df is None:
            return pd.DataFrame()
        
        # Filtrer les ventes selon la période
        if periode == 'hiver':
            soldes_sales = self.sale_df[
                (self.sale_df['date_formatted'] >= self.SOLDES_HIVER['debut']) &
                (self.sale_df['date_formatted'] <= self.SOLDES_HIVER['fin'])
            ]
        elif periode == 'ete':
            soldes_sales = self.sale_df[
                (self.sale_df['date_formatted'] >= self.SOLDES_ETE['debut']) &
                (self.sale_df['date_formatted'] <= self.SOLDES_ETE['fin'])
            ]
        else:  # both
            soldes_sales = self.sale_df[
                ((self.sale_df['date_formatted'] >= self.SOLDES_HIVER['debut']) &
                 (self.sale_df['date_formatted'] <= self.SOLDES_HIVER['fin'])) |
                ((self.sale_df['date_formatted'] >= self.SOLDES_ETE['debut']) &
                 (self.sale_df['date_formatted'] <= self.SOLDES_ETE['fin']))
            ]
        
        # Calculer le nombre d'accords par magasin
        store_counts = soldes_sales['mag_id'].value_counts().reset_index()
        store_counts.columns = ['mag_id', 'agreement_count']
        
        return store_counts.head(n)

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