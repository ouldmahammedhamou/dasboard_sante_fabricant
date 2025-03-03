"""
Module de traitement des données - Responsable de l'analyse des données et du calcul des KPI
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime
import random

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
        self._update_data_cache()
    
    def _update_data_cache(self) -> None:
        """Met à jour le cache de données interne avec des calculs préliminaires"""
        # Pré-calcul des données fréquemment utilisées
        if self.product_df is not None and not self.product_df.empty:
            self.product_categories = sorted(self.product_df['catID'].unique())
            self.manufacturers = sorted(self.product_df['fabID'].unique())
            
        if self.sale_df is not None and not self.sale_df.empty:
            self.stores = sorted(self.sale_df['magID'].unique())
    
    def get_date_from_id(self, date_id: int) -> datetime:
        """
        Convertit un ID de date en objet datetime (implémentation d'exemple, à ajuster selon le format réel de l'ID de date)
        
        Paramètres:
            date_id: ID de date
            
        Retourne:
            Objet datetime
        """
        # Supposons que l'ID de date est un décalage en jours à partir d'une date spécifique
        # Dans une application réelle, cela doit être ajusté selon le format réel de l'ID de date
        base_date = datetime(2022, 1, 1)  # Supposons que la date de référence est le 1er janvier 2022
        days_offset = date_id - 1  # Supposons que l'ID commence à 1
        return base_date + pd.Timedelta(days=days_offset)
    
    def add_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ajoute une colonne 'date' au DataFrame à partir de la colonne 'dateID'
        
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
        
        # Si dateID est au format YYYYMMDD (ex: 20220101)
        if 'dateID' in result_df.columns and isinstance(result_df['dateID'].iloc[0], (str, int)) and str(result_df['dateID'].iloc[0]).isdigit():
            try:
                date_strings = result_df['dateID'].astype(str)
                
                # Vérifier si nous avons un format YYYYMMDD (8 chiffres)
                if date_strings.str.len().max() == 8:
                    result_df['date'] = pd.to_datetime(date_strings, format='%Y%m%d')
                    result_df['month'] = result_df['date'].dt.month
                    return result_df
            except Exception as e:
                print(f"Erreur lors de la conversion du format YYYYMMDD: {e}")
        
        # Si dateID est une chaîne de caractères au format YYYY-MM-DD
        if 'dateID' in result_df.columns and isinstance(result_df['dateID'].iloc[0], str):
            try:
                result_df['date'] = pd.to_datetime(result_df['dateID'])
                result_df['month'] = result_df['date'].dt.month
                return result_df
            except Exception as e:
                print(f"Erreur lors de la conversion de dateID en date: {e}")
        
        # Si dateID est un entier représentant un jour de l'année
        if 'dateID' in result_df.columns and (isinstance(result_df['dateID'].iloc[0], int) or 
                                             isinstance(result_df['dateID'].iloc[0], float)):
            try:
                # Convertir les dateID (jour de l'année) en dates complètes pour 2022
                # Exemple: dateID=1 -> 2022-01-01, dateID=32 -> 2022-02-01
                base_date = datetime(2022, 1, 1)
                result_df['date'] = result_df['dateID'].apply(
                    lambda x: base_date + pd.Timedelta(days=int(x)-1)
                )
                result_df['month'] = result_df['date'].dt.month
                return result_df
            except Exception as e:
                print(f"Erreur lors de la conversion de dateID (entier) en date: {e}")
                
        # Si aucune conversion n'a fonctionné, retourner le DataFrame original
        print("Avertissement: Impossible d'ajouter une colonne de date.")
        return df
    
    def count_market_actors_by_category(self, category_id: int) -> int:
        """
        Calcule le nombre d'acteurs du marché pour une catégorie spécifique (Question 1.1)
        
        Paramètres:
            category_id: ID de la catégorie de produit
            
        Retourne:
            Nombre d'acteurs
        """
        if self.product_df is None:
            raise ValueError("DataFrame de produits non défini")
        
        # Filtre les produits de la catégorie spécifique
        category_products = self.product_df[self.product_df['catID'] == category_id]
        
        # Calcule le nombre de fabricants distincts
        return category_products['fabID'].nunique()
    
    def avg_products_per_manufacturer_by_category(self, category_id: int) -> float:
        """
        Calcule le nombre moyen de produits par fabricant pour une catégorie spécifique (Question 1.2)
        
        Paramètres:
            category_id: ID de la catégorie de produit
            
        Retourne:
            Nombre moyen de produits
        """
        if self.product_df is None:
            raise ValueError("DataFrame de produits non défini")
        
        # Filtre les produits de la catégorie spécifique
        category_products = self.product_df[self.product_df['catID'] == category_id]
        
        if category_products.empty:
            return 0.0
            
        # Pour chaque fabricant, calcule le nombre de produits
        products_per_manufacturer = category_products.groupby('fabID').size()
        
        # Si aucun fabricant n'est trouvé
        if len(products_per_manufacturer) == 0:
            return 0.0
            
        # Calcule la moyenne
        return products_per_manufacturer.mean()
    
    def top_stores(self, n: int = 10) -> pd.DataFrame:
        """
        Identifie les N premiers magasins (Question 1.3)
        Ici, nous définissons "top N magasins" comme ceux ayant le plus grand nombre d'accords de vente
        
        Paramètres:
            n: Nombre de magasins à retourner
            
        Retourne:
            DataFrame contenant les N premiers magasins
        """
        if self.sale_df is None:
            raise ValueError("DataFrame d'accords de vente non défini")
        
        # Calcule le nombre d'accords de vente pour chaque magasin
        store_counts = self.sale_df['magID'].value_counts().reset_index()
        store_counts.columns = ['magID', 'agreement_count']
        
        # Retourne les N premiers magasins
        return store_counts.head(n)
    
    def manufacturer_health_score(self, manufacturer_id: int, category_id: int, top_n_stores: int = 10) -> float:
        """
        Calcule le score de santé d'un fabricant pour une catégorie spécifique et les N premiers magasins (Question 1.4)
        
        Paramètres:
            manufacturer_id: ID du fabricant
            category_id: ID de la catégorie de produit
            top_n_stores: Nombre des premiers magasins à considérer
            
        Retourne:
            Score de santé (proportion moyenne de produits du fabricant parmi tous les produits)
        """
        if self.sale_df is None:
            raise ValueError("DataFrame d'accords de vente non défini")
        
        # Obtient les N premiers magasins
        top_stores_df = self.top_stores(top_n_stores)
        top_store_ids = top_stores_df['magID'].tolist()
        
        # Filtre les accords de vente pour la catégorie spécifique et ces magasins
        category_sales = self.sale_df[
            (self.sale_df['catID'] == category_id) & 
            (self.sale_df['magID'].isin(top_store_ids))
        ]
        
        # Calcule le nombre de produits de ce fabricant et le nombre total de produits dans chaque magasin
        store_stats = []
        for store_id in top_store_ids:
            store_sales = category_sales[category_sales['magID'] == store_id]
            
            # Nombre total de produits de cette catégorie dans ce magasin
            total_products = store_sales['prodID'].nunique()
            
            # Nombre de produits de ce fabricant pour cette catégorie dans ce magasin
            manufacturer_products = store_sales[
                store_sales['fabID'] == manufacturer_id
            ]['prodID'].nunique()
            
            # Calcule la proportion
            ratio = manufacturer_products / total_products if total_products > 0 else 0
            store_stats.append(ratio)
        
        # Calcule la proportion moyenne
        return np.mean(store_stats) if store_stats else 0
    
    def market_actors_over_time(self, category_id: int, start_date: datetime, end_date: datetime, 
                               freq: str = 'M') -> pd.DataFrame:
        """
        Calcule l'évolution du nombre d'acteurs du marché pour une catégorie spécifique sur une période (Question 2.1)
        
        Paramètres:
            category_id: ID de la catégorie de produit
            start_date: Date de début
            end_date: Date de fin
            freq: Fréquence temporelle ('M' pour mensuel, 'W' pour hebdomadaire, etc.)
            
        Retourne:
            DataFrame contenant le temps et le nombre d'acteurs
        """
        if self.product_df is None:
            raise ValueError("DataFrame de produits non défini")
        
        # Ajoute une colonne de date
        df_with_dates = self.add_date_column(self.product_df)
        
        # Filtre les produits pour la catégorie spécifique et la plage de temps
        filtered_df = df_with_dates[
            (df_with_dates['catID'] == category_id) & 
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
            
            actor_count = period_data['fabID'].nunique()
            
            results.append({
                'period_start': period_start,
                'period_end': period_end,
                'actor_count': actor_count
            })
        
        return pd.DataFrame(results)

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