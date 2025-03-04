"""
Module d'acquisition de données - Responsable de la récupération des données sur les produits et les accords de vente depuis l'API
"""
import requests
import json
import pandas as pd
import os
import time
from typing import Dict, Any, Optional, List, Union

class DataFetcher:
    """Classe pour récupérer des données depuis l'API distante"""
    
    def __init__(self, base_url: str = "http://51.255.166.155:1353"):
        """
        Initialise le récupérateur de données
        
        Paramètres:
            base_url: URL de base de l'API
        """
        self.base_url = base_url
        
    def get_product_log(self, log_id: int) -> Dict[str, Any]:
        """
        Récupère les données du journal des produits
        
        Paramètres:
            log_id: ID du journal
            
        Retourne:
        Dictionnaire contenant les données du journal
        """
        url = f"{self.base_url}/logProduits/{log_id}/"
        response = requests.get(url)
        return response.json()
    
    def get_sale_agreement_log(self, log_id: int) -> Dict[str, Any]:
        """
        Récupère les données du journal des accords de vente
        
        Paramètres:
            log_id: ID du journal
            
        Retourne:
            Dictionnaire contenant les données du journal
        """
        url = f"{self.base_url}/logAccordsVente/{log_id}/"
        response = requests.get(url)
        return response.json()
    
    def get_multiple_product_logs(self, start_id: int, end_id: int) -> List[Dict[str, Any]]:
        """
        Récupère plusieurs journaux de produits
        
        Paramètres:
            start_id: ID de journal de départ
            end_id: ID de journal de fin
            
        Retourne:
            Liste contenant tous les journaux récupérés
        """
        logs = []
        for log_id in range(start_id, end_id + 1):
            log_data = self.get_product_log(log_id)
            # Vérifie si le journal a été trouvé
            if 'NOT FOUND' not in log_data:
                logs.append(log_data)
        return logs
    
    def get_multiple_sale_logs(self, start_id: int, end_id: int) -> List[Dict[str, Any]]:
        """
        Récupère plusieurs journaux d'accords de vente
        
        Paramètres:
            start_id: ID de journal de départ
            end_id: ID de journal de fin
            
        Retourne:
            Liste contenant tous les journaux récupérés
        """
        logs = []
        for log_id in range(start_id, end_id + 1):
            log_data = self.get_sale_agreement_log(log_id)
            # Vérifie si le journal a été trouvé
            if 'NOT FOUND' not in log_data:
                logs.append(log_data)
        return logs
    
    def save_logs_to_file(self, logs: List[Dict[str, Any]], file_path: str) -> None:
        """
        Enregistre les journaux dans un fichier
        
        Paramètres:
            logs: Liste des journaux
            file_path: Chemin du fichier où enregistrer
        """
        with open(file_path, 'w') as f:
            json.dump(logs, f, indent=4)
    
    def load_logs_from_file(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Charge des journaux depuis un fichier
        
        Paramètres:
            file_path: Chemin du fichier
            
        Retourne:
            Liste des journaux
        """
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def convert_logs_to_dataframe(self, logs: List[Dict[str, Any]], log_type: str) -> pd.DataFrame:
        """
        Convertit les journaux en DataFrame
        
        Paramètres:
            logs: Liste des journaux
            log_type: Type de journal ('product' ou 'sale')
            
        Retourne:
            DataFrame contenant les données des journaux
        """
        if not logs:
            # Retourner un DataFrame vide avec les colonnes appropriées
            if log_type == 'product':
                return pd.DataFrame(columns=['logID', 'prodID', 'catID', 'fabID', 'dateID'])
            elif log_type == 'sale':
                return pd.DataFrame(columns=['logID', 'prodID', 'catID', 'fabID', 'magID', 'dateID'])
            else:
                raise ValueError("log_type doit être 'product' ou 'sale'")
        
        # Convertir la liste de dictionnaires en DataFrame
        df = pd.DataFrame(logs)
        
        # Renommer les colonnes si nécessaire pour assurer la cohérence
        column_mapping = {}
        if log_type == 'product':
            expected_columns = ['logID', 'prodID', 'catID', 'fabID', 'dateID']
            # Mappages potentiels basés sur l'API
            if 'id' in df.columns:
                column_mapping['id'] = 'logID'
            if 'product_id' in df.columns:
                column_mapping['product_id'] = 'prodID'
            if 'category_id' in df.columns:
                column_mapping['category_id'] = 'catID'
            if 'manufacturer_id' in df.columns:
                column_mapping['manufacturer_id'] = 'fabID'
            if 'date' in df.columns:
                column_mapping['date'] = 'dateID'
        elif log_type == 'sale':
            expected_columns = ['logID', 'prodID', 'catID', 'fabID', 'magID', 'dateID']
            # Mappages potentiels basés sur l'API
            if 'id' in df.columns:
                column_mapping['id'] = 'logID'
            if 'product_id' in df.columns:
                column_mapping['product_id'] = 'prodID'
            if 'category_id' in df.columns:
                column_mapping['category_id'] = 'catID'
            if 'manufacturer_id' in df.columns:
                column_mapping['manufacturer_id'] = 'fabID'
            if 'store_id' in df.columns:
                column_mapping['store_id'] = 'magID'
            if 'date' in df.columns:
                column_mapping['date'] = 'dateID'
        
        # Appliquer les mappages de colonnes
        if column_mapping:
            df = df.rename(columns=column_mapping)
        
        # S'assurer que toutes les colonnes attendues sont présentes
        expected_columns = ['logID', 'prodID', 'catID', 'fabID', 'dateID'] if log_type == 'product' else ['logID', 'prodID', 'catID', 'fabID', 'magID', 'dateID']
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
        
        # S'assurer que le DataFrame a les colonnes dans le bon ordre
        return df[expected_columns]
        
    def load_test_data_from_text_file(self, file_path: str, data_type: str) -> pd.DataFrame:
        """
        Charge les données de test à partir d'un fichier texte
        
        Paramètres:
            file_path: Chemin du fichier texte
            data_type: Type de données ('product' ou 'sale')
            
        Retourne:
            DataFrame contenant les données chargées
        """
        if data_type == 'product':
            # Format: date prodID catID fabID
            columns = ['dateID', 'prodID', 'catID', 'fabID']
            df = pd.read_csv(file_path, sep=' ', header=None, names=columns)
            # Ajoute un logID séquentiel
            df['logID'] = range(1, len(df) + 1)
            # Réorganise les colonnes pour correspondre à l'ordre standard
            df = df[['logID', 'prodID', 'catID', 'fabID', 'dateID']]
        elif data_type == 'sale':
            # Format: date prodID catID fabID magID
            columns = ['dateID', 'prodID', 'catID', 'fabID', 'magID']
            df = pd.read_csv(file_path, sep=' ', header=None, names=columns)
            # Ajoute un logID séquentiel
            df['logID'] = range(1, len(df) + 1)
            # Réorganise les colonnes pour correspondre à l'ordre standard
            df = df[['logID', 'prodID', 'catID', 'fabID', 'magID', 'dateID']]
        else:
            raise ValueError(f"Type de données inconnu: {data_type}")
            
        # Traitement des dates au format YYYYMMDD
        if not df.empty and 'dateID' in df.columns:
            # Convertir dateID en chaîne si ce n'est pas déjà le cas
            df['dateID'] = df['dateID'].astype(str)
            
            # Vérifier si les dateID sont au format YYYYMMDD (8 chiffres)
            if df['dateID'].str.len().max() == 8:
                try:
                    # Ajouter directement la colonne date et month pour les analyses temporelles
                    df['date'] = pd.to_datetime(df['dateID'], format='%Y%m%d')
                    df['month'] = df['date'].dt.month
                except Exception as e:
                    print(f"Erreur lors de la conversion des dates: {e}")
        
        return df
    
    def save_data_to_cache(self, data: pd.DataFrame, cache_file: str) -> None:
        """
        Sauvegarde les données DataFrame dans un fichier cache
        
        Paramètres:
            data: Le DataFrame à mettre en cache
            cache_file: Chemin du fichier cache
        """
        # S'assurer que le répertoire de cache existe
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        # Sauvegarde au format CSV
        data.to_csv(cache_file, index=False)
    
    def load_data_from_cache(self, cache_file: str) -> Optional[pd.DataFrame]:
        """
        Charge les données depuis un fichier cache
        
        Paramètres:
            cache_file: Chemin du fichier cache
        
        Retourne:
            DataFrame ou None si le cache n'existe pas ou est expiré
        """
        if os.path.exists(cache_file):
            # Vérifier si le fichier cache est expiré (24 heures)
            file_mtime = os.path.getmtime(cache_file)
            if (time.time() - file_mtime) < 24*3600:  # Cache de 24 heures
                try:
                    return pd.read_csv(cache_file)
                except Exception:
                    # Si la lecture échoue, retourner None
                    return None
        return None

# Code de test
if __name__ == "__main__":
    fetcher = DataFetcher()
    
    # Test de récupération d'un journal de produit
    product_log = fetcher.get_product_log(1664)
    print("Exemple de journal de produit:", product_log)
    
    # Test de récupération d'un journal d'accord de vente
    sale_log = fetcher.get_sale_agreement_log(1664)
    print("Exemple de journal d'accord de vente:", sale_log) 