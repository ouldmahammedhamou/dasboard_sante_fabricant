"""
Module client API pour récupérer des données d'analyse de marché depuis l'API distante.
"""
import requests
import json
import pandas as pd
from typing import Dict, List, Any, Optional, Union
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

class APIClient:
    """Classe client API pour interagir avec l'API d'analyse de marché"""

    def __init__(self, base_url: str = 'http://51.255.166.155:1353'):
        """
        Initialise le client API
        
        Paramètres:
            base_url: URL de base de l'API
        """
        self.base_url = base_url
        self.session = requests.Session()
        self.endpoints = {
            'product': '/logProduits',
            'sale': '/logAccordsVente',
        }
        
    def get_product_log(self, log_id: int) -> Optional[Dict[str, Any]]:
        """
        Récupère un seul journal de produit
        
        Paramètres:
            log_id: ID du journal à récupérer
            
        Retourne:
            Dictionnaire contenant les données du journal ou None en cas d'échec
        """
        try:
            response = self.session.get(f"{self.base_url}{self.endpoints['product']}/{log_id}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la récupération du journal produit {log_id}: {e}")
            return None
        except ValueError:
            print(f"Erreur de décodage JSON pour le journal produit {log_id}")
            return None
            
    def get_sale_log(self, log_id: int) -> Optional[Dict[str, Any]]:
        """
        Récupère un seul journal d'accord de vente
        
        Paramètres:
            log_id: ID du journal à récupérer
            
        Retourne:
            Dictionnaire contenant les données du journal ou None en cas d'échec
        """
        try:
            response = self.session.get(f"{self.base_url}{self.endpoints['sale']}/{log_id}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la récupération du journal de vente {log_id}: {e}")
            return None
        except ValueError:
            print(f"Erreur de décodage JSON pour le journal de vente {log_id}")
            return None
            
    def get_multiple_product_logs(self, start_id: int, end_id: int) -> List[Dict[str, Any]]:
        """
        Récupère plusieurs journaux de produits dans une plage d'ID
        
        Paramètres:
            start_id: ID de début
            end_id: ID de fin
            
        Retourne:
            Liste de dictionnaires contenant les données des journaux
        """
        results = []
        for log_id in range(start_id, end_id + 1):
            # Attente courte pour éviter de surcharger l'API
            time.sleep(random.uniform(0.05, 0.1))
            
            log = self.get_product_log(log_id)
            if log:
                results.append(log)
                
            # Message de progression tous les 1000 enregistrements
            if log_id % 1000 == 0:
                print(f"Récupération des journaux de produits: {log_id - start_id + 1}/{end_id - start_id + 1} traités")
                
        return results
        
    def get_multiple_sale_logs(self, start_id: int, end_id: int) -> List[Dict[str, Any]]:
        """
        Récupère plusieurs journaux d'accords de vente dans une plage d'ID
        
        Parametres:
            start_id: ID de début
            end_id: ID de fin
            
        Retourne:
            Liste de dictionnaires contenant les données des journaux
        """
        results = []
        for log_id in range(start_id, end_id + 1):
            # Attente courte pour éviter de surcharger l'API
            time.sleep(random.uniform(0.05, 0.1))
            
            log = self.get_sale_log(log_id)
            if log:
                results.append(log)
                
            # Message de progression tous les 1000 enregistrements
            if log_id % 1000 == 0:
                print(f"Récupération des journaux de vente: {log_id - start_id + 1}/{end_id - start_id + 1} traités")
                
        return results
    
    def convert_logs_to_dataframe(self, logs: List[Dict[str, Any]], log_type: str) -> pd.DataFrame:
        """
        Convertit une liste de journaux en DataFrame
        
        Paramètres:
            logs: Liste de dictionnaires contenant les données des journaux
            log_type: Type de journal ('product' ou 'sale')
            
        Retourne:
            DataFrame avec les données des journaux
        """
        if not logs:
            print(f"Aucun journal de type {log_type} à convertir")
            return pd.DataFrame()
            
        try:
            df = pd.DataFrame(logs)
            
            # Renomme les colonnes pour correspondre au schéma de la base de données
            column_mapping = {
                'logID': 'log_id',
                'prodID': 'prod_id',
                'catID': 'cat_id',
                'fabID': 'fab_id',
                'magID': 'mag_id' if log_type == 'sale' else None,
                'dateID': 'date_id'
            }
            
            # Supprime les mappages None
            column_mapping = {k: v for k, v in column_mapping.items() if v is not None}
            
            # Renomme les colonnes
            df = df.rename(columns=column_mapping)
            
            # Ajoute la colonne date_formatted
            df['date_formatted'] = df['date_id'].apply(self._format_date)
            
            return df
            
        except Exception as e:
            print(f"Erreur lors de la conversion des journaux en DataFrame: {e}")
            return pd.DataFrame()
    
    def _format_date(self, date_id: int) -> Optional[str]:
        """
        Formate l'ID de date en format de date ISO
        
        Paramètres:
            date_id: ID de date au format YYYYMMDD (par exemple 20220101)
            
        Retourne:
            Chaîne de date formatée YYYY-MM-DD
        """
        date_str = str(date_id)
        if len(date_str) != 8:
            return None
            
        try:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            return f"{year}-{month:02d}-{day:02d}"
        except ValueError:
            return None
    
    def load_data_from_file(self, file_path: str, log_type: str = "product") -> List[Dict]:
        """
        Charge les données de journaux depuis un fichier texte
        
        Paramètres:
            file_path: Chemin du fichier
            log_type: Type de journal, 'product' ou 'sale'
            
        Retourne:
            Liste de données de journaux
        """
        logs = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                        
                    try:
                        log_data = json.loads(line)
                        if isinstance(log_data, dict):
                            # Valider les champs requis
                            if log_type == "product" and all(k in log_data for k in ['logID', 'prodID', 'catID', 'fabID', 'dateID']):
                                logs.append(log_data)
                            elif log_type == "sale" and all(k in log_data for k in ['logID', 'prodID', 'catID', 'fabID', 'magID', 'dateID']):
                                logs.append(log_data)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            print(f"Erreur lors du chargement des données depuis le fichier {file_path}: {e}")
            
        print(f"Chargé {len(logs)} journaux de type {log_type} depuis le fichier {file_path}")
        return logs 