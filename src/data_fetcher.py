"""
Module d'acquisition de données - Responsable de la récupération des données sur les produits et les accords de vente depuis l'API
"""
import requests
import json
import pandas as pd
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
        if log_type == 'product':
            columns = ['logID', 'prodID', 'catID', 'fabID', 'dateID']
        elif log_type == 'sale':
            columns = ['logID', 'prodID', 'catID', 'fabID', 'magID', 'dateID']
        else:
            raise ValueError("log_type doit être 'product' ou 'sale'")
        
        return pd.DataFrame(logs, columns=columns)

# Code de test
if __name__ == "__main__":
    fetcher = DataFetcher()
    
    # Test de récupération d'un journal de produit
    product_log = fetcher.get_product_log(1664)
    print("Exemple de journal de produit:", product_log)
    
    # Test de récupération d'un journal d'accord de vente
    sale_log = fetcher.get_sale_agreement_log(1664)
    print("Exemple de journal d'accord de vente:", sale_log) 