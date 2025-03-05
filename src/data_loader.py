"""
Module de chargement de données pour importer des données API dans une base de données PostgreSQL
"""
import argparse
import os
import time
import sys
from typing import List, Dict, Tuple
from dotenv import load_dotenv

# Charger les modules locaux
from db_handler import PostgresHandler
from api_client import APIClient

def load_data_from_api(batch_size: int = 1000, max_records: int = 100000) -> None:
    """
    Charge les données depuis l'API et les stocke dans la base de données
    
    Paramètres:
        batch_size: Nombre d'enregistrements à traiter par lot
        max_records: Nombre maximum d'enregistrements à traiter
    """
    # Initialiser le client API et le gestionnaire de base de données
    api_client = APIClient()
    db_handler = PostgresHandler()
    
    # S'assurer que la structure de la base de données est initialisée
    db_handler.initialize_database()
    
    print(f"Début du chargement des données depuis l'API, {batch_size} enregistrements par lot, maximum {max_records} enregistrements...")
    
    # Calculer le nombre de lots nécessaires
    num_batches = (max_records + batch_size - 1) // batch_size
    
    # Statistiques
    total_products = 0
    total_sales = 0
    start_time = time.time()
    
    for batch in range(1, num_batches + 1):
        batch_start_time = time.time()
        start_id = (batch - 1) * batch_size + 1
        end_id = min(batch * batch_size, max_records)
        
        print(f"\nLot {batch}/{num_batches}: Traitement des enregistrements {start_id} à {end_id}")
        
        # Récupérer les données de produits
        print(f"Récupération des journaux de produits {start_id}-{end_id}...")
        product_logs = api_client.get_multiple_product_logs(start_id, end_id)
        
        if product_logs:
            print(f"Importation de {len(product_logs)} enregistrements de produits dans la base de données...")
            imported_count = db_handler.import_products_data(product_logs)
            total_products += imported_count
            print(f"Importation réussie de {imported_count} enregistrements de produits")
        
        # Récupérer les données de ventes
        print(f"Récupération des journaux d'accords de vente {start_id}-{end_id}...")
        sale_logs = api_client.get_multiple_sale_logs(start_id, end_id)
        
        if sale_logs:
            print(f"Importation de {len(sale_logs)} enregistrements de ventes dans la base de données...")
            imported_count = db_handler.import_sales_data(sale_logs)
            total_sales += imported_count
            print(f"Importation réussie de {imported_count} enregistrements de ventes")
        
        # Vérifier s'il n'y a plus de données
        if not product_logs and not sale_logs:
            print("Plus de données disponibles, arrêt de l'importation")
            break
        
        # Afficher le temps de traitement du lot
        batch_end_time = time.time()
        batch_duration = batch_end_time - batch_start_time
        print(f"Lot {batch} terminé, durée: {batch_duration:.2f} secondes")
        
        # Estimer le temps restant
        if batch > 1:
            avg_time_per_batch = (batch_end_time - start_time) / batch
            remaining_batches = num_batches - batch
            est_remaining_time = avg_time_per_batch * remaining_batches
            print(f"Temps restant estimé: {est_remaining_time:.2f} secondes")
    
    # Afficher le temps de traitement total
    end_time = time.time()
    total_duration = end_time - start_time
    print(f"\nImportation des données terminée, durée totale: {total_duration:.2f} secondes")
    print(f"Total importé: {total_products} enregistrements de produits et {total_sales} enregistrements de ventes")

def load_data_from_files(product_file: str = None, sale_file: str = None, batch_size: int = 10000) -> None:
    """
    Charge les données depuis des fichiers texte et les stocke dans la base de données
    
    Paramètres:
        product_file: Chemin du fichier de données de produits
        sale_file: Chemin du fichier de données de ventes
        batch_size: Nombre d'enregistrements à traiter par lot
    """
    # Initialiser le client API (pour la conversion de données) et le gestionnaire de base de données
    api_client = APIClient()
    db_handler = PostgresHandler()
    
    # S'assurer que la structure de la base de données est initialisée
    db_handler.initialize_database()
    
    print("Début du chargement des données depuis les fichiers...")
    
    # Statistiques
    total_products = 0
    total_sales = 0
    start_time = time.time()
    
    # Traiter le fichier de données de produits
    if product_file and os.path.exists(product_file):
        print(f"Chargement des données de produits depuis le fichier {product_file}...")
        product_logs = api_client.load_data_from_file(product_file, "product")
        
        if product_logs:
            # Traitement par lots
            for i in range(0, len(product_logs), batch_size):
                batch = product_logs[i:i+batch_size]
                print(f"Importation du lot de produits {i//batch_size + 1}/{(len(product_logs)+batch_size-1)//batch_size}, {len(batch)} enregistrements...")
                imported_count = db_handler.import_products_data(batch)
                total_products += imported_count
                print(f"Importation réussie de {imported_count} enregistrements de produits")
    else:
        print(f"Le fichier de données de produits {product_file} n'existe pas ou n'est pas spécifié")
    
    # Traiter le fichier de données de ventes
    if sale_file and os.path.exists(sale_file):
        print(f"Chargement des données de ventes depuis le fichier {sale_file}...")
        sale_logs = api_client.load_data_from_file(sale_file, "sale")
        
        if sale_logs:
            # Traitement par lots
            for i in range(0, len(sale_logs), batch_size):
                batch = sale_logs[i:i+batch_size]
                print(f"Importation du lot de ventes {i//batch_size + 1}/{(len(sale_logs)+batch_size-1)//batch_size}, {len(batch)} enregistrements...")
                imported_count = db_handler.import_sales_data(batch)
                total_sales += imported_count
                print(f"Importation réussie de {imported_count} enregistrements de ventes")
    else:
        print(f"Le fichier de données de ventes {sale_file} n'existe pas ou n'est pas spécifié")
    
    # Afficher le temps de traitement total
    end_time = time.time()
    total_duration = end_time - start_time
    print(f"\nImportation des données terminée, durée totale: {total_duration:.2f} secondes")
    print(f"Total importé: {total_products} enregistrements de produits et {total_sales} enregistrements de ventes")

def main():
    """Fonction principale, analyse les arguments de la ligne de commande et exécute les opérations appropriées"""
    # Charger les variables d'environnement
    load_dotenv()
    
    # Analyser les arguments de la ligne de commande
    parser = argparse.ArgumentParser(description="Importer des données d'analyse de marché dans une base de données PostgreSQL")
    
    # Sélection de la source de données
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--api", action="store_true", help="Récupérer les données depuis l'API")
    source_group.add_argument("--files", action="store_true", help="Récupérer les données depuis des fichiers")
    
    # Paramètres API
    parser.add_argument("--batch-size", type=int, default=1000, help="Nombre d'enregistrements à traiter par lot")
    parser.add_argument("--max-records", type=int, default=100000, help="Nombre maximum d'enregistrements à traiter")
    
    # Paramètres de fichier
    parser.add_argument("--product-file", type=str, help="Chemin du fichier de données de produits")
    parser.add_argument("--sale-file", type=str, help="Chemin du fichier de données de ventes")
    
    # Analyser les arguments
    args = parser.parse_args()
    
    # Exécuter les opérations appropriées selon les arguments
    if args.api:
        load_data_from_api(batch_size=args.batch_size, max_records=args.max_records)
    elif args.files:
        if not args.product_file and not args.sale_file:
            parser.error("Lors de l'utilisation de l'option --files, vous devez spécifier au moins un des fichiers --product-file ou --sale-file")
        load_data_from_files(
            product_file=args.product_file,
            sale_file=args.sale_file,
            batch_size=args.batch_size
        )

if __name__ == "__main__":
    main() 