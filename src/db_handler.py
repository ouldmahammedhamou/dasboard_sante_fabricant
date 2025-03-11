"""
Module de gestion des opérations de base de données PostgreSQL
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Dict, Any, Optional, Tuple
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

class PostgresHandler:
    """Classe de gestion de la base de données PostgreSQL"""
    
    def __init__(self, 
                 dbname: str = os.getenv("DB_NAME", "postgres"), 
                 user: str = os.getenv("DB_USER", "postgres"), 
                 password: str = os.getenv("DB_PASSWORD", ""), 
                 host: str = os.getenv("DB_HOST", "localhost"), 
                 port: str = os.getenv("DB_PORT", "5432")):
        """
        Initialise la connexion PostgreSQL, lit la configuration depuis les variables d'environnement
        
        Paramètres:
            dbname: Nom de la base de données, par défaut depuis la variable d'environnement DB_NAME
            user: Nom d'utilisateur, par défaut depuis la variable d'environnement DB_USER
            password: Mot de passe, par défaut depuis la variable d'environnement DB_PASSWORD
            host: Adresse du serveur, par défaut depuis la variable d'environnement DB_HOST
            port: Numéro de port, par défaut depuis la variable d'environnement DB_PORT
        """
        self.conn_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }   
        self.connection = None
        
    def connect(self) -> None:
        """Établit une connexion à la base de données"""
        try:
            self.connection = psycopg2.connect(**self.conn_params)
            print("✓ Connexion à la base de données PostgreSQL réussie")
        except Exception as e:
            print(f"× Echec de la connexion à la base de données: {e}")
            raise
    
    def disconnect(self) -> None:
        """Ferme la connexion à la base de données"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def initialize_database(self) -> None:
        """
        Initialise la base de données, crée les structures de tables nécessaires
        """
        # Se connecter à la base de données
        if not self.connection:
            self.connect()
        
        # Créer les tables si elles n'existent pas
        cursor = self.connection.cursor()
        
        try:
            # Créer la table des produits
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                log_id SERIAL PRIMARY KEY,
                prod_id INTEGER NOT NULL,
                cat_id INTEGER NOT NULL,
                fab_id INTEGER NOT NULL,
                date_id INTEGER NOT NULL,
                date_formatted DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Créer les index pour la table des produits
            cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_products_prod_id') THEN
                    CREATE INDEX idx_products_prod_id ON products(prod_id);
                END IF;
                
                IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_products_cat_id') THEN
                    CREATE INDEX idx_products_cat_id ON products(cat_id);
                END IF;
                
                IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_products_fab_id') THEN
                    CREATE INDEX idx_products_fab_id ON products(fab_id);
                END IF;
                
                IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_products_date_formatted') THEN
                    CREATE INDEX idx_products_date_formatted ON products(date_formatted);
                END IF;
            END
            $$;
            """)
            
            # Créer la table des ventes
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                log_id SERIAL PRIMARY KEY,
                prod_id INTEGER NOT NULL,
                cat_id INTEGER NOT NULL,
                fab_id INTEGER NOT NULL,
                mag_id INTEGER NOT NULL,
                date_id INTEGER NOT NULL,
                date_formatted DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Créer les index pour la table des ventes
            cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_sales_prod_id') THEN
                    CREATE INDEX idx_sales_prod_id ON sales(prod_id);
                END IF;
                
                IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_sales_cat_id') THEN
                    CREATE INDEX idx_sales_cat_id ON sales(cat_id);
                END IF;
                
                IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_sales_fab_id') THEN
                    CREATE INDEX idx_sales_fab_id ON sales(fab_id);
                END IF;
                
                IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_sales_mag_id') THEN
                    CREATE INDEX idx_sales_mag_id ON sales(mag_id);
                END IF;
                
                IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_sales_date_formatted') THEN
                    CREATE INDEX idx_sales_date_formatted ON sales(date_formatted);
                END IF;
            END
            $$;
            """)
            
            self.connection.commit()
            print("✓ Structure de la base de données initialisée avec succès")
        except Exception as e:
            self.connection.rollback()
            print(f"× Erreur lors de l'initialisation de la base de données: {e}")
            raise
        finally:
            cursor.close()
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict]:
        """
        Exécute une requête SQL et retourne les résultats
        
        Paramètres:
            query: Requête SQL à exécuter
            params: Paramètres pour la requête (optionnel)
            
        Retourne:
            Liste de dictionnaires contenant les résultats de la requête
        """
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor()
        result = []
        
        try:
            cursor.execute(query, params)
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                for row in cursor.fetchall():
                    result.append(dict(zip(columns, row)))
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"× Erreur lors de l'exécution de la requête: {e}")
            raise
        finally:
            cursor.close()
            
        return result
    
    def insert_dataframe(self, table_name: str, df: pd.DataFrame, batch_size: int = 1000) -> int:
        """
        Insère un DataFrame dans une table
        
        Paramètres:
            table_name: Nom de la table
            df: DataFrame à insérer
            batch_size: Taille du lot pour les insertions
            
        Retourne:
            Nombre d'enregistrements insérés
        """
        if df.empty:
            return 0
            
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor()
        columns = df.columns.tolist()
        values_template = ','.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({values_template})"
        
        records_total = 0
        
        try:
            # Traitement par lots pour éviter les problèmes de mémoire
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                values = [tuple(row) for row in batch_df.values]
                execute_values(cursor, insert_query, values)
                records_batch = len(batch_df)
                records_total += records_batch
                print(f"✓ Inséré {records_batch} enregistrements dans {table_name} (lot {i//batch_size + 1})")
            
            self.connection.commit()
            print(f"✓ Total: {records_total} enregistrements insérés dans {table_name}")
        except Exception as e:
            self.connection.rollback()
            print(f"× Erreur lors de l'insertion dans {table_name}: {e}")
            raise
        finally:
            cursor.close()
            
        return records_total
    
    def import_api_data(self, fetcher, batch_size: int = 10000, max_records: int = 1000000) -> Tuple[int, int]:
        """
        Importe des données depuis l'API en utilisant le récupérateur de données (fetcher)
        
        Paramètres:
            fetcher: Instance du récupérateur de données (APIClient ou DataFetcher)
            batch_size: Taille du lot pour les requêtes API et les insertions
            max_records: Nombre maximum d'enregistrements à importer
            
        Retourne:
            Tuple (nombre de produits importés, nombre de ventes importées)
        """
        # Initialise la structure de la base de données
        self.initialize_database()
        
        total_products = 0
        total_sales = 0
        
        # Calcule le nombre de lots
        num_batches = (max_records + batch_size - 1) // batch_size
        
        for batch in range(1, num_batches + 1):
            # Calcule les ID de début et de fin pour ce lot
            start_id = (batch - 1) * batch_size + 1
            end_id = min(batch * batch_size, max_records)
            
            print(f"\nLot {batch}/{num_batches}: Traitement des enregistrements {start_id} à {end_id}")
            
            # Récupère et importe les données des produits
            product_logs = fetcher.get_multiple_product_logs(start_id, end_id)
            if product_logs:
                # Importe les données des produits
                imported_count = self.import_products_data(product_logs)
                total_products += imported_count
            
            # Récupère et importe les données des ventes
            sale_logs = fetcher.get_multiple_sale_logs(start_id, end_id)
            if sale_logs:
                # Importe les données des ventes
                imported_count = self.import_sales_data(sale_logs)
                total_sales += imported_count
            
            # Vérifie s'il n'y a plus de données
            if not product_logs and not sale_logs:
                print("Aucune donnée supplémentaire disponible, arrêt de l'importation")
                break
                
        return total_products, total_sales
    
    def count_manufacturers_by_category(self, category_id: int) -> int:
        """
        Compte le nombre de fabricants dans une catégorie spécifique
        
        Paramètres:
            category_id: ID de la catégorie
            
        Retourne:
            Nombre de fabricants dans la catégorie
        """
        query = """
        SELECT COUNT(DISTINCT fab_id) as manufacturer_count
        FROM products
        WHERE cat_id = %s
        """
        result = self.execute_query(query, (category_id,))
        return result[0]['manufacturer_count'] if result else 0
    
    def get_products_dataframe(self) -> pd.DataFrame:
        """
        Récupère tous les produits sous forme de DataFrame
        
        Retourne:
            DataFrame contenant les données des produits
        """
        query = """
        SELECT log_id, prod_id, cat_id, fab_id, date_id, date_formatted
        FROM products
        ORDER BY log_id
        """
        result = self.execute_query(query)
        return pd.DataFrame(result) if result else pd.DataFrame()
    
    def get_sales_dataframe(self) -> pd.DataFrame:
        """
        Récupère tous les accords de vente sous forme de DataFrame
        
        Retourne:
            DataFrame contenant les données des accords de vente
        """
        query = """
        SELECT log_id, prod_id, cat_id, fab_id, mag_id, date_id, date_formatted
        FROM sales
        ORDER BY log_id
        """
        result = self.execute_query(query)
        return pd.DataFrame(result) if result else pd.DataFrame()
    
    def _format_date_from_id(self, date_id: int) -> str:
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
    
    def import_products_data(self, data_list: List[Dict]) -> int:
        """
        Importe une liste de données de produits dans la base de données
        
        Paramètres:
            data_list: Liste de dictionnaires contenant les données des produits
            
        Retourne:
            Nombre d'enregistrements importés
        """
        if not data_list:
            return 0
            
        # Prépare les données pour l'insertion
        insert_data = []
        
        for data in data_list:
            # Valide que toutes les clés nécessaires sont présentes
            required_keys = ['logID', 'prodID', 'catID', 'fabID', 'dateID']
            if not all(key in data for key in required_keys):
                continue
                
            # Formate la date
            date_formatted = self._format_date_from_id(data['dateID'])
            
            # Prépare les données
            record = {
                'log_id': data['logID'],
                'prod_id': data['prodID'],
                'cat_id': data['catID'],
                'fab_id': data['fabID'],
                'date_id': data['dateID'],
                'date_formatted': date_formatted
            }
            
            insert_data.append(record)
        
        if not insert_data:
            return 0
            
        # Convertit en DataFrame pour l'insertion
        df = pd.DataFrame(insert_data)
        
        # Vérifie si les enregistrements existent déjà
        existing_log_ids = []
        
        if not self.connection:
            self.connect()
            
        log_ids = df['log_id'].tolist()
        placeholders = ','.join(['%s'] * len(log_ids))
        
        try:
            query = f"SELECT log_id FROM products WHERE log_id IN ({placeholders})"
            existing_records = self.execute_query(query, tuple(log_ids))
            existing_log_ids = [record['log_id'] for record in existing_records]
        except Exception as e:
            print(f"× Erreur lors de la vérification des enregistrements existants: {e}")
            
        # Filtre les enregistrements qui n'existent pas encore
        if existing_log_ids:
            df = df[~df['log_id'].isin(existing_log_ids)]
            
        if df.empty:
            print("✓ Aucun nouvel enregistrement de produit à importer")
            return 0
            
        # Importe les données
        return self.insert_dataframe('products', df)
    
    def import_sales_data(self, data_list: List[Dict]) -> int:
        """
        Importe une liste de données d'accords de vente dans la base de données
        
        Paramètres:
            data_list: Liste de dictionnaires contenant les données des accords de vente
            
        Retourne:
            Nombre d'enregistrements importés
        """
        if not data_list:
            return 0
            
        # Prépare les données pour l'insertion
        insert_data = []
        
        for data in data_list:
            # Valide que toutes les clés nécessaires sont présentes
            required_keys = ['logID', 'prodID', 'catID', 'fabID', 'magID', 'dateID']
            if not all(key in data for key in required_keys):
                continue
                
            # Formate la date
            date_formatted = self._format_date_from_id(data['dateID'])
            
            # Prépare les données
            record = {
                'log_id': data['logID'],
                'prod_id': data['prodID'],
                'cat_id': data['catID'],
                'fab_id': data['fabID'],
                'mag_id': data['magID'],
                'date_id': data['dateID'],
                'date_formatted': date_formatted
            }
            
            insert_data.append(record)
        
        if not insert_data:
            return 0
            
        # Convertit en DataFrame pour l'insertion
        df = pd.DataFrame(insert_data)
        
        # Vérifie si les enregistrements existent déjà
        existing_log_ids = []
        
        if not self.connection:
            self.connect()
            
        log_ids = df['log_id'].tolist()
        placeholders = ','.join(['%s'] * len(log_ids))
        
        try:
            query = f"SELECT log_id FROM sales WHERE log_id IN ({placeholders})"
            existing_records = self.execute_query(query, tuple(log_ids))
            existing_log_ids = [record['log_id'] for record in existing_records]
        except Exception as e:
            print(f"× Erreur lors de la vérification des enregistrements existants: {e}")
            
        # Filtre les enregistrements qui n'existent pas encore
        if existing_log_ids:
            df = df[~df['log_id'].isin(existing_log_ids)]
            
        if df.empty:
            print("✓ Aucun nouvel enregistrement d'accord de vente à importer")
            return 0
            
        # Importe les données
        return self.insert_dataframe('sales', df)
        
    def get_last_product_id(self) -> Optional[int]:
        """
        Récupère le dernier ID de produit dans la base de données
        
        Retourne:
            Le dernier ID de produit ou None si la table est vide
        """
        query = "SELECT MAX(log_id) FROM products"
        result = self.execute_query(query)
        return result[0]['max'] if result and result[0]['max'] is not None else None

    def get_last_sale_id(self) -> Optional[int]:
        """
        Récupère le dernier ID de vente dans la base de données
        
        Retourne:
            Le dernier ID de vente ou None si la table est vide
        """
        query = "SELECT MAX(log_id) FROM sales"
        result = self.execute_query(query)
        return result[0]['max'] if result and result[0]['max'] is not None else None