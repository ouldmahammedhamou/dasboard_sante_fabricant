"""
Point d'entrée principal - Lance l'application de tableau de bord
"""
import os
import sys

# Ajoute le répertoire src au chemin Python
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

def main():
    """
    Fonction principale - Exécute l'application de tableau de bord
    """
    print("Démarrage du tableau de bord de santé des fabricants sur le marché...")
    print("Utilisant la commande: streamlit run src/dashboard.py")
    os.system("streamlit run src/dashboard.py")

if __name__ == "__main__":
    main() 