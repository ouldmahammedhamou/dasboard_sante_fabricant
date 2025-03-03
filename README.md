# Tableau de Bord de Santé des Fabricants sur le Marché

Ce projet vise à analyser les indicateurs de performance clés (KPI) liés à la santé des fabricants de produits de consommation sur leur marché. Le tableau de bord affiche la performance et le score de santé d'un fabricant dans diverses catégories de produits en fonction de l'ID du fabricant saisi.

## Contexte du Projet

Ce tableau de bord analyse les indicateurs de leadership du marché des fabricants sur une période d'environ un an à partir du 1er janvier 2022. Il répond principalement aux questions suivantes :

1. Nombre d'acteurs sur le marché pour une catégorie spécifique
2. Nombre moyen de produits qu'un fabricant propose dans une catégorie spécifique
3. Identification et analyse des 10 premiers magasins
4. Score de santé d'un fabricant dans une catégorie spécifique et dans les 10 premiers magasins

## Source des Données

Les données proviennent d'une API distante : http://51.255.166.155:1353/, qui fournit deux points d'accès :
- `/logProduits/<int:logID>/` : Données des journaux de produits
- `/logAccordsVente/<int:logID>/` : Données des journaux d'accords de vente

## Caractéristiques

- Tableau de bord interactif permettant de sélectionner l'ID du fabricant, la catégorie de produit et la plage de dates
- Visualisation des indicateurs de performance clés
- Graphiques d'analyse des tendances temporelles
- Classement et analyse des 10 premiers magasins
- Fonction d'exploration des données brutes

## Instructions d'Installation

1. Cloner le dépôt du projet
```
git clone <url-du-dépôt>
cd dashboard_sante_fabricant
```

2. Installer les dépendances
```
pip install -r requirements.txt
```

3. Exécuter l'application
```
python main.py
```
Ou lancer directement l'application Streamlit :
```
streamlit run src/dashboard.py
```

## Structure du Projet

```
dashboard_sante_fabricant/
├── data/                # Répertoire de stockage des données
├── src/                 # Code source
│   ├── data_fetcher.py  # Module d'acquisition de données depuis l'API
│   ├── data_processor.py # Module de traitement des données et de calcul des KPI
│   ├── dashboard.py     # Interface du tableau de bord Streamlit
├── docs/                # Documentation
├── main.py              # Point d'entrée principal
├── requirements.txt     # Liste des dépendances
└── README.md            # Description du projet
```

## Guide d'Utilisation

1. Après le lancement de l'application, le navigateur ouvrira automatiquement la page du tableau de bord
2. Sélectionnez l'ID du fabricant, la catégorie de produit et la plage de dates dans la barre latérale gauche
3. Le tableau de bord se mettra à jour automatiquement pour afficher les KPI et graphiques correspondants
4. Vous pouvez déplier la section "Voir les Données Brutes" pour explorer les données sous-jacentes

## Auteur

[Nom de l'auteur]

## Licence

[Informations de licence] 