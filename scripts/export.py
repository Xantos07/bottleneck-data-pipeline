# export.py

import duckdb
import pandas as pd

TABLES_CONFIG = {
    "erp_table": ["product_id"],
    "liaison_table": ["product_id", "id_web"], 
    "web_table": ["sku"]
}

print("=== EXPORTATION DES DONNEES ===")

try:

    conn = duckdb.connect("/app/data/database.duckdb")
    
    # Une extraction du rapport avec les chiffres d’affaires par produit en .xls
    # Une extraction de la liste des vins premium en .csv
    # Une extraction des vins ordinaires en .csv

    # faire une nouvelle table correct ? 
    # post_title | prix TT
    # post_title | categorie 
    
    df.to_csv("petit_test_export", sep='\t')

    
  

except Exception as e:
    print(f"Erreur de connexion à la base: {e}")
finally:
    if 'conn' in locals():
        conn.close()
        print("\n Connexion fermée")