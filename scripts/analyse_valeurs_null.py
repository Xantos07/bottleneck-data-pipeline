# analyse_nulls.py

import duckdb
from null_service import analyze_nulls, sample_nulls
from null_repository import table_exists, get_table_columns

TABLES_CONFIG = {
    "erp_table": ["product_id"],
    "liaison_table": ["product_id", "id_web"], 
    "web_table": ["sku"]
}

print("=== ANALYSE DES VALEURS NULL ===")

try:
    #conn = duckdb.connect("/app/data/database.duckdb")
    
    # Charger les données
    # conn = duckdb.connect(":memory:")  #  Mémoire
    # Se connecter à la base de données existante
    conn = duckdb.connect("database.duckdb")
    
    print("Connexion à la base de données existante")
    total_null_found = 0
    
    for table_name, id_columns in TABLES_CONFIG.items():
        if not table_exists(conn, table_name):
            print(f"[ANALYSE WARINING]  Table {table_name} non trouvée, ignorée")
            continue
        
        existing_columns = get_table_columns(conn, table_name)
        print(f"\nColonnes dans {table_name}: {existing_columns}")
        
        for column in id_columns:
            if column not in existing_columns:
                print(f"[ANALYSE WARINING]  Colonne {column} non trouvée dans {table_name}, ignorée")
                continue
            
            try:
                stats = analyze_nulls(conn, table_name, column)
                
                print(f"\n[ANALYSE]  Analyse {table_name}.{column}")
                print(f"   - Total lignes: {stats['total_rows']:,}")
                print(f"   - NULL: {stats['null_count']:,}")
                print(f"   - Vides: {stats['empty_count']:,}")
                print(f"   - Valides: {stats['valid_count']:,}")
                print(f"   - Taux manquants: {stats['missing_rate']:.2f}%")
                
                total_null_found += stats['missing_count']
                
                if stats['missing_count'] > 0:
                    print("[ANALYSE] Exemples de lignes avec valeurs manquantes:")
                    for i, row in enumerate(sample_nulls(conn, table_name, column), 1):
                        print(f"     Exemple {i}: {dict(zip(existing_columns, row))}")
                else:
                    print("[ANALYSE] Aucune valeur manquante")
                    
            except Exception as e:
                print(f"[ANALYSE ERREUR] Erreur lors de l'analyse de {table_name}.{column}: {e}")
    
    print(f"\n [ANALYSE] RÉSUMÉ GLOBAL:")
    print(f"   - Total valeurs manquantes trouvées: {total_null_found:,}")

except Exception as e:
    print(f" [ANALYSE ERROR] Erreur de connexion à la base: {e}")
finally:
    if 'conn' in locals():
        conn.close()
        print("\n Connexion fermée")