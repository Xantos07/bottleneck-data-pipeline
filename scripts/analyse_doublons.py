# analyse_doublons.py

import duckdb
from doublon_service import analyze_duplicates, sample_duplicates
from doublon_repository import table_exists, get_table_columns

TABLES_CONFIG = {
    "erp_table": ["product_id"],
    "liaison_table": ["product_id", "id_web"],
    "web_table": ["sku"]
}

print("=== AUDIT DES DOUBLONS ===")

try:
    # Charger les données
    #conn = duckdb.connect(":memory:")  #  Mémoire
    # Se connecter à la base de données existante
    conn = duckdb.connect("database.duckdb")
    
    print("Connexion à la base de données existante")
    

    # conn = duckdb.connect("/app/data/database.duckdb")
    
    for table_name, id_columns in TABLES_CONFIG.items():
        if not table_exists(conn, table_name):
            print(f"[ANALYSE WARINING]  Table {table_name} non trouvée, ignorée")
            continue
            
        existing_columns = get_table_columns(conn, table_name)
        print(f"\nColonnes dans {table_name}: {existing_columns}")
        
        for id_column in id_columns:
            if id_column not in existing_columns:
                print(f"[ANALYSE WARINING]  Colonne {id_column} non trouvée dans {table_name}, ignorée")
                continue
                
            try:
                stats = analyze_duplicates(conn, table_name, id_column)
                print(f"\n [ANALYSE] Audit {table_name}.{id_column}")
                print(f"   - Lignes totales: {stats['total_rows']:,}")
                print(f"   - IDs uniques: {stats['unique_ids']:,}")
                print(f"   - Doublons: {stats['duplicate_count']:,}")
                print(f"   - Taux de doublons: {stats['duplicate_rate']:.2f}%")

                if stats["duplicate_count"] > 0:
                    print("[ANALYSE] Exemples de doublons:")
                    for dup_id, occ in sample_duplicates(conn, table_name, id_column):
                        print(f"     • {dup_id} ({occ} occurrences)")
                else:
                    print("[ANALYSE] Aucun doublon détecté")
                    
            except Exception as e:
                print(f"[ANALYSE ERREUR] Erreur lors de l'analyse de {table_name}.{id_column}: {e}")

except Exception as e:
    print(f"[ANALYSE ERREUR] Erreur de connexion à la base: {e}")
finally:
    if 'conn' in locals():
        conn.close()
        print("\n Connexion fermée")