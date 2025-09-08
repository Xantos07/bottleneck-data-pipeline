import duckdb

db_path = "/app/data/database.duckdb"

# Configuration des tables et leurs colonnes d'ID
TABLES_CONFIG = {
    "erp_table": ["product_id"],
    "liaison_table": ["product_id", "id_web"], 
    "web_table": ["id_web"]
}

print("ANALYSE DES DOUBLONS D'IDs")
print("="*50)

conn = duckdb.connect(db_path)

# Vérifier les tables existantes
existing_tables = [table[0] for table in conn.execute("SHOW TABLES").fetchall()]
print(f"Tables disponibles: {existing_tables}")

total_duplicates_found = 0

for table_name, id_columns in TABLES_CONFIG.items():
    print(f"\n ANALYSE DE: {table_name}")
    print("-" * 40)
    
    if table_name not in existing_tables:
        print(f"Table '{table_name}' non trouvée - ignorée")
        continue
    
    # Vérifier que les colonnes existent
    table_columns = [col[0] for col in conn.execute(f"DESCRIBE {table_name}").fetchall()]
    
    for id_column in id_columns:
        print(f"\n Analyse de la colonne: {id_column}")
        
        if id_column not in table_columns:
            print(f"Colonne '{id_column}' non trouvée dans {table_name}")
            continue
        
        try:
            # Analyser les doublons pour cette colonne
            duplicate_analysis = conn.execute(f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT {id_column}) as unique_ids,
                COUNT(*) - COUNT(DISTINCT {id_column}) as duplicate_count
            FROM {table_name}
            WHERE {id_column} IS NOT NULL
            """).fetchone()
            
            total, unique, duplicates = duplicate_analysis
            
            print(f"Total lignes (non NULL): {total:,}")
            print(f"IDs uniques: {unique:,}")
            print(f"Doublons: {duplicates:,}")
            
            if duplicates > 0:
                duplicate_rate = (duplicates/total)*100 if total > 0 else 0
                print(f"Taux de doublons: {duplicate_rate:.2f}%")
                total_duplicates_found += duplicates
                
                # Identifier les IDs en doublon (top 10)
                duplicate_ids = conn.execute(f"""
                SELECT {id_column}, COUNT(*) as occurrences
                FROM {table_name}
                WHERE {id_column} IS NOT NULL
                GROUP BY {id_column}
                HAVING COUNT(*) > 1
                ORDER BY COUNT(*) DESC
                LIMIT 10
                """).fetchall()
                
                print(f"Top 10 des {id_column} en doublon:")
                for item_id, count in duplicate_ids:
                    print(f"      • '{item_id}': {count} occurrences")
            else:
                print(f"Aucun doublon détecté pour {id_column}")
                
        except Exception as e:
            print(f"Erreur lors de l'analyse de {id_column}: {str(e)}")

# Résumé global
print(f"\n" + "="*50)
print(f"RÉSUMÉ DES DOUBLONS")
print(f"="*50)

if total_duplicates_found > 0:
    print(f"Total doublons détectés: {total_duplicates_found:,}")
else:
    print(f"Aucun doublon détecté dans les IDs")

conn.close()
print(f"\n Analyse des doublons terminée!")