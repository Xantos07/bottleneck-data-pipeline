import duckdb

db_path = "/app/data/database.duckdb"

# Configuration des tables et leurs colonnes d'ID critiques
TABLES_CONFIG = {
    "erp_table": ["product_id"],
    "liaison_table": ["product_id", "id_web"], 
    "web_table": ["id_web"]
}

print(" ANALYSE DES VALEURS NULL")
print("="*50)

conn = duckdb.connect(db_path)

# Vérifier les tables existantes
existing_tables = [table[0] for table in conn.execute("SHOW TABLES").fetchall()]
print(f" Tables disponibles: {existing_tables}")

total_null_found = 0
tables_with_nulls = 0

for table_name, id_columns in TABLES_CONFIG.items():
    print(f"\n ANALYSE DE: {table_name}")
    print("-" * 40)
    
    if table_name not in existing_tables:
        print(f" Table '{table_name}' non trouvée - ignorée")
        continue
    
    # Obtenir le nombre total de lignes
    total_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f" Total lignes dans la table: {total_rows:,}")
    
    if total_rows == 0:
        print(f"  Table vide - aucune analyse possible")
        continue
    
    # Vérifier que les colonnes existent
    table_columns = [col[0] for col in conn.execute(f"DESCRIBE {table_name}").fetchall()]
    
    table_has_nulls = False
    
    for id_column in id_columns:
        print(f"\n Analyse des NULL pour: {id_column}")
        
        if id_column not in table_columns:
            print(f" Colonne '{id_column}' non trouvée dans {table_name}")
            continue
        
        try:
            # Compter les valeurs NULL
            null_count = conn.execute(f"""
            SELECT COUNT(*) 
            FROM {table_name} 
            WHERE {id_column} IS NULL
            """).fetchone()[0]
            
            # Compter les valeurs vides (chaînes vides)
            empty_count = conn.execute(f"""
            SELECT COUNT(*) 
            FROM {table_name} 
            WHERE {id_column} = '' OR TRIM({id_column}) = ''
            """).fetchone()[0]
            
            valid_count = total_rows - null_count - empty_count
            
            print(f"Valeurs NULL: {null_count:,}")
            print(f"Valeurs vides: {empty_count:,}")
            print(f"Valeurs valides: {valid_count:,}")
            
            if null_count > 0 or empty_count > 0:
                null_rate = ((null_count + empty_count)/total_rows)*100
                print(f"Taux de valeurs manquantes: {null_rate:.2f}%")
                total_null_found += null_count + empty_count
                table_has_nulls = True
                
                # Exemples de lignes avec des valeurs NULL/vides
                if null_count > 0:
                    print(f"   🔍 Exemples de lignes avec {id_column} NULL:")
                    sample_nulls = conn.execute(f"""
                    SELECT *
                    FROM {table_name}
                    WHERE {id_column} IS NULL
                    LIMIT 3
                    """).fetchall()
                    
                    for i, row in enumerate(sample_nulls, 1):
                        print(f"      Ligne {i}: {row}")
            else:
                print(f"Aucune valeur manquante pour {id_column}")
                
        except Exception as e:
            print(f"Erreur lors de l'analyse de {id_column}: {str(e)}")
    
    if table_has_nulls:
        tables_with_nulls += 1

# Analyse supplémentaire : valeurs NULL dans toutes les colonnes
print(f"\n" + "="*50)
print(f"ANALYSE GLOBALE DES NULL (toutes colonnes)")
print(f"="*50)

for table_name in existing_tables:
    if table_name not in TABLES_CONFIG:
        continue
        
    print(f"\n Résumé pour {table_name}:")
    
    try:
        # Obtenir toutes les colonnes
        all_columns = [col[0] for col in conn.execute(f"DESCRIBE {table_name}").fetchall()]
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        if total_rows == 0:
            continue
            
        columns_with_nulls = []
        
        for column in all_columns:
            null_count = conn.execute(f"""
            SELECT COUNT(*) 
            FROM {table_name} 
            WHERE {column} IS NULL
            """).fetchone()[0]
            
            if null_count > 0:
                null_rate = (null_count/total_rows)*100
                columns_with_nulls.append((column, null_count, null_rate))
        
        if columns_with_nulls:
            print(f"Colonnes avec des NULL:")
            for column, count, rate in columns_with_nulls:
                print(f" {column}: {count:,} NULL ({rate:.1f}%)")
        else:
            print(f"Aucune colonne avec des NULL")
            
    except Exception as e:
        print(f" Erreur: {str(e)}")

# Résumé final
print(f"\n" + "="*50)
print(f"RÉSUMÉ DES VALEURS NULL")
print(f"="*50)

print(f"Tables analysées: {len([t for t in TABLES_CONFIG.keys() if t in existing_tables])}")
print(f"Tables avec des NULL: {tables_with_nulls}")

if total_null_found > 0:
    print(f" Total valeurs manquantes détectées: {total_null_found:,}")
else:
    print(f" Aucune valeur NULL détectée dans les colonnes critiques")

conn.close()
print(f"\n Analyse des valeurs NULL terminée!")