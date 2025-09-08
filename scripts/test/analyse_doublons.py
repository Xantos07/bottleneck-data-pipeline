import duckdb

db_path = "/app/data/database.duckdb"
table_name = "wine_table"

print("Analyse des doublons...")

conn = duckdb.connect(db_path)

# Vérifier que la table existe
tables = conn.execute("SHOW TABLES").fetchall()
if (table_name,) not in tables:
    print(f"Table '{table_name}' non trouvée")
    exit(1)

# Analyser les doublons sur product_id
duplicate_analysis = conn.execute(f"""
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT product_id) as unique_products,
    COUNT(*) - COUNT(DISTINCT product_id) as duplicate_count
FROM {table_name}
""").fetchone()

total, unique, duplicates = duplicate_analysis
print(f"Analyse des doublons:")
print(f"Total des lignes: {total}")
print(f"Produits uniques: {unique}")
print(f"Doublons détectés: {duplicates}")

if duplicates > 0:
    # Identifier les product_id en doublon
    duplicate_ids = conn.execute(f"""
    SELECT product_id, COUNT(*) as occurrences
    FROM {table_name}
    GROUP BY product_id
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC
    LIMIT 10
    """).fetchall()
    
    print("Top 10 des product_id en doublon:")
    for product_id, count in duplicate_ids:
        print(f"  Product ID '{product_id}': {count} occurrences")
else:
    print("Aucun doublon détecté")

conn.close()