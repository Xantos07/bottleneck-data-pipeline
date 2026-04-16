import duckdb
import pandas as pd

try:
    conn = duckdb.connect("database.duckdb")
    
    query = """
    SELECT 
        e.product_id,
        l.id_web,
        CAST(e.price AS DOUBLE) AS prix_unitaire,
        CAST(w.total_sales AS DOUBLE) AS nombre_vendu,
        w.post_title,
        (CAST(e.price AS DOUBLE) * CAST(w.total_sales AS DOUBLE)) AS prix_total
    FROM erp_table e
    JOIN liaison_table l ON e.product_id = l.product_id
    JOIN web_table w ON l.id_web = w.sku
    WHERE CAST(e.price AS DOUBLE) > 0
    """

    df_querry = pd.read_sql_query(query, conn)
    print(df_querry.head())

    chiffreTotal= """
    SELECT
      SUM(CAST(e.price AS DOUBLE) * CAST(w.total_sales AS DOUBLE)) AS chiffre_affaire_total
    FROM erp_table e
    JOIN liaison_table l on e.product_id = l.product_id
    JOIN web_table w ON l.id_web = w.sku
    WHERE CAST(e.price AS DOUBLE) > 0
    """
    df = pd.read_sql_query(chiffreTotal, conn)
    
    print(df)
    
    df_querry.to_csv("/app/data/chiffre_affaire.csv", sep='\t', index=False)

except Exception as e:
    print("ERROR:", e)

finally:
    if 'conn' in locals():
        conn.close()
        print("\nConnexion fermée")
