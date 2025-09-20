import duckdb
import pandas as pd

try:
    conn = duckdb.connect(database='/app/data/database.duckdb')

    query = """
    SELECT 
        e.product_id,
        l.id_web,
        e.price AS prix_unitaire,
        w.total_sales AS nombre_vendu,
        w.post_title,
        (e.price * w.total_sales) AS prix_total
    FROM erp_table e
    JOIN liaison_table l ON e.product_id = l.product_id
    JOIN web_table w ON l.id_web = w.sku
    WHERE e.price > 0
    """

    df_querry = pd.read_sql_query(query, conn)
    print(df_querry.head())

    chiffreTotal= """
    SELECT
      SUM(e.price * w.total_sales) AS chiffre_affaire_total
      FROM erp_table e
      JOIN liaison_table l on e.product_id = l.product_id
      JOIN web_table w ON l.id_web = w.sku
      WHERE e.price > 0
      """
    df = pd.read_sql_query(chiffreTotal, conn)
    
    print(df)
    
    output_path = f"/app/data/chiffre_affaire.csv"
    df_querry.to_csv(output_path, sep='\t', index=False)

except Exception as e:
    print("ERROR:", e)

finally:
    if 'conn' in locals():
        conn.close()
        print("\nConnexion fermée")
