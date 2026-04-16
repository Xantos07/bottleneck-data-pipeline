import duckdb
import pandas as pd

try:
    conn = duckdb.connect("database.duckdb")
    
    query = """
    SELECT 
        e.product_id, 
        CAST(e.price AS DOUBLE) AS price, 
        e.stock_quantity, 
        w.post_title
    FROM erp_table e
    JOIN liaison_table l ON e.product_id = l.product_id
    JOIN web_table w ON l.id_web = w.sku  
    WHERE CAST(e.price AS DOUBLE) > 0
    """

    df = pd.read_sql_query(query, conn)

    moyenne_prix = df['price'].mean()
    ecart_type = df['price'].std()

    print(f"Moyenne des prix: {moyenne_prix:.2f} (€/$)")
    print(f"Ecart type des prix: {ecart_type:.2f}")

    df['z-score'] = (df['price'] - moyenne_prix) / ecart_type
    df["category"] = df["z-score"].apply(lambda x: "millésime" if x > 2 else "ordinaires")

    # Échantillons
    millesimes = df[df["category"] == "millésime"].head(5)
    ordinaires = df[df["category"] == "ordinaires"].head(5)
    echantillon = pd.concat([millesimes, ordinaires])

    print(df["category"].value_counts())
    print(echantillon)

    # Export principal
    df[["price","post_title","category"]].to_csv("/app/data/petit_test_export.csv", sep='\t', index=False)

    # Export séparé
    exports = {
        "vins_premium.csv": df[df["category"] == "millésime"],
        "vins_ordinaires.csv": df[df["category"] == "ordinaires"]
    }

    for filename, data in exports.items():
        data.to_csv(f"/app/data/{filename}", sep='\t', index=False)
        print(f"Exporté {filename}")

except Exception as e:
    print("ERROR:", e)

finally:
    if 'conn' in locals():
        conn.close()
        print("\nConnexion fermée")
