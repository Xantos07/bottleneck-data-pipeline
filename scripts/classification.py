import duckdb
import pandas as pd

try:
    conn = duckdb.connect(database='/app/data/database.duckdb')
    query = """SELECT 
    e.product_id, 
    e.onsale_web, 
    e.price, 
    e.stock_quantity, 
    e.stock_status,
    w.post_title
    FROM erp_table e
    JOIN liaison_table l ON e.product_id = l.product_id
    JOIN web_table w ON l.id_web = w.sku  
    WHERE price > 0 """

    df = pd.read_sql_query(query, conn)

    moyenne_prix = df['price'].mean()
    print(f"Moyenne des prix: {moyenne_prix:.2f} (€/$ ???) ")

    ecart_type = df['price'].std()
    print(f"Ecart type des prix: {ecart_type:.2f}")

    df['z-score'] = (df['price'] - moyenne_prix) /  ecart_type

    
    df["category"] = df["z-score"].apply(lambda x: "millésime" if x > 2 else "ordinaires")

    millesimes = df[df["category"] == "millésime"].head(5)
    ordinaires = df[df["category"] == "ordinaires"].head(5)
    echantillon = pd.concat([millesimes, ordinaires])
    print(df["category"].value_counts())
    print(echantillon)

    df = df[["price","post_title","category"]]
    df.to_csv("/app/data/petit_test_export.csv", sep='\t', index=False)

    millesimes = df[df["category"] == "millésime"]
    ordinaires = df[df["category"] == "ordinaires"]

    exports = {
        "vins_premium.csv": millesimes,
        "vins_ordinaires.csv": ordinaires
    }

    for filename, data in exports.items():
        output_path = f"/app/data/{filename}"
        data.to_csv(output_path, sep='\t', index=False)
        print(f"Exporté {filename}")

    
except Exception as e:
    print("ERROR")
finally:
    if 'conn' in locals():
        conn.close()
        print("\n Connexion fermée")