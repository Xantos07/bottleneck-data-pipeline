import duckdb
import pandas as pd

try:
    conn = duckdb.connect(database='/app/data/database.duckdb')
    query = """SELECT product_id, onsale_web, price, stock_quantity, stock_status
    FROM erp_table
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

    print(echantillon)
    
except Exception as e:
    print("ERROR")
finally:
    if 'conn' in locals():
        conn.close()
        print("\n Connexion fermée")