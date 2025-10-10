# delete_valeurs_null.py

import duckdb
from null_service import  clean_nulls
from null_repository import table_exists, get_table_columns

TABLES_CONFIG = {
    "erp_table": ["product_id"],
    "liaison_table": ["product_id", "id_web"],
    "web_table": ["sku"]
}

print ("\n=== Suppression des valeurs nulles et vides ===")

try:
    # Se connecter à la base de données reçue depuis analyse_data
    conn = duckdb.connect("database.duckdb")

    total_deleted = 0

    for table_name, id_columns in TABLES_CONFIG.items():
        if not table_exists(conn, table_name):
            print(f"[DELETE NULL WARINING]   Table {table_name} non trouvée, passage...")
            continue


        existing_columns = get_table_columns(conn, table_name)
        print(f"\nColonnes dans {table_name}: {existing_columns}")

        for id_column in id_columns:
            if id_column not in existing_columns:
                print(f"[DELETE NULL WARINING]  Colonne {id_column} non trouvée dans {table_name}, passage...")
                continue

            try:
                result = clean_nulls(conn, table_name, id_column)

                print(f"\n [DELETE NULL] Nettoyage {table_name}.{id_column}")
                print(f"   - Prévisualisation: {result['preview']:,} lignes à supprimer")
                print(f"   - Supprimées: {result['deleted']:,} lignes")
                print(f"   - Statut: {result['message']}")

                total_deleted += result['deleted']

                if result['deleted'] > 0:
                    print(f" [DELETE NULL] Valeurs nulles et vides supprimées avec succès")
                else:
                    print(f" [DELETE NULL]  Aucune suppression nécessaire")

            except Exception as e:
                print(f"[DELETE NULL ERREUR] Erreur lors du nettoyage de {table_name}.{id_column}: {e}")

        print(f"\n [DELETE NULL] RÉSUMÉ GLOBAL:")
        print(f"   - Total lignes supprimées: {total_deleted:,}")
        if total_deleted > 0:
            print(f"   [DELETE NULL] Nettoyage terminé avec succès !")
        else:
            print(f"   [DELETE NULL]  Base de données déjà propre, aucune valeur nulle ou vide trouvée")


except Exception as e:
    print(f"[DELETE NULL ERREUR] Erreur de connexion à la base: {e}")

except Exception as e:
    print(f"[DELETE NULL ERREUR] Erreur de connexion à la base: {e}")
finally:
    if 'conn' in locals():
        conn.close()
        print("Connexion à la base fermée.")


