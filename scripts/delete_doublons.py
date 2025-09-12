# =============================================================================
# delete_doublons.py - UNIQUEMENT orchestration et affichage
# =============================================================================

import duckdb
from doublon_service import clean_duplicates
from doublon_repository import table_exists, get_table_columns

TABLES_CONFIG = {
    "erp_table": ["product_id"],
    "liaison_table": ["product_id", "id_web"],
    "web_table": ["sku"]
}

print("=== SUPPRESSION DES DOUBLONS ===")

try:
    conn = duckdb.connect("/app/data/database.duckdb")
    
    total_deleted = 0
    
    for table_name, id_columns in TABLES_CONFIG.items():
        if not table_exists(conn, table_name):
            print(f"⚠️  Table {table_name} non trouvée, ignorée")
            continue
            
        existing_columns = get_table_columns(conn, table_name)
        print(f"\nColonnes dans {table_name}: {existing_columns}")
        
        for id_column in id_columns:
            if id_column not in existing_columns:
                print(f"⚠️  Colonne {id_column} non trouvée dans {table_name}, ignorée")
                continue
                
            try:
                result = clean_duplicates(conn, table_name, id_column)
                
                print(f"\n🧹 Nettoyage {table_name}.{id_column}")
                print(f"   - Prévisualisation: {result['preview']:,} lignes à supprimer")
                print(f"   - Supprimées: {result['deleted']:,} lignes")
                print(f"   - Statut: {result['message']}")
                
                total_deleted += result['deleted']
                
                if result['deleted'] > 0:
                    print(f"   ✅ Doublons supprimés avec succès")
                else:
                    print(f"   ℹ️  Aucune suppression nécessaire")
                    
            except Exception as e:
                print(f"❌ Erreur lors du nettoyage de {table_name}.{id_column}: {e}")
    
    print(f"\n📈 RÉSUMÉ GLOBAL:")
    print(f"   - Total lignes supprimées: {total_deleted:,}")
    if total_deleted > 0:
        print(f"   🎉 Nettoyage terminé avec succès !")
    else:
        print(f"   ✨ Base de données déjà propre, aucun doublon trouvé")

except Exception as e:
    print(f"❌ Erreur de connexion à la base: {e}")
finally:
    if 'conn' in locals():
        conn.close()
        print("\n🔚 Connexion fermée")