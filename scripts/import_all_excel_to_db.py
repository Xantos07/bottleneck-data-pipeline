import duckdb
import pandas as pd
import os

# Configuration des fichiers à importer
# Peut être revoir car nous importons tout en brut avec les erreurs pour ensuite faire le nettoyage, à voir pour peut être fair l'inverse 

# mettre dans un file model/util
FILES_CONFIG = {
    "erp_table": {
        "file": "/app/data/Fichier_erp.xlsx",
        "table": "erp_table"
    },
    "liaison_table": {
        "file": "/app/data/fichier_liaison.xlsx", 
        "table": "liaison_table"
    },
    "web_table": {
        "file": "/app/data/Fichier_web.xlsx",
        "table": "web_table"
    }
}

db_path = "/app/data/database.duckdb"

def import_excel_to_table(file_path, table_name, conn):
    """Importe un fichier Excel vers une table DuckDB"""
    print(f"\n=== Import de {file_path} vers {table_name} ===")
    
    # Vérifier que le fichier existe
    if not os.path.exists(file_path):
        print(f"  Fichier non trouvé: {file_path}")
        return False
    
    try:
        # Lire le fichier Excel
        print(f" Lecture du fichier: {file_path}")
        df = pd.read_excel(file_path)
        print(f"Données lues: {df.shape[0]} lignes, {df.shape[1]} colonnes")
        print(f"Colonnes: {list(df.columns)}")
        
        # Supprimer la table si elle existe déjà
        # REPOSITORY /!\  rien a faire ici :)
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Créer la table depuis le DataFrame
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        
        # Vérifier l'import
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f" Table '{table_name}' créée avec {count} lignes")
        
        # Afficher un aperçu
        sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
        print(" Aperçu des données:")
        for row in sample:
            print(f"   {row}")
            
        return True
        
    except Exception as e:
        print(f" Erreur lors de l'import de {file_path}: {str(e)}")
        return False

def main():
    print("Import de tous les fichiers Excel vers DuckDB...")
    
    # Connexion à DuckDB
    conn = duckdb.connect(db_path)
    
    success_count = 0
    total_files = len(FILES_CONFIG)
    
    # Importer chaque fichier
    for config_name, config in FILES_CONFIG.items():
        success = import_excel_to_table(
            config["file"], 
            config["table"], 
            conn
        )
        if success:
            success_count += 1
    
    # Résumé final
    print(f"\n RÉSUMÉ DE L'IMPORT")
    print(f"Fichiers importés avec succès: {success_count}/{total_files}")
    
    if success_count > 0:
        print(f"\n Tables créées dans la base:")
        tables = conn.execute("SHOW TABLES").fetchall()
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
            print(f"   • {table[0]}: {count} lignes")
    
    conn.close()
    print(f"\n Import terminé!")

if __name__ == "__main__":
    main()