import pandas as pd
import os

# Configuration des fichiers à importer
FILES_CONFIG = {
    "erp_table": {
        "file": "/app/data/Fichier_erp.xlsx",
        "output": "erp_table.parquet"
    },
    "liaison_table": {
        "file": "/app/data/fichier_liaison.xlsx", 
        "output": "liaison_table.parquet"
    },
    "web_table": {
        "file": "/app/data/Fichier_web.xlsx",
        "output": "web_table.parquet"
    }
}

def import_excel_to_parquet(excel_path, parquet_path, table_name):
    """Importe un fichier Excel vers Parquet"""
    print(f"\n=== Import de {table_name} ===")

    # Vérifier que le fichier existe
    if not os.path.exists(excel_path):
        print(f"Fichier non trouvé: {excel_path}")
        return False
    
    try:
        # Lire le fichier
        print(f"Lecture du fichier: {excel_path}")
        df = pd.read_excel(excel_path, dtype=str)
        print(f"Données lues: {df.shape[0]} lignes, {df.shape[1]} colonnes")
        print(f"Colonnes: {list(df.columns)}")
        
        # Export en Parquet
        df.to_parquet(parquet_path, index=False, compression='snappy', engine='pyarrow')
        print(f"Export réussi: {parquet_path}")
        
        # Afficher un aperçu
        print("Aperçu des données:")
        print(df.head(3).to_string())
        
        return True
        
    except Exception as e:
        print(f"Erreur lors de l'import de {excel_path}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("Import de tous les fichiers Excel vers Parquet...")
    
    # Importer chaque fichier
    success_count = 0
    for table_name, config in FILES_CONFIG.items():
        if import_excel_to_parquet(config["file"], config["output"], table_name):
            success_count += 1
    
    print(f"\n=== Import terminé : {success_count}/{len(FILES_CONFIG)} fichiers importés ===")
    
    # Lister les fichiers créés
    print("\n=== Fichiers Parquet créés ===")
    for file in os.listdir('.'):
        if file.endswith('.parquet'):
            size = os.path.getsize(file)
            print(f"✓ {file} ({size:,} octets)")

if __name__ == "__main__":
    main()