from doublon_repository import count_duplicates, fetch_duplicate_samples, delete_duplicate_samples, preview_deletation

def analyze_duplicates(conn, table_name, column):
    total_rows, unique_ids, duplicate_count = count_duplicates(conn, table_name, column)
    duplicate_rate = (duplicate_count / total_rows) * 100 if total_rows > 0 else 0

    return {
        "table": table_name,
        "column": column,
        "total_rows": total_rows,
        "unique_ids": unique_ids,
        "duplicate_count": duplicate_count,
        "duplicate_rate": duplicate_rate
    }

def sample_duplicates(conn, table_name, column, limit=3):
    return fetch_duplicate_samples(conn, table_name, column, limit)
    
def clean_duplicates(conn, table_name, column):
    """Nettoie les doublons avec prévisualisation pour sécurité"""

    rows_to_delete = preview_deletation(conn, table_name, column)
    
    if rows_to_delete == 0:
        return {
            "preview": 0,
            "deleted": 0,
            "message": "Aucun doublon à supprimer"
        }
    
    rows_deleted = delete_duplicate_samples(conn, table_name, column)
    
    return {
        "preview": rows_to_delete,
        "deleted": rows_deleted,
        "message": f"Suppression terminée: {rows_deleted} lignes supprimées"
    }