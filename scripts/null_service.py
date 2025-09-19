from null_repository import count_total_rows, count_null_values, count_empty_values, fetch_null_samples, delete_nulls_samples, preview_deletation

def analyze_nulls(conn, table_name, column):
    total_rows = count_total_rows(conn, table_name)
    null_count = count_null_values(conn, table_name, column)
    empty_count = count_empty_values(conn, table_name, column)
    
    valid_count = total_rows - null_count - empty_count
    missing_count = null_count + empty_count
    missing_rate = (missing_count / total_rows) * 100 if total_rows > 0 else 0
    
    return {
        "table": table_name,
        "column": column,
        "total_rows": total_rows,
        "null_count": null_count,
        "empty_count": empty_count,
        "valid_count": valid_count,
        "missing_count": missing_count,
        "missing_rate": missing_rate
    }

def sample_nulls(conn, table_name, column, limit=3):
    return fetch_null_samples(conn, table_name, column, limit)



def clean_nulls(conn, table_name, column):
    """Nettoie les valeurs nulles et vides avec prévisualisation pour sécurité"""

    rows_to_delete = preview_deletation(conn, table_name, column)

    if rows_to_delete == 0:
        return {
            "preview": 0,
            "deleted": 0,
            "message": "Aucune valeur nulle ou vide à supprimer"
        }

    # Suppression des lignes avec valeurs nulles ou vides
    rows_deleted = delete_nulls_samples(conn, table_name, column)

    return {
        "preview": rows_to_delete,
        "deleted": rows_deleted,
        "message": f"Suppression terminée: {rows_deleted} lignes supprimées"
    }
