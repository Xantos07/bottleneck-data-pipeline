from null_repository import  count_total_rows, count_null_values, count_empty_values, fetch_null_samples

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