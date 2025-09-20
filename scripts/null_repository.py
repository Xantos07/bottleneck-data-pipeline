def delete_nulls_samples(conn, table_name, column):
    """Supprime les lignes avec valeurs NULL ou vides"""
    try:
        count_before = count_total_rows(conn, table_name)

        conn.execute(f"""
            DELETE FROM {table_name}
            WHERE {column} IS NULL
                OR (TRIM(CAST({column} AS VARCHAR)) = '' OR CAST({column} AS VARCHAR) = '')
        """)

        count_after = count_total_rows(conn, table_name)
        return count_before - count_after
    except Exception as e:
        print(f"Erreur lors de la suppression: {e}")
        return 0

def preview_deletation(conn, table_name, column):
  """Prévisualise combien de lignes seraient supprimées SANS les supprimer"""
  return conn.execute(f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE {column} IS NULL
           OR (TRIM(CAST({column} AS VARCHAR)) = '' OR CAST({column} AS VARCHAR) = '')
  """).fetchone()[0]



def count_total_rows(conn, table_name):
  return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

def count_null_values(conn, table_name, column):
    return conn.execute(f"""
        SELECT COUNT(*) 
        FROM {table_name}
        WHERE {column} IS NULL
    """).fetchone()[0]

def count_empty_values(conn, table_name, column):
    return conn.execute(f"""
        SELECT COUNT(*) 
        FROM {table_name}
        WHERE {column} IS NOT NULL 
          AND (TRIM(CAST({column} AS VARCHAR)) = '' OR CAST({column} AS VARCHAR) = '')
    """).fetchone()[0]


def fetch_null_samples(conn, table_name, column, limit=3):
    """Récupère des exemples de lignes avec des valeurs NULL ou vides"""
    return conn.execute(f"""
        SELECT *
        FROM {table_name}
        WHERE {column} IS NULL 
           OR (TRIM(CAST({column} AS VARCHAR)) = '' OR CAST({column} AS VARCHAR) = '')
        LIMIT {limit}
    """).fetchall()

def table_exists(conn, table_name):
    existing_tables = [row[0] for row in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()]
    return table_name in existing_tables

def get_table_columns(conn, table_name):
    return [row[0] for row in conn.execute(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
    ).fetchall()]