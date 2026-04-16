def delete_duplicate_samples(conn, table_name, column):
    """Supprime les doublons, garde la première occurrence (plus petit rowid)"""
    try:
        count_before = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        conn.execute(f"""
            DELETE FROM {table_name}
            WHERE rowid NOT IN (
              SELECT MIN(rowid)
              FROM {table_name}
              WHERE {column} IS NOT NULL
              GROUP BY {column} 
            )
            AND {column} IS NOT NULL
        """)
        
        count_after = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        return count_before - count_after
        
    except Exception as e:
        print(f"Erreur lors de la suppression v2: {e}")
        return 0

def preview_deletation(conn, table_name, column):
  """Prévisualise combien de lignes seraient supprimées SANS les supprimer"""
  return conn.execute(f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE rowid NOT IN (
          SELECT MIN(rowid)
          FROM {table_name}
          WHERE {column} IS NOT NULL
          GROUP BY {column}
        )
        AND {column} IS NOT NULL
  """).fetchone()[0]


def count_duplicates(conn, table_name, column):
    return conn.execute(f"""
        SELECT COUNT(*) as total_rows,
               COUNT(DISTINCT CASE WHEN {column} IS NOT NULL THEN {column} END) as unique_ids,
               (SELECT COUNT(*) FROM {table_name} WHERE {column} IS NOT NULL) - COUNT(DISTINCT CASE WHEN {column} IS NOT NULL THEN {column} END) as duplicate_count
        FROM {table_name}
    """).fetchone()

def fetch_duplicate_samples(conn, table_name, column, limit=3):
    return conn.execute(f"""
        SELECT {column}, COUNT(*) as occurrences
        FROM {table_name}
        WHERE {column} IS NOT NULL
        GROUP BY {column}
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
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