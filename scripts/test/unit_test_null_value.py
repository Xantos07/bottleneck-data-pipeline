import pytest
from unittest.mock import Mock, MagicMock
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from null_service import analyze_nulls, sample_nulls
from null_repository import table_exists, get_table_columns

def test_colomn_none_null():
  """Test avec une table ne contenant pas de valeur null"""
  
  mock_conn = Mock()

  # 4 lignes / 4 ids unique / 0 null
  mock_conn.execute.return_value.fetchone.return_value = (4,0,0)

  result = analyze_nulls(mock_conn, "test_table", "id")

  assert result["total_rows"] == 4
  assert result["null_count"] == 0
  assert result["empty_count"] == 0
  assert result["valid_count"] == 4
  assert result["missing_count"] == 0
  assert result["missing_rate"] == 0.0
  
def test_colomn_with_null():
  """Test avec une table contenant des valeurs null"""

  mock_conn = Mock()

  # 7 lignes / 5 ids unique / 2 null
  mock_conn.execute.return_value.fetchone.return_value = (7, 5, 2)

  result = analyze_nulls(mock_conn, "test_table", "id")

  assert result["total_rows"] == 7
  assert result["null_count"] == 2
  assert result["empty_count"] == 2
  assert result["valid_count"] == 5
  assert result["missing_count"] == 2
  assert result["missing_rate"] > 0.0


try:
    test_colomn_none_doublon();
    test_colomn_with_doublon();
except Exception as e:
    print(f"[UNIT TEST ERREUR] Erreur {e}")
finally:
    if 'conn' in locals():
        conn.close()
        print("\n Connexion fermée")