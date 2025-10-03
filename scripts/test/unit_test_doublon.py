import pytest
from unittest.mock import Mock, MagicMock
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from doublon_service import analyze_duplicates, sample_duplicates
from doublon_repository import table_exists, get_table_columns

def test_colomn_none_doublon():
  """Test avec une table ne contenant pad de doublon"""
  
  mock_conn = Mock()

  mock_conn.execute.return_value.fetchone.return_value = (4,4,0)

  result = analyze_duplicates(mock_conn, "test_table", "id")

  assert result["duplicate_count"] == 0
  assert result["duplicate_rate"] == 0.0
  assert result["total_rows"] == 4
  assert result["unique_ids"] == 4
  


try:
    test_colomn_none_doublon();
except Exception as e:
    print(f"[UNIT TEST ERREUR] Erreur {e}")
finally:
    if 'conn' in locals():
        conn.close()
        print("\n Connexion fermée")