import pytest
from unittest.mock import Mock, MagicMock
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from doublon_service import analyze_duplicates, sample_duplicates
from doublon_repository import table_exists, get_table_columns

def test_colomn_none_doublon():
  """Test avec une table ne contenant pas de doublon"""
  
  mock_conn = Mock()

  # 4 lignes / 4 ids unique / 0 doublon
  mock_conn.execute.return_value.fetchone.return_value = (4,4,0)

  result = analyze_duplicates(mock_conn, "test_table", "id")

  assert result["duplicate_count"] == 0
  assert result["duplicate_rate"] == 0.0
  assert result["total_rows"] == 4
  assert result["unique_ids"] == 4
  
def test_colomn_with_doublon():
  """Test avec une table contenant des doublons"""

  mock_conn = Mock()

  # 7 lignes / 5 ids unique / 2 doublons
  mock_conn.execute.return_value.fetchone.return_value = (7, 5, 2)

  result = analyze_duplicates(mock_conn, "test_table", "id")

  assert result["duplicate_count"] == 2
  assert result["duplicate_rate"] > 0.0
  assert result["total_rows"] == 7
  assert result["unique_ids"] == 5

def test_sample_duplicates():
    """Test de l'échantillon des doublons"""
    mock_conn = Mock()

    #id 1 apparait 2 fois
    #is 5 apparait 3 fois
    mock_conn.execute.return_value.fetchall.return_value = [
        (1, 2),  
        (5, 3),  
    ]
    
    result = sample_duplicates(mock_conn, "test_table", "id", limit=3)
    
    assert len(result) == 2
    assert result[0][0] == 1
    assert result[0][1] == 2
    assert result[1][0] == 5
    assert result[1][1] == 3

def test_table_vide():
    """Test avec une table vide"""
    mock_conn = Mock()
    mock_conn.execute.return_value.fetchone.return_value = (0, 0, 0)
    
    result = analyze_duplicates(mock_conn, "empty_table", "id")
    
    assert result["duplicate_count"] == 0
    assert result["duplicate_rate"] == 0.0


@pytest.mark.parametrize("total,unique,expected_duplicates,expected_rate", [
    (10, 10, 0, 0.0),      #aucun doublon
    (10, 8, 2, 20.0),      #20% 
    (100, 50, 50, 50.0),   #50% 
    (5, 2, 3, 60.0),       #60% 
])

def test_duplicate_rates(total, unique, expected_duplicates, expected_rate):
    """Test des différents taux de doublons"""
    mock_conn = Mock()
    mock_conn.execute.return_value.fetchone.return_value = (total, unique, expected_duplicates)
    
    result = analyze_duplicates(mock_conn, "test_table", "id")
    
    assert result["duplicate_count"] == expected_duplicates
    assert result["duplicate_rate"] == expected_rate

try:
    test_colomn_none_doublon();
    test_colomn_with_doublon();
except Exception as e:
    print(f"[UNIT TEST ERREUR] Erreur {e}")
finally:
    if 'conn' in locals():
        conn.close()
        print("\n Connexion fermée")