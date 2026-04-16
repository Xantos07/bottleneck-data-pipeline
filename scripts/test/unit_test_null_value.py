import pytest
from unittest.mock import Mock, MagicMock
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from null_service import analyze_nulls, sample_nulls
from null_repository import table_exists, get_table_columns

def test_colomn_none_null():
    mock_conn = Mock()
    mock_conn.execute.return_value.fetchone.side_effect = [
        (4,),  # total rows
        (0,),  # nulls
        (0,)   # empty
    ]

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
    mock_conn.execute.return_value.fetchone.side_effect = [
        (7,),  # total rows
        (2,),  # nulls
        (2,)   # empty
    ]

    result = analyze_nulls(mock_conn, "test_table", "id")

    assert result["total_rows"] == 7
    assert result["null_count"] == 2
    assert result["empty_count"] == 2
    assert result["valid_count"] == 3  # 7 - 2 - 2
    assert result["missing_count"] == 4
    assert result["missing_rate"] > 0




if __name__ == "__main__":
    test_colomn_none_null()
    test_colomn_with_null()
    print("Tous les tests ont passé")