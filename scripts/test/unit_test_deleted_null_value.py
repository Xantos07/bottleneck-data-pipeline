import pytest
from unittest.mock import Mock, MagicMock, patch
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from null_service import clean_nulls
from null_repository import table_exists, get_table_columns


def test_deleted_null_nothing():
    """Test quand il n'y a aucune valeur NULL ou vide"""    
    mock_conn = MagicMock()
 
    mock_cursor_preview = MagicMock()
    mock_cursor_preview.fetchone.return_value = (0,)
    
    mock_conn.execute.return_value = mock_cursor_preview
    
    result = clean_nulls(mock_conn, "test_table", "email")
    
    assert result["deleted"] == 0, f"Attendu 0 lignes supprimées, obtenu {result['deleted']}"
    assert result["preview"] == 0, f"Preview devrait être 0"
    assert result["message"] == "Aucune valeur nulle ou vide à supprimer"


def test_deleted_null_with_nulls():
    """Test quand il y a des valeurs NULL ou vides"""
    mock_conn = MagicMock()
    
    mock_cursor_preview = MagicMock()
    mock_cursor_preview.fetchone.return_value = (8,)  #8 lignes NULL/vides
    
    mock_cursor_count_before = MagicMock()
    mock_cursor_count_before.fetchone.return_value = (100,) # 100 lignes avant
    
    mock_cursor_count_after = MagicMock()
    mock_cursor_count_after.fetchone.return_value = (92,)# 92 lignes après
    

    #SELECT COUNT / count_total_rows / DELETE / count_total_rows (après)
    mock_conn.execute.side_effect = [
        mock_cursor_preview,
        mock_cursor_count_before,
        None,
        mock_cursor_count_after
    ]
    
    # Exécuter le service
    result = clean_nulls(mock_conn, "test_table", "email")
    
    # Vérifier le résultat
    assert result["deleted"] == 8, f"Attendu 8 lignes supprimées, obtenu {result['deleted']}"
    assert result["preview"] == 8, f"Preview devrait être 8"
    assert "8 lignes supprimées" in result["message"]
    print("✓ Test réussi: 8 valeurs NULL/vides supprimées")


def test_deleted_null_only_nulls():
    """Test avec uniquement des valeurs NULL (pas de vides)"""
    print("\n=== Test: Uniquement des valeurs NULL ===")
    
    mock_conn = MagicMock()
    
    mock_cursor_preview = MagicMock()
    mock_cursor_preview.fetchone.return_value = (3,)  # 3 NULL
    
    mock_cursor_count_before = MagicMock()
    mock_cursor_count_before.fetchone.return_value = (50,)
    
    mock_cursor_count_after = MagicMock()
    mock_cursor_count_after.fetchone.return_value = (47,)  # 3 supprimées
    
    mock_conn.execute.side_effect = [
        mock_cursor_preview,
        mock_cursor_count_before,
        None,
        mock_cursor_count_after
    ]
    
    result = clean_nulls(mock_conn, "test_table", "phone")
    
    assert result["deleted"] == 3, f"Attendu 3 lignes supprimées, obtenu {result['deleted']}"
    assert result["preview"] == 3
    print("✓ Test réussi: 3 valeurs NULL supprimées")


def test_deleted_null_only_empty():
    """Test avec uniquement des chaines vides (pas de NULL)"""

    mock_conn = MagicMock()
    
    mock_cursor_preview = MagicMock()
    mock_cursor_preview.fetchone.return_value = (2,) # 2 vides
    
    mock_cursor_count_before = MagicMock()
    mock_cursor_count_before.fetchone.return_value = (30,)
    
    mock_cursor_count_after = MagicMock()
    mock_cursor_count_after.fetchone.return_value = (28,)# 2supprimées
    
    mock_conn.execute.side_effect = [
        mock_cursor_preview,
        mock_cursor_count_before,
        None,
        mock_cursor_count_after
    ]
    
    result = clean_nulls(mock_conn, "test_table", "address")
    
    assert result["deleted"] == 2, f"Attendu 2 lignes supprimées, obtenu {result['deleted']}"
    assert result["preview"] == 2

if __name__ == "__main__":
    test_deleted_null_nothing()
    test_deleted_null_with_nulls()
    test_deleted_null_only_nulls()
    test_deleted_null_only_empty()
    print("Tous les tests ont passé")