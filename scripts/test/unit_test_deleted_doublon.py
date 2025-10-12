import pytest
from unittest.mock import Mock, MagicMock, patch
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from doublon_service import clean_duplicates
from doublon_repository import table_exists, get_table_columns


def test_deleted_doublon_nothing():
    """Test quand il n'y a aucun doublon"""
    
    mock_conn = MagicMock()
    
    mock_cursor_preview = MagicMock()
    mock_cursor_preview.fetchone.return_value = (0,) #0 doublon
    
    mock_conn.execute.return_value = mock_cursor_preview
    
    result = clean_duplicates(mock_conn, "test_table", "id")

    assert result["deleted"] == 0, f"Attendu 0 doublons, obtenu {result['deleted']}"
    assert result["preview"] == 0, f"Preview devrait être 0"
    assert result["message"] == "Aucun doublon à supprimer"

def test_deleted_doublon_with_doublon():
    """Test quand il y a des doublons"""
    
    mock_conn = MagicMock()
    
    mock_cursor_preview = MagicMock()
    mock_cursor_preview.fetchone.return_value = (5,)#5 doublons
    
    mock_cursor_count_before = MagicMock()
    mock_cursor_count_before.fetchone.return_value = (100,)# 100 lignes avant
    
    mock_cursor_count_after = MagicMock()
    mock_cursor_count_after.fetchone.return_value = (95,) # 95 lignes après (5 supprimées)
    
    #side_effect pour retourner différentes valeurs à chaque appel
    # preview_deletation / count_befor / DELETE pas de retour / count_after
    mock_conn.execute.side_effect = [
        mock_cursor_preview,
        mock_cursor_count_before,
        None,
        mock_cursor_count_after
    ]
    
    result = clean_duplicates(mock_conn, "test_table", "id")
    
    assert result["deleted"] == 5, f"Attendu 5 doublons supprimés, obtenu {result['deleted']}"
    assert result["preview"] == 5, f"Preview devrait être 5"
    assert "5 lignes supprimées" in result["message"]


try:
    test_deleted_doublon_nothing()
    test_deleted_doublon_with_doublon()
except Exception as e:
    print(f"[UNIT TEST ERREUR] Erreur {e}")
finally:
    if 'conn' in locals():
        conn.close()
        print("\n Connexion fermée")