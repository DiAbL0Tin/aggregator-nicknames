"""
Tests unitaires pour les utilitaires de gestion de fichiers de données.
"""
from pathlib import Path
import shutil
import os
import pytest
from aggregator.utils import has_valid_data_files

def setup_test_dir(base, files):
    """Crée un dossier de test avec les fichiers spécifiés."""
    if os.path.exists(base):
        shutil.rmtree(base)
    os.makedirs(base)
    for fname in files:
        with open(os.path.join(base, fname), "w", encoding="utf-8") as f:
            f.write("test\n")

def teardown_test_dir(base):
    if os.path.exists(base):
        shutil.rmtree(base)

def test_cache_vide():
    base = "test_cache_vide"
    setup_test_dir(base, [])
    assert has_valid_data_files(Path(base), ['.txt', '.csv']) is False
    teardown_test_dir(base)

def test_cache_non_valide():
    base = "test_cache_non_valide"
    setup_test_dir(base, [".git", "README.md", "script.py"])
    assert has_valid_data_files(Path(base), ['.txt', '.csv']) is False
    teardown_test_dir(base)

def test_cache_valide():
    base = "test_cache_valide"
    setup_test_dir(base, ["data1.txt", "data2.csv", "README.md"])
    assert has_valid_data_files(Path(base), ['.txt', '.csv']) is True
    teardown_test_dir(base)

