"""
Tests unitaires pour le module d'export.
"""

import os
import tempfile
from pathlib import Path

import polars as pl
import pytest

from aggregator.config import Config, Source, Defaults
from aggregator.export import Exporter, export_data


@pytest.fixture
def test_config():
    """Fixture pour créer une configuration de test."""
    return Config(
        sources=[
            Source(
                slug="test1",
                type="git",
                ref="test_ref1",
                repo="test/repo1"
            ),
            Source(
                slug="test2",
                type="http",
                ref="test_ref2",
                url="http://example.com/data.zip"
            )
        ],
        defaults=Defaults(
            cache_dir="data/raw",
            force=True,
            workers=16
        )
    )


@pytest.fixture
def test_deduped_path(tmp_path):
    """Fixture pour créer un fichier dédupliqué de test."""
    # Créer un fichier parquet de test
    df = pl.DataFrame({"nick": ["user1", "user2", "user3", "user4", "user5"]})
    path = tmp_path / "deduped.parquet"
    df.write_parquet(path)
    return path


@pytest.fixture
def test_normalized_paths(tmp_path):
    """Fixture pour créer des chemins normalisés de test avec formes originales."""
    # Créer des fichiers parquet de test
    paths = {}
    
    # Premier fichier avec des données
    df1 = pl.DataFrame({
        "nick": ["user1", "user2", "user3"],
        "original": ["User1", "USER2", "User_3"]
    })
    path1 = tmp_path / "test1.parquet"
    df1.write_parquet(path1)
    paths["test1"] = path1
    
    # Deuxième fichier avec des données
    df2 = pl.DataFrame({
        "nick": ["user4", "user5"],
        "original": ["User-4", "User.5"]
    })
    path2 = tmp_path / "test2.parquet"
    df2.write_parquet(path2)
    paths["test2"] = path2
    
    return paths


def test_exporter_init(test_config):
    """Test de l'initialisation de l'exporteur."""
    exporter = Exporter(test_config)
    assert exporter.config == test_config
    assert exporter.output_dir == Path("data/output")
    assert exporter.output_dir.exists()


def test_export_streaming(test_config, test_deduped_path):
    """Test de l'export streaming."""
    exporter = Exporter(test_config)
    
    # Exporter les données
    output_path = exporter.export_streaming(test_deduped_path, "test_output.txt", chunk_size=2)
    
    # Vérifier que le fichier de sortie existe
    assert output_path.exists()
    
    # Lire le fichier de sortie
    with open(output_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Vérifier le contenu
    assert "user1,user2," in content
    assert "user3,user4," in content
    assert "user5," in content
    assert content.endswith(",\n")


def test_export_with_original(test_config, test_deduped_path, test_normalized_paths):
    """Test de l'export avec formes originales."""
    exporter = Exporter(test_config)
    
    # Exporter les données avec les formes originales
    output_path = exporter.export_with_original(test_deduped_path, test_normalized_paths)
    
    # Vérifier que le fichier de sortie existe
    assert output_path.exists()
    
    # Lire le fichier de sortie
    df = pl.read_parquet(output_path)
    
    # Vérifier les colonnes
    assert "nick" in df.columns
    assert "original" in df.columns
    
    # Vérifier que les formes originales sont correctes
    nick_to_original = {row[0]: row[1] for row in df.iter_rows()}
    assert nick_to_original["user1"] == "User1"
    assert nick_to_original["user2"] == "USER2"
    assert nick_to_original["user3"] == "User_3"
    assert nick_to_original["user4"] == "User-4"
    assert nick_to_original["user5"] == "User.5"

