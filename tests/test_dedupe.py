"""
Tests unitaires pour le module de déduplication.
"""

import os
import tempfile
from pathlib import Path

import polars as pl
import pytest

from aggregator.config import Config, Source, Defaults
from aggregator.dedupe import Deduplicator, deduplicate_sources


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
def test_normalized_paths(tmp_path):
    """Fixture pour créer des chemins normalisés de test."""
    # Créer des fichiers parquet de test
    paths = {}
    
    # Premier fichier avec des données
    df1 = pl.DataFrame({"nick": ["user1", "user2", "user3", "common"]})
    path1 = tmp_path / "test1.parquet"
    df1.write_parquet(path1)
    paths["test1"] = path1
    
    # Deuxième fichier avec des données (incluant des doublons)
    df2 = pl.DataFrame({"nick": ["user4", "user5", "common", "user3"]})
    path2 = tmp_path / "test2.parquet"
    df2.write_parquet(path2)
    paths["test2"] = path2
    
    return paths


def test_deduplicator_init(test_config):
    """Test de l'initialisation du déduplicateur."""
    deduplicator = Deduplicator(test_config)
    assert deduplicator.config == test_config
    assert deduplicator.cache_dir == Path("data/deduped")
    assert deduplicator.cache_dir.exists()


def test_deduplicate_all(test_config, test_normalized_paths):
    """Test de la déduplication de toutes les sources."""
    deduplicator = Deduplicator(test_config)
    
    # Dédupliquer les sources
    output_path = deduplicator.deduplicate_all(test_normalized_paths)
    
    # Vérifier que le fichier de sortie existe
    assert output_path.exists()
    
    # Lire le fichier de sortie
    df = pl.read_parquet(output_path)
    
    # Vérifier les résultats
    assert df.shape[0] == 6  # 6 entrées uniques au total
    assert "nick" in df.columns
    
    # Vérifier que l'ordre de priorité est respecté (test1 avant test2)
    # Les doublons de test2 ne doivent pas apparaître
    nick_list = df["nick"].to_list()
    assert "user1" in nick_list
    assert "user2" in nick_list
    assert "user3" in nick_list  # De test1, pas de test2
    assert "common" in nick_list  # De test1, pas de test2
    assert "user4" in nick_list
    assert "user5" in nick_list


def test_deduplicate_high_volume(test_config, test_normalized_paths):
    """Test de la déduplication haute performance."""
    deduplicator = Deduplicator(test_config)
    
    # Dédupliquer les sources avec l'approche haute performance
    output_path = deduplicator.deduplicate_high_volume(test_normalized_paths)
    
    # Vérifier que le fichier de sortie existe
    assert output_path.exists()
    
    # Lire le fichier de sortie
    df = pl.read_parquet(output_path)
    
    # Vérifier les résultats
    assert df.shape[0] == 6  # 6 entrées uniques au total
    assert "nick" in df.columns
    
    # Vérifier que toutes les entrées uniques sont présentes
    nick_set = set(df["nick"].to_list())
    assert "user1" in nick_set
    assert "user2" in nick_set
    assert "user3" in nick_set
    assert "user4" in nick_set
    assert "user5" in nick_set
    assert "common" in nick_set

