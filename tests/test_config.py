"""
Tests unitaires pour le module de configuration.
"""

import os
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from aggregator.config import Config, Source, Defaults, load_config


def test_source_model():
    """Test de la validation du modèle Source."""
    # Test avec des données valides
    source = Source(
        slug="test",
        type="git",
        ref="test_ref",
        repo="test/repo"
    )
    assert source.slug == "test"
    assert source.type == "git"
    assert source.ref == "test_ref"
    assert source.repo == "test/repo"
    
    # Test avec un type invalide
    with pytest.raises(ValidationError):
        Source(
            slug="test",
            type="invalid_type",
            ref="test_ref"
        )


def test_defaults_model():
    """Test de la validation du modèle Defaults."""
    # Test avec des valeurs par défaut
    defaults = Defaults()
    assert defaults.cache_dir == "data/raw"
    assert defaults.force is False
    assert defaults.workers == 32
    
    # Test avec des valeurs personnalisées
    defaults = Defaults(
        cache_dir="custom/path",
        force=True,
        workers=16
    )
    assert defaults.cache_dir == "custom/path"
    assert defaults.force is True
    assert defaults.workers == 16


def test_config_model():
    """Test de la validation du modèle Config."""
    # Test avec des données valides
    config = Config(
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
            cache_dir="custom/path",
            force=True,
            workers=16
        )
    )
    assert len(config.sources) == 2
    assert config.sources[0].slug == "test1"
    assert config.sources[1].slug == "test2"
    assert config.defaults.cache_dir == "custom/path"


def test_load_config():
    """Test du chargement de la configuration depuis un fichier YAML."""
    # Créer un fichier YAML temporaire
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("""
sources:
  - slug: test1
    type: git
    ref: test_ref1
    repo: test/repo1
  - slug: test2
    type: http
    ref: test_ref2
    url: http://example.com/data.zip
defaults:
  cache_dir: custom/path
  force: true
  workers: 16
        """)
    
    # Charger la configuration
    config = load_config(f.name)
    
    # Vérifier la configuration
    assert len(config.sources) == 2
    assert config.sources[0].slug == "test1"
    assert config.sources[0].type == "git"
    assert config.sources[0].ref == "test_ref1"
    assert config.sources[0].repo == "test/repo1"
    assert config.sources[1].slug == "test2"
    assert config.sources[1].type == "http"
    assert config.sources[1].ref == "test_ref2"
    assert config.sources[1].url == "http://example.com/data.zip"
    assert config.defaults.cache_dir == "custom/path"
    assert config.defaults.force is True
    assert config.defaults.workers == 16
    
    # Supprimer le fichier temporaire
    os.unlink(f.name)


def test_load_config_invalid_yaml():
    """Test du chargement d'un fichier YAML invalide."""
    # Créer un fichier YAML temporaire invalide
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("invalid: yaml: content:")
    
    # Vérifier que le chargement échoue
    with pytest.raises(Exception):
        load_config(f.name)
    
    # Supprimer le fichier temporaire
    os.unlink(f.name)

