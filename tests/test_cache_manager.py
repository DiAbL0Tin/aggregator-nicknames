"""
Tests unitaires pour le gestionnaire de cache : suppression des dossiers obsolètes.
"""
import os
import shutil
from pathlib import Path
from aggregator.cache_manager import clean_cache

def setup_cache_dir(base, slugs):
    if os.path.exists(base):
        shutil.rmtree(base)
    os.makedirs(base)
    for slug in slugs:
        os.makedirs(os.path.join(base, slug))

def teardown_cache_dir(base):
    if os.path.exists(base):
        shutil.rmtree(base)

def test_clean_cache():
    base = "test_cache"
    valid = ["slug1", "slug3"]
    # Création de dossiers valides et obsolètes
    setup_cache_dir(base, valid + ["old1", "old2"])
    cache_dir = Path(base)
    assert (cache_dir / "old1").exists()
    assert (cache_dir / "old2").exists()
    # Nettoyage
    n = clean_cache(cache_dir, valid)
    assert n == 2
    assert not (cache_dir / "old1").exists()
    assert not (cache_dir / "old2").exists()
    assert (cache_dir / "slug1").exists()
    assert (cache_dir / "slug3").exists()
    teardown_cache_dir(base)

