"""
Gestionnaire du cache pour nettoyage automatique des dossiers obsolètes ou corrompus.
"""
from pathlib import Path
from typing import List
import shutil

def clean_cache(cache_dir: Path, valid_slugs: List[str]) -> int:
    """
    Supprime les dossiers du cache qui ne correspondent à aucune source valide.
    Args:
        cache_dir: Dossier racine du cache
        valid_slugs: Liste des slugs valides (sources attendues)
    Returns:
        int: Nombre de dossiers supprimés
    """
    count = 0
    if not cache_dir.exists():
        return 0
    for sub in cache_dir.iterdir():
        if sub.is_dir() and sub.name not in valid_slugs:
            shutil.rmtree(sub)
            count += 1
    return count
