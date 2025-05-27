"""
Utilitaires pour la gestion des fichiers de données et du cache.
"""
from pathlib import Path
from typing import List

"""
Utilitaires pour la gestion des fichiers de données et du cache.
"""
from pathlib import Path
from typing import List

def is_valid_data_file(path: Path, valid_exts: List[str]) -> bool:
    """
    Vérifie si un fichier correspond à une extension de données valide.
    Args:
        path: Chemin du fichier à tester
        valid_exts: Liste des extensions valides (avec le point)
    Returns:
        bool: True si le fichier est un fichier de données valide
    """
    return path.is_file() and path.suffix.lower() in valid_exts and not path.name.startswith('.')

def has_valid_data_files(directory: Path, valid_exts: List[str]) -> bool:
    """
    Parcourt récursivement un dossier pour détecter au moins un fichier de données valide.
    Args:
        directory: Dossier à parcourir
        valid_exts: Extensions considérées comme valides
    Returns:
        bool: True si au moins un fichier de données est trouvé
    """
    try:
        for f in directory.rglob('*'):
            if is_valid_data_file(f, valid_exts):
                return True
    except (PermissionError, FileNotFoundError):
        pass
    return False
