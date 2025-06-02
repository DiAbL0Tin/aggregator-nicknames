"""
Module de gestion des logs pour aggregator Nickname.
Utilise loguru pour une gestion efficace et élégante des logs.
"""

from loguru import logger as _logger
import sys
from pathlib import Path
from typing import Optional, Any

# Supprimer le gestionnaire par défaut
_logger.remove()

def setup_logger(output_dir: Optional[Path] = None, 
                 console_level: str = "INFO", 
                 file_level: str = "ERROR",
                 retention: str = "1 week",
                 rotation: str = "10 MB") -> Any:
    """Configure le logger pour l'application.
    
    Args:
        output_dir: Répertoire de sortie pour les fichiers de log
        console_level: Niveau de log pour la console (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        file_level: Niveau de log pour le fichier (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        retention: Durée de conservation des logs (ex: "1 week", "10 days")
        rotation: Taille maximale d'un fichier de log avant rotation (ex: "10 MB", "1 GB")
        
    Returns:
        Instance configurée de loguru.logger
    """

    # Ajouter un gestionnaire pour la console avec un niveau personnalisable
    _logger.add(
        sys.stderr,
        level=console_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True
    )
    
    # Si un répertoire de sortie est fourni, ajouter un gestionnaire pour le fichier
    if output_dir:
        log_file = output_dir / "errors.log"
        
        # S'assurer que le répertoire existe
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Ajouter un gestionnaire pour le fichier
        _logger.add(
            log_file,
            level=file_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message} | {extra}",
            rotation=rotation,
            retention=retention,
            compression="zip"
        )
    
    return _logger

def get_logger() -> Any:
    """Retourne l'instance configurée du logger.
    Si le logger n'a pas de gestionnaires, en ajoute un par défaut pour la console.
    
    Returns:
        Instance configurée de loguru.logger
    """
    # Créer un gestionnaire par défaut si nécessaire
    # Nous évitons d'accéder directement à _core qui est une implémentation interne
    _logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True
    )
    
    return _logger
