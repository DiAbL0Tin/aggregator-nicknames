"""
Module de base pour l'orchestrateur d'aggregator Nickname.
Contient la classe principale OrchestratorBase qui initialise les chemins et la configuration.
"""

import os
from pathlib import Path
from rich.console import Console

from ..config import load_config


class OrchestratorBase:
    """Classe de base de l'orchestrateur pour aggregator Nickname."""

    def __init__(self, config_path: str):
        """
        Initialise l'orchestrateur avec la configuration.
        
        Args:
            config_path: Chemin vers le fichier de configuration
        """
        self.config_path = config_path
        self.console = Console()
        
        # Vérifier si le fichier de configuration existe
        if not os.path.exists(config_path):
            self.config = None
            self.console.print(f"[yellow]Attention: Le fichier de configuration {config_path} n'existe pas.[/yellow]")
        else:
            try:
                self.config = load_config(config_path)
                
                # Chemins des répertoires
                self.raw_dir = Path(self.config.defaults.cache_dir)
                self.normalized_dir = self.raw_dir.parent / "normalized"
                self.deduped_dir = self.raw_dir.parent / "deduped"
                self.output_dir = self.raw_dir.parent / "output"
                
                # Initialiser les statistiques
                self.stats = {
                    'sources_downloaded': 0,
                    'entries_raw': 0,
                    'entries_normalized': 0,
                    'entries_deduped': 0
                }
                
                # Chemins des sources
                self.source_paths = {}
                
                # Chemin du fichier dédupliqué
                self.deduped_path = None
                
                # Créer les répertoires s'ils n'existent pas
                for d in [self.raw_dir, self.normalized_dir, self.deduped_dir, self.output_dir]:
                    d.mkdir(parents=True, exist_ok=True)
                    
            except Exception as e:
                self.config = None
                self.console.print(f"[bold red]Erreur lors du chargement de la configuration: {e}[/bold red]")
