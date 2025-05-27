"""
Module d'export pour aggregator Nickname.
Gère l'export des données dédupliquées vers le format final.
"""

import os
from pathlib import Path
from typing import List, Optional

import polars as pl
from rich.console import Console
from rich.progress import Progress
from tqdm import tqdm

from .config import Config


class Exporter:
    """Gestionnaire d'export des données."""

    def __init__(self, config: Config):
        """
        Initialise l'exporteur avec la configuration.
        
        Args:
            config: Configuration validée
        """
        self.config = config
        self.console = Console()
        
        # Créer le répertoire de sortie
        self.output_dir = Path(config.defaults.cache_dir).parent / "output"
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def export_streaming(self, deduped_path: Path, output_filename: str = "aggregator_nicks.txt", chunk_size: int = 1_000_000) -> Path:
        """
        Exporte les données dédupliquées en streaming par blocs.
        
        Args:
            deduped_path: Chemin vers le fichier dédupliqué
            output_filename: Nom du fichier de sortie
            chunk_size: Taille des blocs pour l'export streaming
            
        Returns:
            Path: Chemin vers le fichier exporté
        """
        output_path = self.output_dir / output_filename
        
        self.console.print(f"[bold blue]Export streaming vers {output_filename}...[/bold blue]")
        
        # Déterminer le type de fichier et le lire en conséquence
        suffix = deduped_path.suffix.lower()
        
        if suffix == ".parquet":
            try:
                # Lecture du fichier parquet
                df = pl.read_parquet(deduped_path)
                total_count = len(df)
                
                # Ouvrir le fichier de sortie et écrire par blocs en mémoire
                with open(output_path, "w", encoding="utf-8") as f:
                    with tqdm(total=total_count, desc="Export", unit="entries") as pbar:
                        nicks = df["nick"].to_list()
                        for idx in range(0, total_count, chunk_size):
                            chunk = nicks[idx: idx + chunk_size]
                            f.write(",".join(chunk) + ",\n")
                            pbar.update(len(chunk))
            except Exception as e:
                self.console.print(f"[bold red]Erreur lors de la lecture du fichier Parquet: {e}[/bold red]")
                raise
        
        else:  # Traiter comme un fichier texte (.txt, .csv, etc.)
            try:
                # Compter le nombre total de lignes dans le fichier
                with open(deduped_path, "r", encoding="utf-8") as f:
                    total_count = sum(1 for _ in f)
                
                # Traiter et exporter le fichier texte
                with open(deduped_path, "r", encoding="utf-8") as source:
                    with open(output_path, "w", encoding="utf-8") as target:
                        with tqdm(total=total_count, desc="Export", unit="lines") as pbar:
                            lines_buffered = []
                            line_count = 0
                            
                            for line in source:
                                line = line.strip()
                                if line:  # Ignorer les lignes vides
                                    lines_buffered.append(line)
                                    line_count += 1
                                    
                                    if line_count % chunk_size == 0:
                                        target.write(",".join(lines_buffered) + ",\n")
                                        lines_buffered = []
                                        pbar.update(chunk_size)
                            
                            # Écrire les lignes restantes
                            if lines_buffered:
                                target.write(",".join(lines_buffered) + ",\n")
                                pbar.update(len(lines_buffered))
            except Exception as e:
                self.console.print(f"[bold red]Erreur lors de la lecture du fichier texte: {e}[/bold red]")
                raise
        
        self.console.print(f"[green]✓[/green] Export terminé avec succès")
        self.console.print(f"[blue]Fichier exporté: {output_path}[/blue]")
        self.console.print(f"[blue]Nombre total d'entrées/lignes: {total_count}[/blue]")
        
        return output_path
    
    def export_with_original(self, deduped_path: Path, normalized_paths: dict, output_filename: str = "aggregator_nicks_with_original.parquet") -> Path:
        """
        Exporte les données dédupliquées avec les formes originales.
        
        Args:
            deduped_path: Chemin vers le fichier dédupliqué
            normalized_paths: Dictionnaire des chemins de fichiers normalisés par slug
            output_filename: Nom du fichier de sortie
            
        Returns:
            Path: Chemin vers le fichier exporté
        """
        output_path = self.output_dir / output_filename
        
        self.console.print(f"[bold blue]Export avec formes originales vers {output_filename}...[/bold blue]")
        
        # Créer un dictionnaire pour stocker les correspondances {clean: original}
        clean_to_original = {}
        
        # Parcourir toutes les sources normalisées dans l'ordre de priorité
        with Progress() as progress:
            task = progress.add_task("[green]Traitement des sources...", total=len(normalized_paths))
            
            for source in self.config.sources:
                if source.slug in normalized_paths:
                    path = normalized_paths[source.slug]
                    try:
                        # Lire le fichier parquet
                        df = pl.read_parquet(path)
                        
                        # Si le DataFrame contient une colonne "original"
                        if "original" in df.columns:
                            # Parcourir les lignes et mettre à jour le dictionnaire
                            for row in df.iter_rows():
                                clean, original = row
                                # Ne pas écraser les valeurs existantes (priorité aux premières sources)
                                if clean not in clean_to_original:
                                    clean_to_original[clean] = original
                    except Exception as e:
                        self.console.print(f"[bold red]Erreur lors du traitement de {source.slug}: {e}[/bold red]")
                
                progress.update(task, advance=1)
        
        # Lecture et écriture via Python pur
        deduped_df = pl.read_parquet(deduped_path)
        nicks = deduped_df["nick"].to_list()
        originals = [clean_to_original.get(n, n) for n in nicks]
        output_df = pl.DataFrame({"nick": nicks, "original": originals})
        output_df.write_parquet(output_path)
        
        self.console.print(f"[green]✓[/green] Export avec formes originales terminé avec succès")
        self.console.print(f"[blue]Fichier exporté: {output_path}[/blue]")
        
        return output_path


def export_data(config_path: str, deduped_path: Path, normalized_paths: Optional[dict] = None, keep_original: bool = False) -> Path:
    """
    Fonction principale pour exporter les données.
    
    Args:
        config_path: Chemin vers le fichier de configuration
        deduped_path: Chemin vers le fichier dédupliqué
        normalized_paths: Dictionnaire des chemins de fichiers normalisés par slug (requis si keep_original=True)
        keep_original: Conserver les formes originales
        
    Returns:
        Path: Chemin vers le fichier exporté
    """
    from .config import load_config
    
    config = load_config(config_path)
    exporter = Exporter(config)
    
    if keep_original:
        if normalized_paths is None:
            raise ValueError("normalized_paths est requis lorsque keep_original=True")
        return exporter.export_with_original(deduped_path, normalized_paths)
    else:
        return exporter.export_streaming(deduped_path)
