"""
Module de déduplication pour aggregator Nickname.
Gère la déduplication des données normalisées.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl
from rich.console import Console
from rich.progress import Progress

from .config import Config


class Deduplicator:
    """Gestionnaire de déduplication des données."""

    def __init__(self, config: Config):
        """
        Initialise le déduplicateur avec la configuration.

        Args:
            config: Configuration validée
        """
        self.config = config
        self.console = Console()

        # Créer le répertoire de cache pour les données dédupliquées
        self.cache_dir = Path(config.defaults.cache_dir).parent / "deduped"
        self.cache_dir.mkdir(exist_ok=True)

    def deduplicate_all(self, normalized_paths: Dict[str, Path]) -> Path:
        """
        Déduplique toutes les sources normalisées et combine les résultats.
        Priorité donnée selon l'ordre des sources dans la configuration.
        
        Args:
            normalized_paths: Dictionnaire des chemins de fichiers normalisés par slug
            
        Returns:
            Path: Chemin vers le fichier dédupliqué final
        """
        output_path = self.cache_dir / "all_deduped.parquet"
        
        # Si le fichier dédupliqué existe déjà et qu'on ne force pas la déduplication
        if output_path.exists() and not self.config.defaults.force:
            return output_path
        
        self.console.print("[bold blue]Déduplication des sources...[/bold blue]")
        
        dfs = []
        
        # Traiter chaque source dans l'ordre de priorité défini dans la configuration
        with Progress() as progress:
            task = progress.add_task("[green]Traitement des sources...", total=len(normalized_paths))
            
            for source in self.config.sources:
                if source.slug in normalized_paths:
                    path = normalized_paths[source.slug]
                    try:
                        # Lire et compter les entrées pour la source
                        df_full = pl.read_parquet(path)
                        count = df_full.shape[0]
                        # Conserver uniquement la colonne 'nick'
                        dfs.append(df_full.select("nick"))
                        # Log détaillé par source
                        self.console.print(f"[blue]{source.slug} : {count} entrées lues")
                    except Exception as e:
                        self.console.print(f"[bold red]Erreur lors du traitement de {source.slug}: {e}[/bold red]")
                
                progress.update(task, advance=1)
        
        if not dfs:
            raise ValueError("Aucune source valide à traiter")
        
        # Concaténer les DataFrames (les premières occurrences ont la priorité)
        self.console.print("[bold blue]Concaténation des sources...[/bold blue]")
        combined_df = pl.concat(dfs)
        
        # Dédupliquer en conservant l'ordre
        self.console.print("[bold blue]Déduplication en cours...[/bold blue]")
        deduped_df = combined_df.unique(maintain_order=True)
        
        # Collecter et sauvegarder le résultat
        self.console.print("[bold blue]Sauvegarde du résultat...[/bold blue]")
        # Correction : un DataFrame Polars n'a pas besoin de .collect(), seulement les LazyFrame.
        deduped_df.write_parquet(output_path)
        self.console.print(f"[green]✓[/green] Déduplication terminée avec succès")
        
        return output_path
    
    def deduplicate_high_volume(self, normalized_paths: Dict[str, Path]) -> Path:
        """
        Déduplique les sources en mode batch pour très grands volumes,
        avec fallback à Polars pour garantir la complétude.
        """
        output_path = self.cache_dir / "all_deduped.parquet"
        if output_path.exists() and not self.config.defaults.force:
            return output_path

        self.console.print("[bold blue]Déduplication haute performance par batch...[/bold blue]")
        unique_values: set[str] = set()
        batch_size = 10_000_000

        # Import pyarrow.dataset si disponible
        try:
            import pyarrow.dataset as ds  # type: ignore
        except ImportError:
            ds = None
            self.console.print("[yellow]pyarrow.dataset non disponible, fallback via Polars[/yellow]")

        for source in self.config.sources:
            slug = source.slug
            if slug not in normalized_paths:
                continue
            path = normalized_paths[slug]

            if ds:
                try:
                    # Lecture par batch via Arrow Dataset
                    dataset = ds.dataset(str(path), format="parquet")
                    scanner = dataset.scanner(columns=["nick"], batch_size=batch_size)
                    for batch in scanner.to_batches():
                        unique_values.update(batch.column("nick").to_pylist())
                    self.console.print(f"[green]{slug} (Arrow): maintenant {len(unique_values)} uniques[/green]")
                    continue
                except Exception as e:
                    self.console.print(f"[yellow]Arrow scan {slug} échoué: {e} – fallback Polars[/yellow]")

            # Fallback Polars pour ce slug
            try:
                old = len(unique_values)
                df = pl.read_parquet(path).select("nick")
                unique_values.update(df["nick"].to_list())
                self.console.print(f"[green]{slug} (Polars): +{len(unique_values)-old} uniques, total {len(unique_values)}[/green]")
            except Exception as e:
                self.console.print(f"[bold red]Erreur fallback Polars {slug}: {e}[/bold red]")

        if not unique_values:
            raise ValueError("Aucune donnée valide à dédupliquer")

        # Sauvegarde du résultat
        deduped_df = pl.DataFrame({"nick": list(unique_values)})
        deduped_df.write_parquet(output_path)
        self.console.print(f"[green]✓ Déduplication terminée. Entrées uniques: {deduped_df.height}[/green]")
        return output_path


def deduplicate_sources(config_path: str, normalized_paths: Dict[str, Path], high_volume: bool = True) -> Path:
    """
    Fonction principale pour dédupliquer toutes les sources.
    
    Args:
        config_path: Chemin vers le fichier de configuration
        normalized_paths: Dictionnaire des chemins de fichiers normalisés par slug
        high_volume: Utiliser l'approche haute performance pour les grands volumes
        
    Returns:
        Path: Chemin vers le fichier dédupliqué final
    """
    from .config import load_config
    
    config = load_config(config_path)
    deduplicator = Deduplicator(config)
    
    if high_volume:
        return deduplicator.deduplicate_high_volume(normalized_paths)
    else:
        return deduplicator.deduplicate_all(normalized_paths)


# Déduplication séquentielle des chunks de texte brut
def deduplicate_chunks(chunk_dir: Path, output_path: Path, console: Console = None) -> Path:
    """
    Déduplique séquentiellement les fichiers chunk_XXX.txt dans chunk_dir.
    Args:
        chunk_dir: Répertoire contenant les chunks à dédupliquer
        output_path: Fichier de sortie sans doublon
        console: Console Rich pour l'affichage des logs
    Returns:
        Path: Chemin vers le fichier résultant sans doublon
    """
    seen = set()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as out_f:
        for chunk_file in sorted(chunk_dir.glob("chunk_*.txt")):
            if console:
                console.print(f"[blue]Traitement de {chunk_file.name}…[/blue]")
            with chunk_file.open("r", encoding="utf-8", errors="ignore") as in_f:
                for line in in_f:
                    line_stripped = line.rstrip("\n")
                    if line_stripped not in seen:
                        seen.add(line_stripped)
                        out_f.write(line)
    if console:
        console.print(f"[green]✓ Déduplication des chunks terminée. Entrées uniques: {len(seen)}[/green]")
    return output_path
