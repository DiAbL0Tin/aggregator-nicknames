"""
Interface en ligne de commande pour aggregator Nickname.
"""

import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from .config import load_config
from .download import download_sources
from .normalize import normalize_sources
from .dedupe import deduplicate_sources
from .export import export_data

app = typer.Typer(help="aggregator Nickname - Agrégateur de pseudos, prénoms et noms")
console = Console()


@app.command()
def run(
    config_path: str = typer.Option("config.yaml", "--config", "-c", help="Chemin vers le fichier de configuration"),
    force: bool = typer.Option(False, "--force", "-f", help="Forcer le téléchargement et le traitement même si le cache existe"),
    keep_original: bool = typer.Option(False, "--keep-original", "-k", help="Conserver les formes originales"),
    output: str = typer.Option("aggregator_nicks.txt", "--output", "-o", help="Nom du fichier de sortie"),
    high_volume: bool = typer.Option(True, "--high-volume", help="Utiliser l'approche haute performance pour les grands volumes"),
):
    """
    Exécute le pipeline complet : téléchargement, normalisation, déduplication et export.
    """
    console.print("[bold blue]Démarrage du pipeline aggregator Nickname...[/bold blue]")
    
    # Charger la configuration
    try:
        config = load_config(config_path)
        if force:
            config.defaults.force = True
    except Exception as e:
        console.print(f"[bold red]Erreur lors du chargement de la configuration: {e}[/bold red]")
        raise typer.Exit(code=1)
    
    # Exécuter le pipeline asynchrone
    try:
        source_paths = asyncio.run(download_sources(config_path))
        console.print(f"[green]✓[/green] Téléchargement terminé: {len(source_paths)} sources")
        
        normalized_paths = normalize_sources(config_path, source_paths)
        console.print(f"[green]✓[/green] Normalisation terminée: {len(normalized_paths)} sources")
        
        deduped_path = deduplicate_sources(config_path, normalized_paths, high_volume=high_volume)
        console.print(f"[green]✓[/green] Déduplication terminée")
        
        if keep_original:
            output_path = export_data(config_path, deduped_path, normalized_paths, keep_original=True)
        else:
            output_path = export_data(config_path, deduped_path, output_filename=output)
        
        console.print(f"[green]✓[/green] Export terminé: {output_path}")
        console.print("[bold green]Pipeline terminé avec succès![/bold green]")
    
    except Exception as e:
        console.print(f"[bold red]Erreur lors de l'exécution du pipeline: {e}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def download(
    config_path: str = typer.Option("config.yaml", "--config", "-c", help="Chemin vers le fichier de configuration"),
    force: bool = typer.Option(False, "--force", "-f", help="Forcer le téléchargement même si le cache existe"),
):
    """
    Télécharge les sources de données.
    """
    console.print("[bold blue]Téléchargement des sources...[/bold blue]")
    
    # Charger la configuration
    try:
        config = load_config(config_path)
        if force:
            config.defaults.force = True
    except Exception as e:
        console.print(f"[bold red]Erreur lors du chargement de la configuration: {e}[/bold red]")
        raise typer.Exit(code=1)
    
    # Exécuter le téléchargement
    try:
        source_paths = asyncio.run(download_sources(config_path))
        console.print(f"[green]✓[/green] Téléchargement terminé: {len(source_paths)} sources")
        for slug, path in source_paths.items():
            console.print(f"  - {slug}: {path}")
    except Exception as e:
        console.print(f"[bold red]Erreur lors du téléchargement: {e}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def normalize(
    config_path: str = typer.Option("config.yaml", "--config", "-c", help="Chemin vers le fichier de configuration"),
    force: bool = typer.Option(False, "--force", "-f", help="Forcer la normalisation même si le cache existe"),
):
    """
    Normalise les sources téléchargées.
    """
    console.print("[bold blue]Normalisation des sources...[/bold blue]")
    
    # Charger la configuration
    try:
        config = load_config(config_path)
        if force:
            config.defaults.force = True
    except Exception as e:
        console.print(f"[bold red]Erreur lors du chargement de la configuration: {e}[/bold red]")
        raise typer.Exit(code=1)
    
    # Exécuter le téléchargement puis la normalisation
    try:
        source_paths = asyncio.run(download_sources(config_path))
        console.print(f"[green]✓[/green] Téléchargement terminé: {len(source_paths)} sources")
        
        normalized_paths = normalize_sources(config_path, source_paths)
        console.print(f"[green]✓[/green] Normalisation terminée: {len(normalized_paths)} sources")
        for slug, path in normalized_paths.items():
            console.print(f"  - {slug}: {path}")
    except Exception as e:
        console.print(f"[bold red]Erreur lors de la normalisation: {e}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def dedupe(
    config_path: str = typer.Option("config.yaml", "--config", "-c", help="Chemin vers le fichier de configuration"),
    force: bool = typer.Option(False, "--force", "-f", help="Forcer la déduplication même si le cache existe"),
    high_volume: bool = typer.Option(True, "--high-volume", help="Utiliser l'approche haute performance pour les grands volumes"),
):
    """
    Déduplique les sources normalisées.
    """
    console.print("[bold blue]Déduplication des sources...[/bold blue]")
    
    # Charger la configuration
    try:
        config = load_config(config_path)
        if force:
            config.defaults.force = True
    except Exception as e:
        console.print(f"[bold red]Erreur lors du chargement de la configuration: {e}[/bold red]")
        raise typer.Exit(code=1)
    
    # Exécuter le téléchargement, la normalisation puis la déduplication
    try:
        source_paths = asyncio.run(download_sources(config_path))
        console.print(f"[green]✓[/green] Téléchargement terminé: {len(source_paths)} sources")
        
        normalized_paths = normalize_sources(config_path, source_paths)
        console.print(f"[green]✓[/green] Normalisation terminée: {len(normalized_paths)} sources")
        
        deduped_path = deduplicate_sources(config_path, normalized_paths, high_volume=high_volume)
        console.print(f"[green]✓[/green] Déduplication terminée: {deduped_path}")
    except Exception as e:
        console.print(f"[bold red]Erreur lors de la déduplication: {e}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def export(
    config_path: str = typer.Option("config.yaml", "--config", "-c", help="Chemin vers le fichier de configuration"),
    output: str = typer.Option("aggregator_nicks.txt", "--output", "-o", help="Nom du fichier de sortie"),
    keep_original: bool = typer.Option(False, "--keep-original", "-k", help="Conserver les formes originales"),
    high_volume: bool = typer.Option(True, "--high-volume", help="Utiliser l'approche haute performance pour les grands volumes"),
):
    """
    Exporte les données dédupliquées.
    """
    console.print("[bold blue]Export des données...[/bold blue]")
    
    # Charger la configuration
    try:
        config = load_config(config_path)
    except Exception as e:
        console.print(f"[bold red]Erreur lors du chargement de la configuration: {e}[/bold red]")
        raise typer.Exit(code=1)
    
    # Exécuter le pipeline complet
    try:
        source_paths = asyncio.run(download_sources(config_path))
        console.print(f"[green]✓[/green] Téléchargement terminé: {len(source_paths)} sources")
        
        normalized_paths = normalize_sources(config_path, source_paths)
        console.print(f"[green]✓[/green] Normalisation terminée: {len(normalized_paths)} sources")
        
        deduped_path = deduplicate_sources(config_path, normalized_paths, high_volume=high_volume)
        console.print(f"[green]✓[/green] Déduplication terminée")
        
        if keep_original:
            output_path = export_data(config_path, deduped_path, normalized_paths, keep_original=True)
        else:
            output_path = export_data(config_path, deduped_path, output_filename=output)
        
        console.print(f"[green]✓[/green] Export terminé: {output_path}")
    except Exception as e:
        console.print(f"[bold red]Erreur lors de l'export: {e}[/bold red]")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
