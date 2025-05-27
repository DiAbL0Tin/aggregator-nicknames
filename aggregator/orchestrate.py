"""
Point d'entrée pour l'orchestrateur interactif d'aggregator Nickname.
"""

import asyncio
from pathlib import Path

# Import de typer
import typer

from aggregator.orchestrator import run_orchestrator
from aggregator.orchestration.combined import CombinedOrchestrator
from aggregator.dedupe import deduplicate_chunks

app = typer.Typer(invoke_without_command=True, help="Orchestrateur interactif pour aggregator Nickname")


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context, config_path: str = typer.Option("config.yaml", "--config", "-c", help="Chemin vers le fichier de configuration")):
    """
    Lance l'orchestrateur ou exécute la sous-commande spécifiée.
    """
    # Si aucune sous-commande n'est invoquée, lancer l'orchestrateur interactif
    if ctx.invoked_subcommand is None:
        asyncio.run(run_orchestrator(config_path))


@app.command("dedupe-chunks")
def dedupe_chunks_cli(
    config_path: str = typer.Option("config.yaml", "--config", "-c", help="Chemin vers le fichier de configuration")
) -> None:
    """
    Exécute la déduplication séquentielle des chunks de texte existants.
    """
    orchestrator = CombinedOrchestrator(config_path)
    # Répertoire des chunks
    split_dir = orchestrator.output_dir / "splits"
    if not split_dir.exists() or not any(split_dir.glob("chunk_*.txt")):
        orchestrator.console.print("[yellow]Aucun chunk à dédupliquer. Veuillez d'abord scinder les fichiers bruts en chunks.[/yellow]")
        raise typer.Exit()
    # Appel de la déduplication séquentielle
    final = deduplicate_chunks(
        split_dir,
        orchestrator.deduped_dir / "deduped_chunks.txt",
        orchestrator.console
    )
    orchestrator.console.print(f"[green]Fichier dédupliqué généré : {final}[/green]")
    # Nombre de lignes uniques
    count = sum(1 for _ in open(final, "r", encoding="utf-8"))
    orchestrator.console.print(f"[green]Total lignes uniques : {count}[/green]")


if __name__ == "__main__":
    app()

