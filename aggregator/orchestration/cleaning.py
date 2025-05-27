"""
Module de nettoyage pour l'orchestrateur d'aggregator Nickname.
Contient toutes les fonctions liées au nettoyage des fichiers et répertoires.
"""

import os
import subprocess
import asyncio
import tempfile
import stat
import random
import time
import shutil
from pathlib import Path
from contextlib import suppress
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

# Correction : la classe Orchestrator a été renommée OrchestratorBase lors de la refactorisation modulaire.
# Pour toute logique de base, importer OrchestratorBase ; pour la logique complète, utiliser CombinedOrchestrator.
from .base import OrchestratorBase


class CleaningMixin:
    """Mixin pour les fonctionnalités de nettoyage de l'orchestrateur."""

    async def run_clear_project_strict(self):
        """
        Nettoyage radical : supprime tout sauf la liste blanche (README.md, config.yaml, pyproject.toml, poetry.lock, aggregator/, tests/, .github/).
        Vide également complètement le dossier data s'il existe.
        Utilise plusieurs stratégies robustes pour assurer la suppression complète des fichiers et dossiers, même verrouillés.
        """
        ROOT = Path(__file__).resolve().parent.parent.parent
        whitelist = {
            'README.md', 'config.yaml', 'pyproject.toml', 'poetry.lock',
            'aggregator', 'tests', '.github', 'run_menu.py'
        }
    
        # Fonction pour rendre un répertoire accessible en écriture (résoudre les problèmes de permissions)
        async def make_writable(path):
            """Rend un répertoire et son contenu accessible en écriture pour faciliter la suppression."""
            if not path.exists():
                return
                
            try:
                if path.is_file() or path.is_symlink():
                    # Rendre le fichier accessible en écriture
                    os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
                elif path.is_dir():
                    # Rendre le répertoire accessible en écriture
                    os.chmod(path, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
                    # Traiter récursivement tous les éléments du répertoire
                    for item in path.iterdir():
                        await make_writable(item)
            except Exception as e:
                self.console.print(f"[red]Erreur lors de la modification des permissions de {path}: {e}[/red]")
                
        # Fonction pour renommer puis supprimer (contourne certains verrouillages)
        async def rename_and_remove(path):
            """Renomme un répertoire puis le supprime pour contourner certains verrouillages."""
            if not path.exists():
                return True
                
            try:
                # Créer un nom temporaire aléatoire dans le même répertoire parent
                temp_name = path.parent / f"temp_delete_{random.randint(10000, 99999)}"
                
                # Essayer de renommer
                path.rename(temp_name)
                
                # Supprimer le répertoire renommé
                shutil.rmtree(temp_name, ignore_errors=True)
                return True
            except Exception as e:
                self.console.print(f"[red]Erreur lors du renommage et suppression de {path}: {e}[/red]")
                return False
                
        # Fonction pour supprimer les fichiers individuellement
        async def remove_files_individually(path):
            """Supprime les fichiers un par un pour contourner les verrouillages partiels."""
            if not path.exists() or not path.is_dir():
                return True
                
            try:
                # Rendre tous les fichiers accessibles en écriture d'abord
                await make_writable(path)
                
                # Supprimer les fichiers un par un
                for item in list(path.iterdir()):
                    try:
                        if item.is_file() or item.is_symlink():
                            item.unlink(missing_ok=True)
                        elif item.is_dir():
                            await remove_files_individually(item)
                    except Exception as e:
                        self.console.print(f"[red]Erreur lors de la suppression de {item}: {e}[/red]")
                
                # Essayer de supprimer le répertoire maintenant vide
                with suppress(Exception):
                    path.rmdir()
                return True
            except Exception as e:
                self.console.print(f"[red]Erreur lors de la suppression individuelle des fichiers dans {path}: {e}[/red]")
                return False
                
        # Fonction robuste pour supprimer un répertoire avec plusieurs tentatives et stratégies
        async def robust_rmtree(path, max_attempts=5):
            """Supprime un répertoire de manière robuste avec plusieurs tentatives et stratégies."""
            if not path.exists():
                return True
            
            self.console.print(f"[bold yellow]Tentative de suppression robuste de {path}[/bold yellow]")
            
            # Stratégies de suppression à essayer dans l'ordre
            # Fonction auxiliaire pour appeler make_writable puis supprimer
            async def make_writable_and_remove(p):
                await make_writable(p)
                shutil.rmtree(p, ignore_errors=True)
                
            strategies = [
                # Stratégie 1: shutil.rmtree standard
                lambda p: shutil.rmtree(p, ignore_errors=True),
                
                # Stratégie 2: Rendre accessible en écriture puis supprimer
                make_writable_and_remove,
                
                # Stratégie 3: Commande rmdir de Windows
                lambda p: subprocess.run(f'rmdir /s /q "{str(p)}"', shell=True, check=False),
                
                # Stratégie 4: Renommer puis supprimer (fonction locale)
                rename_and_remove,
                
                # Stratégie 5: Supprimer fichier par fichier (fonction locale)
                remove_files_individually
            ]
            
            # Essayer chaque stratégie jusqu'à ce qu'une fonctionne
            for attempt in range(max_attempts):
                strategy_index = min(attempt, len(strategies) - 1)
                try:
                    await asyncio.to_thread(strategies[strategy_index], path)
                    
                    # Vérifier si le répertoire existe encore
                    if not path.exists():
                        self.console.print(f"[green]✓ Suppression réussie de {path} (tentative {attempt+1})[/green]")
                        return True
                        
                    # Si le répertoire existe mais est vide, essayer de le supprimer directement
                    if path.is_dir() and not any(path.iterdir()):
                        try:
                            path.rmdir()
                            self.console.print(f"[green]✓ Suppression réussie du répertoire vide {path}[/green]")
                            return True
                        except Exception:
                            pass
                            
                    self.console.print(f"[yellow]Tentative {attempt+1} échouée, essai d'une autre stratégie...[/yellow]")
                    time.sleep(1)  # Pause avant la prochaine tentative
                except Exception as e:
                    self.console.print(f"[red]Erreur lors de la tentative {attempt+1}: {e}[/red]")
                    
            self.console.print(f"[bold red]Échec de toutes les tentatives de suppression pour {path}[/bold red]")
            return False
            
        # Fonction pour nettoyer les dossiers Git spécifiquement
        async def clean_git_directories(path):
            """Nettoie spécifiquement les dossiers Git qui peuvent causer des problèmes."""
            if not path.exists() or not path.is_dir():
                return
            
            # Rechercher tous les dossiers .git
            git_dirs = list(path.glob("**/.git"))
            if git_dirs:
                self.console.print(f"[bold yellow]Nettoyage de {len(git_dirs)} dossiers Git trouvés...[/bold yellow]")
                
                for git_dir in git_dirs:
                    # Supprimer d'abord les fichiers index.lock qui peuvent bloquer la suppression
                    lock_file = git_dir / "index.lock"
                    if lock_file.exists():
                        try:
                            os.chmod(lock_file, stat.S_IWRITE | stat.S_IREAD)
                            lock_file.unlink()
                            self.console.print(f"[yellow]Suppression du verrou Git : {lock_file}[/yellow]")
                        except Exception as e:
                            self.console.print(f"[red]Impossible de supprimer le verrou Git {lock_file}: {e}[/red]")
                    
                    # Supprimer le dossier .git
                    try:
                        await robust_rmtree(git_dir)
                        self.console.print(f"[green]Suppression du dossier Git : {git_dir}[/green]")
                    except Exception as e:
                        self.console.print(f"[red]Erreur lors de la suppression du dossier Git {git_dir}: {e}[/red]")
        
        # 1. Nettoyage des fichiers et dossiers à la racine (sauf liste blanche)
        self.console.print("\n[bold magenta]Nettoyage strict du projet (liste blanche)...[/bold magenta]")
        
        # Nettoyer d'abord les dossiers Git pour éviter les problèmes de verrouillage
        for item in ROOT.iterdir():
            if item.name in whitelist or not item.is_dir():
                continue
            await clean_git_directories(item)
        
        # Supprimer les éléments non whitelistés
        for item in ROOT.iterdir():
            if item.name in whitelist:
                continue
            
            try:
                if item.is_file() or item.is_symlink():
                    os.chmod(item, stat.S_IWRITE | stat.S_IREAD)
                    item.unlink()
                    self.console.print(f"[yellow]Suppression fichier : {item}[/yellow]")
                elif item.is_dir():
                    await robust_rmtree(item)
                    self.console.print(f"[green]Suppression dossier : {item}[/green]")
            except Exception as e:
                self.console.print(f"[red]Erreur suppression {item} : {e}[/red]")
        
        # 2. Nettoyage et recréation du dossier data
        data_dir = ROOT / 'data'
        if data_dir.exists():
            self.console.print("\n[bold magenta]Nettoyage complet du dossier data...[/bold magenta]")
            
            # Nettoyer d'abord les dossiers Git dans data
            await clean_git_directories(data_dir)
            
            # Supprimer le dossier data avec notre méthode robuste
            await robust_rmtree(data_dir)
        
        # Recréer le dossier data vide
        if not data_dir.exists():
            data_dir.mkdir(parents=True, exist_ok=True)
            self.console.print("[green]✓ Dossier data recréé avec succès.[/green]")
        
        # Créer les sous-dossiers standard de data
        for subdir in ['raw', 'normalized', 'output']:
            subdir_path = data_dir / subdir
            if not subdir_path.exists():
                subdir_path.mkdir(parents=True, exist_ok=True)
                self.console.print(f"[green]✓ Sous-dossier {subdir} créé.[/green]")
        
        self.console.print("[bold green]✓ Nettoyage strict terminé. Seuls les fichiers essentiels sont conservés.[/bold green]")

    # Ancienne méthode conservée pour référence ou fallback
    async def run_clear_all(self):
        """
        Vide tous les répertoires critiques du projet : raw, normalized, deduped, output, splits, final (ancienne version, conservée pour fallback).
        """
        self.console.print("\n[bold magenta]Nettoyage complet du projet : suppression de tous les fichiers et caches...[/bold magenta]")
        await self.run_clear_split_deduped()
        await self.run_clear_raw()
        await self.run_clear_normalized()
        await self.run_clear_temp()
        self.console.print("[green]✓ Projet remis à zéro. Tous les répertoires critiques sont vides.[/green]")

    async def run_clear_raw(self):
        """
        Vide le dossier des données brutes (raw).
        Cette fonction supprime tous les fichiers et sous-répertoires du dossier raw
        pour permettre un nouveau téléchargement complet des sources.
        
        La suppression est forcée pour les fichiers verrouillés, notamment les fichiers Git.
        En cas d'erreur sur un fichier ou dossier spécifique, la fonction continue
        avec les autres éléments et tente plusieurs approches de suppression.
        """
        self.console.print("\n[bold blue]Suppression du dossier raw avec progression...[/bold blue]")
        if not hasattr(self, 'raw_dir') or not self.raw_dir.exists():
            self.console.print("[yellow]Le dossier raw n'existe pas ou n'est pas configuré.[/yellow]")
            return
        # Récupérer tous les fichiers et répertoires à supprimer
        items = list(self.raw_dir.glob('*'))
        total = len(items)
        # Barre de progression Rich
        with Progress(
            SpinnerColumn(),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task("Suppression raw", total=total)
            for item in items:
                try:
                    if item.is_file():
                        os.chmod(item, stat.S_IWRITE | stat.S_IREAD)
                        item.unlink()
                    else:
                        shutil.rmtree(item, ignore_errors=True)
                except Exception as e:
                    self.console.print(f"[red]Erreur lors de la suppression de {item}: {e}[/red]")
                progress.update(task, advance=1)
        self.console.print("[green]✓ Dossier raw vidé avec succès ![/green]")
        # Réinitialiser les statistiques
        self.stats['sources_downloaded'] = 0
        self.stats['entries_raw'] = 0
        self.source_paths = {}

    async def run_clear_normalized(self):
        """
        Vide le cache des données normalisées.
        """
        self.console.print("\n[bold blue]Suppression du cache normalisé avec progression...[/bold blue]")
        dir_norm = self.normalized_dir
        if not dir_norm.exists():
            self.console.print("[yellow]Le dossier normalized n'existe pas ou n'est pas configuré.[/yellow]")
            return
        # Récupérer les fichiers et répertoires à supprimer
        items = list(dir_norm.glob('*'))
        total = len(items)
        # Barre de progression Rich
        with Progress(
            SpinnerColumn(),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task("Suppression normalized", total=total)
            for item in items:
                try:
                    if item.is_file():
                        item.unlink()
                    else:
                        shutil.rmtree(item, ignore_errors=True)
                except Exception as e:
                    self.console.print(f"[red]Erreur lors de la suppression de {item}: {e}[/red]")
                progress.update(task, advance=1)
        self.console.print("[green]✓ Cache normalisé vidé avec succès ![/green]")
    
    async def run_clear_temp(self):
        """
        Supprime les chunks, le cache dédupliqué et l'output final.
        """
        self.console.print("\n[bold blue]Suppression temporaire (splits, deduped, output) avec progression...[/bold blue]")
        targets = [self.output_dir / "splits", self.deduped_dir, self.output_dir]
        # Collecter tous les fichiers et dossiers à supprimer
        all_items = []
        for d in targets:
            if d.exists():
                for item in d.glob('*'):
                    all_items.append(item)
        total = len(all_items)
        # Barre de progression Rich
        with Progress(
            SpinnerColumn(),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task("Suppression temporaire", total=total)
            for item in all_items:
                try:
                    if item.is_file():
                        item.unlink(missing_ok=True)
                    else:
                        shutil.rmtree(item, ignore_errors=True)
                except Exception:
                    pass
                progress.update(task, advance=1)
        # Recréer les dossiers nécessaires
        for d in targets:
            if d != self.output_dir:
                d.mkdir(parents=True, exist_ok=True)
        self.console.print("[green]✓ Suppression temporaire terminée avec succès ![/green]")

    async def run_clear_split_deduped(self):
        """
        Supprime les fichiers txt générés par le split des données dédupliquées.
        """
        dir_to_clear = self.output_dir / "final"
        self.console.print("\n[bold blue]Suppression des splits dédupliqués avec progression...[/bold blue]")
        if not dir_to_clear.exists():
            self.console.print(f"[yellow]{dir_to_clear} n'existe pas.[/yellow]")
            return
        # Collecter les éléments à supprimer
        items = list(dir_to_clear.glob('*'))
        total = len(items)
        # Barre de progression Rich
        with Progress(
            SpinnerColumn(),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task("Suppression splits dédupliqués", total=total)
            for item in items:
                try:
                    if item.is_file():
                        item.unlink()
                    else:
                        shutil.rmtree(item, ignore_errors=True)
                except Exception as e:
                    self.console.print(f"[red]Erreur lors de la suppression de {item}: {e}[/red]")
                progress.update(task, advance=1)
        self.console.print("[green]✓ Splits dédupliqués supprimés avec succès ![/green]")

