"""
Module de nettoyage pour l'orchestrateur d'aggregator Nickname.
Contient toutes les fonctions liées au nettoyage des fichiers et répertoires.
"""

import os
import subprocess
import stat
import random
import time
import shutil
from pathlib import Path
from contextlib import suppress
from typing import Optional, Union, Dict, List
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
# Ce mixin est utilisé avec OrchestratorBase mais n'en hérite pas directement
from rich.console import Console


class CleaningMixin:
    """Mixin pour les fonctionnalités de nettoyage de l'orchestrateur."""
    
    # Attributs hérités de OrchestratorBase
    console: 'Console'
    raw_dir: Path
    normalized_dir: Path
    deduped_dir: Path
    output_dir: Path
    deduped_path: Optional[Path]
    stats: Dict[str, int]

    async def run_clear_project_strict(self):
        """
        Nettoyage radical : supprime tout sauf la liste blanche (README.md, config.yaml, pyproject.toml, poetry.lock, aggregator/, tests/, .github/).
        Vide également complètement le dossier data s'il existe.
        Utilise plusieurs stratégies robustes pour assurer la suppression complète des fichiers et dossiers, même verrouillés.
        """
        ROOT = Path(__file__).resolve().parent.parent.parent
        whitelist = {
            'README.md', 'config.yaml', 'pyproject.toml', 'poetry.lock',
            'aggregator', 'tests', '.github', 'run_menu.py', 'LICENSE', '.git'
        }
    
        # Fonction pour rendre un répertoire accessible en écriture (résoudre les problèmes de permissions)
        async def make_writable(path: Path) -> None:
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
        async def rename_and_remove(path: Path) -> bool:
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
        async def remove_files_individually(path: Path) -> bool:
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
        async def robust_rmtree(path: Path, max_attempts: int = 5) -> bool:
            """Supprime un répertoire de manière robuste avec plusieurs tentatives et stratégies."""
            if not path.exists():
                return True
            
            self.console.print(f"[bold yellow]Tentative de suppression robuste de {path}[/bold yellow]")
            
            # Stratégies de suppression à essayer dans l'ordre
            # Fonction auxiliaire pour appeler make_writable puis supprimer
            async def make_writable_and_remove(p: Path) -> None:
                await make_writable(p)
                shutil.rmtree(p, ignore_errors=True)
            
            # Fonction pour utiliser subprocess
            def subprocess_remove(p: Path) -> subprocess.CompletedProcess[bytes]:
                if os.name == 'nt':  # Windows
                    return subprocess.run(f'rmdir /s /q "{p}"', shell=True)
                else:  # Unix/Linux
                    return subprocess.run(['rm', '-rf', str(p)])
                
            # Type des stratégies : Callable[[Path], Awaitable[Any]]
            
            # Convertir les fonctions synchrones en asynchrones pour uniformiser les types
            async def sync_rmtree(p: Path) -> None:
                shutil.rmtree(p, ignore_errors=True)
                
            async def sync_subprocess_remove(p: Path) -> subprocess.CompletedProcess[bytes]:
                return subprocess_remove(p)
                
            strategies = [
                # Liste explicitement typée de fonctions asynchrones traitant des Path
                # Stratégie 1: shutil.rmtree standard
                sync_rmtree,
                
                # Stratégie 2: utiliser subprocess pour appeler rm -rf (Windows: rmdir /s /q)
                sync_subprocess_remove,
                
                # Stratégie 3: Rendre accessible en écriture puis supprimer
                make_writable_and_remove,
                
                # Stratégie 4: Renommer puis supprimer (fonction locale)
                rename_and_remove,
                
                # Stratégie 5: Suppression des fichiers individuels puis rmdir
                remove_files_individually
            ]
            
            # Essayer chaque stratégie jusqu'à ce qu'une fonctionne
            for attempt in range(max_attempts):
                strategy_index = min(attempt, len(strategies) - 1)
                try:
                    # Appel direct de la fonction asynchrone avec son paramètre
                    await strategies[strategy_index](path)
                    
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
        async def clean_git_directories(path: Path) -> None:
            """Nettoie spécifiquement les dossiers Git qui peuvent causer des problèmes."""
            if not path.exists() or not path.is_dir():
                return
            
            # Rechercher tous les dossiers .git, mais exclure le dossier .git principal s'il est dans la whitelist
            git_dirs = []
            for git_dir in path.glob("**/.git"):
                # Vérifier si c'est le dossier .git principal (à la racine)
                if git_dir.parent == ROOT and '.git' in whitelist:
                    self.console.print(f"[bold blue]Protection du dossier Git principal (.git est dans la whitelist)[/bold blue]")
                    continue
                git_dirs.append(git_dir)
                
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
                    
                    # Supprimer le dossier .git (sauf celui à la racine qui est protégé)
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
            
            # Nous n'avons pas besoin de nettoyer les dossiers Git dans data car nous allons supprimer tout le dossier
            # Vérifions juste qu'il n'y a pas un lien symbolique vers le .git principal pour éviter de le supprimer
            root_git_dir = ROOT / '.git'
            if root_git_dir.exists() and '.git' in whitelist:
                for git_dir in data_dir.glob("**/.git"):
                    if git_dir.resolve() == root_git_dir.resolve():
                        self.console.print(f"[bold red]Attention! Détection d'un lien vers le .git principal dans {git_dir}, protection activée[/bold red]")
                        # Ne pas supprimer data_dir directement, mais plutôt son contenu item par item
                        for item in data_dir.iterdir():
                            if item.name != '.git':
                                if item.is_file() or item.is_symlink():
                                    item.unlink()
                                elif item.is_dir():
                                    await robust_rmtree(item)
                        break
                else:  # Ce else appartient au for, il s'exécute si aucun break n'est rencontré
                    # Supprimer le dossier data avec notre méthode robuste
                    await robust_rmtree(data_dir)
            else:
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
    async def run_clear_all(self) -> None:
        """
        Vide tous les répertoires critiques du projet : raw, normalized, deduped, output, splits, final (ancienne version, conservée pour fallback).
        """
        # La version moderne utilise des opérations spécifiques plutôt que ce nettoyage global
        self.console.print("\n[bold yellow]ATTENTION: Cette fonction est dépréciée. Utilisez plutôt les fonctions spécifiques:[/bold yellow]")
        self.console.print("- [cyan]run_clear_raw[/cyan] pour vider le cache de données brutes")
        self.console.print("- [cyan]run_clear_normalized[/cyan] pour vider le cache de données normalisées")
        self.console.print("- [cyan]run_clear_temp[/cyan] pour vider les caches temporaires")
        
        await self.run_clear_raw()
        await self.run_clear_normalized()
        await self.run_clear_temp()  
        
        self.console.print("[green]✓ Tous les répertoires vidés avec succès ![/green]")

    async def run_clear_raw(self) -> None:
        """
        Vide le dossier des données brutes (raw).
        Cette fonction supprime tous les fichiers et sous-répertoires du dossier raw
        pour permettre un nouveau téléchargement complet des sources.
        
        La suppression est forcée pour les fichiers verrouillés, notamment les fichiers Git.
        En cas d'erreur sur un fichier ou dossier spécifique, la fonction continue
        avec les autres éléments et tente plusieurs approches de suppression.
        """
        self.console.print("\n[bold blue]Nettoyage des données brutes avec progression...[/bold blue]")
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

    async def run_clear_normalized(self) -> None:
        """
        Vide le cache des données normalisées.
        """
        self.console.print("\n[bold blue]Nettoyage du cache normalisé avec progression...[/bold blue]")
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
    
    async def run_clear_temp(self) -> None:
        """
        Supprime les chunks, le cache dédupliqué et l'output final.
        """
        self.console.print("\n[bold blue]Suppression temporaire (splits, deduped, output) avec progression...[/bold blue]")
        targets = [self.output_dir / "splits", self.deduped_dir, self.output_dir]
        # Collecter tous les fichiers et dossiers à supprimer
        all_items: List[Path] = []
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
                        shutil.rmtree(str(item), ignore_errors=True)
                except Exception:
                    pass
                progress.update(task, advance=1)
        # Recréer les dossiers nécessaires
        for d in targets:
            if d != self.output_dir:
                d.mkdir(parents=True, exist_ok=True)
        self.console.print("[green]✓ Suppression temporaire terminée avec succès ![/green]")

    async def run_clear_split_deduped(self) -> None:
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

