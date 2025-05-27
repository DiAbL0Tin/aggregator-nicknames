"""
Module d'utilitaires pour l'orchestrateur d'aggregator Nickname.
Contient des fonctions utilitaires communes utilisées par différents modules.
"""

import asyncio
import os
import shutil
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from rich.console import Console

from ..download import download_source_data, is_binary_file, validate_downloaded_files
from ..export import export_data
from ..dedupe import deduplicate_chunks


class UtilsMixin:
    """Mixin pour les fonctionnalités utilitaires de l'orchestrateur."""

    async def run_download_sources(self):
        """
        Télécharge les sources définies dans la configuration.
        """
        self.console.print("\n[bold blue]Téléchargement des sources...[/bold blue]")
        
        if not self.config or not hasattr(self.config, 'sources') or not self.config.sources:
            self.console.print("[bold red]Aucune source définie dans la configuration.[/bold red]")
            return
            
        # Créer le répertoire raw s'il n'existe pas
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        
        # Télécharger chaque source
        for source in self.config.sources:
            self.console.print(f"[cyan]Téléchargement de {source.slug}...[/cyan]")
            try:
                paths = await download_source_data(
                    source,
                    self.raw_dir,
                    self.console
                )
                
                if paths:
                    self.source_paths[source.slug] = paths
                    self.stats['sources_downloaded'] += 1
                    
                    # Compter les entrées brutes
                    for path in paths:
                        if path.exists() and path.is_file():
                            try:
                                # Vérifier s'il s'agit d'un fichier texte avant d'essayer de le lire
                                # Liste des extensions de fichiers binaires à ignorer
                                binary_extensions = ['.zip', '.rar', '.gz', '.tar', '.7z', '.exe', '.bin', '.dat', '.pdf', '.png', '.jpg', '.jpeg', '.gif']
                                
                                # Ignorer les fichiers binaires
                                if any(str(path).lower().endswith(ext) for ext in binary_extensions):
                                    self.console.print(f"[yellow]Fichier binaire ignoré pour le comptage: {path}[/yellow]")
                                    continue
                                    
                                # Essayer de lire le fichier comme texte
                                count = sum(1 for _ in open(path, "r", encoding="utf-8", errors="ignore"))
                                self.stats['entries_raw'] += count
                                self.console.print(f"[green]Fichier {path.name} : {count} entrées brutes[/green]")
                            except Exception as e:
                                self.console.print(f"[red]Erreur lors du comptage des entrées dans {path}: {e}[/red]")
            except Exception as e:
                self.console.print(f"[bold red]Erreur lors du téléchargement de {source.slug}: {e}[/bold red]")
                
        self.console.print(f"[bold green]✓ Téléchargement terminé. {self.stats['sources_downloaded']} sources téléchargées.[/bold green]")
    
    async def run_normalize(self):
        """
        Normalise les données brutes.
        """
        self.console.print("\n[bold blue]Normalisation des données...[/bold blue]")
        
        if not self.source_paths:
            self.console.print("[yellow]Aucune source téléchargée. Veuillez d'abord télécharger les sources.[/yellow]")
            return
            
        # Créer le répertoire normalized s'il n'existe pas
        self.normalized_dir.mkdir(parents=True, exist_ok=True)
        
        # Normaliser chaque source
        normalized_count = 0
        normalized_files = {}
        
        # Valider tous les chemins avant traitement
        all_paths = []
        for source_slug, paths in self.source_paths.items():
            # Convertir en liste si ce n'est pas déjà le cas
            if isinstance(paths, (list, tuple)):
                all_paths.extend(paths)
            else:
                all_paths.append(paths)
        
        # Vérifier que tous les fichiers existent et sont accessibles
        valid_paths, invalid_paths = await asyncio.to_thread(
            validate_downloaded_files,
            all_paths,
            console=self.console
        )
        
        if invalid_paths:
            self.console.print(f"[yellow]⚠ {len(invalid_paths)} fichiers invalides ou inaccessibles seront ignorés.[/yellow]")
        
        if not valid_paths:
            self.console.print("[red]Aucun fichier valide à normaliser. Veuillez vérifier les sources téléchargées.[/red]")
            return
        
        self.console.print(f"[green]✓ {len(valid_paths)} fichiers valides à normaliser.[/green]")
        
        # Créer un dictionnaire pour suivre les fichiers valides par source
        valid_files_by_source = {}
        for path in valid_paths:
            # Trouver à quelle source appartient ce fichier
            for source_slug, source_paths in self.source_paths.items():
                if isinstance(source_paths, (list, tuple)):
                    if path in source_paths:
                        if source_slug not in valid_files_by_source:
                            valid_files_by_source[source_slug] = []
                        valid_files_by_source[source_slug].append(path)
                elif path == source_paths:
                    if source_slug not in valid_files_by_source:
                        valid_files_by_source[source_slug] = []
                    valid_files_by_source[source_slug].append(path)
        
        # Traiter chaque source avec les fichiers valides
        for source_slug, paths in valid_files_by_source.items():
            self.console.print(f"[cyan]Normalisation de {source_slug} ({len(paths)} fichiers)...[/cyan]")
            
            for path in paths:
                # Créer un nom de fichier normalisé en garantissant qu'il est unique
                base_name = path.stem
                extension = path.suffix
                normalized_file_name = f"normalized_{source_slug}_{base_name}{extension}"
                normalized_path = self.normalized_dir / normalized_file_name
                
                # Si le fichier existe déjà, ajouter un suffixe numérique
                counter = 1
                while normalized_path.exists():
                    normalized_file_name = f"normalized_{source_slug}_{base_name}_{counter}{extension}"
                    normalized_path = self.normalized_dir / normalized_file_name
                    counter += 1
                
                # Vérifier si c'est un fichier binaire en utilisant notre fonction avancée
                is_binary = await asyncio.to_thread(is_binary_file, path)
                
                # TODO: Implémenter une meilleure normalisation des données
                # Pour l'instant, on copie simplement les fichiers
                try:
                    # Vérifier si c'est un fichier ou un dossier
                    if path.is_file():
                        # Pour les fichiers, utiliser copy2
                        self.console.print(f"[cyan]Copie du fichier {path.name} vers {normalized_path}[/cyan]")
                        await asyncio.to_thread(shutil.copy2, path, normalized_path)
                    elif path.is_dir():
                        # Pour les dossiers, utiliser copytree et créer un dossier avec le même nom normalisé
                        self.console.print(f"[cyan]Copie récursive du dossier {path.name} vers {normalized_path}[/cyan]")
                        # Si le dossier de destination existe déjà, il faut le supprimer d'abord
                        if normalized_path.exists():
                            await asyncio.to_thread(shutil.rmtree, normalized_path)
                        await asyncio.to_thread(shutil.copytree, path, normalized_path)
                    else:
                        # Si ce n'est ni un fichier ni un dossier, c'est probablement un lien symbolique ou autre
                        self.console.print(f"[yellow]Le chemin {path} n'est ni un fichier ni un dossier, ignoré.[/yellow]")
                        continue
                except Exception as e:
                    self.console.print(f"[red]Erreur lors de la copie de {path} vers {normalized_path}: {e}[/red]")
                    continue
                
                # Enregistrer le chemin normalisé pour cette source
                if source_slug not in normalized_files:
                    normalized_files[source_slug] = []
                normalized_files[source_slug].append(normalized_path)
                
                # Compter les entrées normalisées
                try:
                    if normalized_path.is_file():
                        if not is_binary:
                            # Pour les fichiers texte, compter les lignes
                            with open(normalized_path, "r", encoding="utf-8", errors="ignore") as f:
                                lines = f.readlines()
                                count = len(lines)
                                normalized_count += count
                                self.console.print(f"[green]Fichier {normalized_path.name} : {count} entrées normalisées[/green]")
                        else:
                            # Pour les fichiers binaires, ignorer le comptage des lignes mais noter la taille
                            file_size = os.path.getsize(normalized_path)
                            size_kb = file_size / 1024
                            self.console.print(f"[yellow]Fichier binaire {normalized_path.name} copié ({size_kb:.2f} KB)[/yellow]")
                    elif normalized_path.is_dir():
                        # Pour les dossiers, compter récursivement le nombre de fichiers et la taille totale
                        total_files = 0
                        total_size = 0
                        text_lines = 0
                        
                        for root, dirs, files in os.walk(normalized_path):
                            for file in files:
                                file_path = Path(root) / file
                                total_files += 1
                                file_size = os.path.getsize(file_path)
                                total_size += file_size
                                
                                # Si c'est un fichier texte, compter les lignes pour les statistiques
                                if not await asyncio.to_thread(is_binary_file, file_path):
                                    try:
                                        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                                            lines = f.readlines()
                                            file_lines = len(lines)
                                            text_lines += file_lines
                                            normalized_count += file_lines
                                    except Exception as e:
                                        self.console.print(f"[yellow]Impossible de compter les lignes dans {file_path}: {e}[/yellow]")
                        
                        size_mb = total_size / (1024 * 1024)
                        self.console.print(f"[cyan]Dossier {normalized_path.name} : {total_files} fichiers, {text_lines} lignes texte, {size_mb:.2f} MB[/cyan]")
                except Exception as e:
                    self.console.print(f"[red]Erreur lors du traitement de {normalized_path}: {e}[/red]")
        
        # Mettre à jour notre suivi des chemins normalisés
        self.normalized_paths = normalized_files
        
        self.stats['entries_normalized'] = normalized_count
        self.console.print(f"[bold green]✓ Normalisation terminée. {normalized_count} entrées normalisées.[/bold green]")
    
    async def run_deduplicate(self):
        """
        Déduplique les chunks bruts.
        """
        self.console.print("\n[bold blue]Déduplication des chunks...[/bold blue]")
        
        split_dir = self.output_dir / "splits"
        if not split_dir.exists() or not any(split_dir.glob("*.txt")):
            self.console.print("[yellow]Aucun chunk à dédupliquer. Veuillez d'abord créer des chunks bruts.[/yellow]")
            return
            
        # Créer le répertoire deduped s'il n'existe pas
        self.deduped_dir.mkdir(parents=True, exist_ok=True)
        
        # Dédupliquer les chunks
        try:
            output_path = self.deduped_dir / "deduped_chunks.txt"
            final_path = await asyncio.to_thread(
                deduplicate_chunks,
                split_dir,
                output_path,
                self.console
            )
            
            self.deduped_path = final_path
            
            # Compter les entrées dédupliquées
            count = sum(1 for _ in open(final_path, "r", encoding="utf-8"))
            self.stats['entries_deduped'] = count
            
            self.console.print(f"[bold green]✓ Déduplication terminée. {count} entrées uniques.[/bold green]")
        except Exception as e:
            self.console.print(f"[bold red]Erreur lors de la déduplication: {e}[/bold red]")
    
    async def run_export(self):
        """
        Exporte les données dédupliquées.
        """
        self.console.print("\n[bold blue]Exportation des données...[/bold blue]")
        
        if not self.deduped_path or not self.deduped_path.exists():
            self.console.print("[yellow]Aucun fichier dédupliqué à exporter. Veuillez d'abord dédupliquer les chunks.[/yellow]")
            return
            
        try:
            final_path = await asyncio.to_thread(
                export_data,
                self.config_path,
                self.deduped_path
            )
            
            self.console.print(f"[bold green]✓ Exportation terminée. Fichier créé: {final_path}[/bold green]")
        except Exception as e:
            self.console.print(f"[bold red]Erreur lors de l'exportation: {e}[/bold red]")
