"""
Module de division des fichiers pour l'orchestrateur d'aggregator Nickname.
Contient toutes les fonctions liées à la division des fichiers bruts et dédupliqués.
"""

import asyncio
from pathlib import Path
from typing import Optional, Dict, List
from rich.console import Console

from ..split_raw import split_raw_files
# Correction : le module de déduplication s'appelle 'dedupe.py' (et non 'deduplication.py').
# La fonction deduplicate_chunks est définie dans aggregator/dedupe.py.
from ..dedupe import deduplicate_chunks


class SplittingMixin:
    """Mixin pour les fonctionnalités de division des fichiers de l'orchestrateur."""
    
    # Définir les attributs hérités de OrchestratorBase avec leurs types
    normalized_dir: Path
    output_dir: Path
    deduped_dir: Path
    deduped_path: Optional[Path] = None
    console: Console
    stats: Dict[str, int]

    async def run_split_normalized(self):
        """
        Scinde les fichiers normalisés (.txt, .csv, .tsv) en fichiers txt de 5M lignes max.
        Cette étape crée des chunks à partir des données NORMALISÉES, pas des données brutes.
        """
        # Vérifier que des données normalisées existent
        if not self.normalized_dir.exists() or not any(self.normalized_dir.glob("*.*")):
            self.console.print("[yellow]Aucune donnée normalisée trouvée. Veuillez d'abord normaliser les données (option 2).[/yellow]")
            return
            
        self.console.print("\n[bold blue]Scission des fichiers normalisés en chunks de 5M lignes...[/bold blue]")
        split_raw_files(
            input_dir=self.normalized_dir,  # Utiliser les données normalisées au lieu des données brutes
            output_dir=self.output_dir / "splits",
            max_lines=5_000_000,
            console=self.console
        )
    
    async def run_split_deduped(self):
        """
        Scinde le fichier des données dédupliquées en plusieurs fichiers par taille.
        Demande à l'utilisateur le nombre de lignes souhaité par fichier, avec 1 000 000 comme valeur par défaut.
        """
        # Si pas de fichier dédupliqué, tenter une déduplication automatique si des chunks bruts existent
        if not self.deduped_path or not self.deduped_path.exists():
            split_dir = self.output_dir / "splits"
            if split_dir.exists() and any(split_dir.glob("*.txt")):
                self.console.print("[yellow]Aucun fichier dédupliqué trouvé, lancement de la déduplication automatique...[/yellow]")
                final_path = await asyncio.to_thread(
                    deduplicate_chunks,
                    split_dir,
                    self.deduped_dir / "deduped_chunks.txt",
                    self.console
                )
                self.deduped_path = final_path
                count = sum(1 for _ in open(final_path, "r", encoding="utf-8"))
                self.stats['entries_deduped'] = count
                self.console.print(f"[green]Entrées uniques après chunks : {count}[/green]")
            else:
                self.console.print("[yellow]Aucun fichier dédupliqué trouvé. Exécutez d'abord l'option 5 pour dédupliquer ou l'option 4 pour créer des chunks bruts.[/yellow]")
                return
        
        self.console.print("\n[bold blue]Division du fichier dédupliqué en fichiers plus petits...[/bold blue]")
        
        # Demander à l'utilisateur le nombre de lignes par fichier
        default_chunk_size = 1_000_000
        self.console.print(f"\n[bold]Par défaut, chaque fichier contiendra {default_chunk_size:,} lignes.[/bold]")
        # Utiliser le système de traductions pour l'invite utilisateur
        from .translations import get_translation
        lang = getattr(self, 'lang', 'fr')  # Récupérer la langue ou utiliser fr par défaut
        user_input = input(get_translation("modify_lines_per_file", lang))
        
        # Traiter l'entrée de l'utilisateur
        chunk_size = default_chunk_size
        if user_input.strip():
            try:
                user_chunk_size = int(user_input.strip().replace(',', '').replace('_', ''))
                if user_chunk_size > 0:
                    chunk_size = user_chunk_size
                    self.console.print(f"[green]Utilisation de {chunk_size:,} lignes par fichier.[/green]")
                else:
                    self.console.print(f"[yellow]Valeur incorrecte. Utilisation de la valeur par défaut : {default_chunk_size:,} lignes par fichier.[/yellow]")
            except ValueError:
                self.console.print(f"[yellow]Entrée non valide. Utilisation de la valeur par défaut : {default_chunk_size:,} lignes par fichier.[/yellow]")
        else:
            self.console.print(f"[green]Utilisation de la valeur par défaut : {chunk_size:,} lignes par fichier.[/green]")
        
        # Créer le répertoire de sortie s'il n'existe pas
        final_dir = self.output_dir / "final"
        final_dir.mkdir(parents=True, exist_ok=True)
        
        # Lire le fichier dédupliqué et le diviser en morceaux selon la taille choisie
        with open(self.deduped_path, "r", encoding="utf-8") as f:
            lines: List[str] = []
            last_idx = 0
            total_lines = sum(1 for _ in open(self.deduped_path, "r", encoding="utf-8"))
            
            for i, line in enumerate(f):
                # Ajouter une virgule après chaque terme sauf pour le dernier du fichier
                term = line.strip()
                if i < total_lines - 1:  # Si ce n'est pas la dernière ligne du fichier entier
                    term = term + ','
                lines.append(term)
                
                if (i + 1) % chunk_size == 0:
                    # Écrire le chunk actuel
                    last_idx = (i + 1) // chunk_size
                    last_file = final_dir / f"chunk_{last_idx:03d}.txt"
                    
                    # Si c'est le dernier chunk et que c'est aussi le dernier fichier, on retire la virgule du dernier terme
                    if (i + 1) == total_lines and lines:
                        lines[-1] = lines[-1].rstrip(',')
                    
                    # Utiliser open() au lieu de write_text() pour avoir plus de contrôle sur les erreurs d'encodage
                    with open(last_file, "w", encoding="utf-8", errors="replace") as f:
                        f.write("\n".join(lines) + "\n")
                    self.console.print(f"[green]Fichier {last_file.name} créé avec {len(lines):,} lignes.[/green]")
                    lines = []
            
            # Écrire le dernier chunk s'il reste des lignes
            if lines:
                last_idx += 1
                last_file = final_dir / f"chunk_{last_idx:03d}.txt"
                
                # S'assurer que le dernier terme du dernier fichier n'a pas de virgule
                if lines:  # cette vérification est redondante mais préserve la logique d'origine
                    lines[-1] = lines[-1].rstrip(',')
                
                # Utiliser open() au lieu de write_text() pour avoir plus de contrôle sur les erreurs d'encodage
                with open(str(last_file), "w", encoding="utf-8", errors="replace") as f:
                    f.write("\n".join(lines) + "\n")
                self.console.print(f"[green]Fichier {last_file.name} créé avec {len(lines):,} lignes.[/green]")
        
        self.console.print(f"[green]✓ Split des données dédupliquées terminé: {last_idx} fichiers créés.[/green]")

