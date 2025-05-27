"""
Module de déduplication pour aggregator Nickname.
Gère la déduplication des chunks de données pour éviter les redondances.
"""

import os
from pathlib import Path
from typing import List, Set, Optional, Union
from rich.console import Console
from rich.progress import Progress

def deduplicate_chunks(
    input_dir: Union[str, Path], 
    output_path: Union[str, Path], 
    console: Optional[Console] = None
) -> Path:
    """
    Déduplique les chunks de texte provenant de fichiers dans un répertoire.
    
    Args:
        input_dir: Répertoire contenant les fichiers de chunks à dédupliquer
        output_path: Chemin du fichier de sortie pour les chunks dédupliqués
        console: Console rich pour l'affichage (optionnel)
    
    Returns:
        Path: Chemin du fichier contenant les chunks dédupliqués
    """
    # Convertir les chemins en objets Path
    input_dir = Path(input_dir)
    output_path = Path(output_path)
    
    # S'assurer que le répertoire de sortie existe
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Initialiser l'ensemble pour stocker les lignes uniques
    unique_lines: Set[str] = set()
    
    # Afficher un message de début
    if console:
        console.print(f"[cyan]Déduplication des chunks de {input_dir}...[/cyan]")
    
    # Obtenir la liste des fichiers texte du répertoire d'entrée
    text_files = list(input_dir.glob("*.txt"))
    total_files = len(text_files)
    
    if total_files == 0:
        if console:
            console.print(f"[yellow]Aucun fichier texte trouvé dans {input_dir}[/yellow]")
        return output_path
    
    # Utiliser un Progress pour suivre l'avancement
    with Progress() as progress:
        task = progress.add_task("[green]Déduplication...", total=total_files)
        
        # Traiter chaque fichier
        for i, file_path in enumerate(text_files):
            if console:
                console.print(f"[cyan]Traitement du fichier {i+1}/{total_files}: {file_path.name}[/cyan]")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    # Ajouter chaque ligne du fichier à l'ensemble
                    for line in f:
                        line = line.strip()
                        if line:  # Ignorer les lignes vides
                            unique_lines.add(line)
            except Exception as e:
                if console:
                    console.print(f"[red]Erreur lors de la lecture de {file_path.name}: {e}[/red]")
            
            progress.update(task, advance=1)
    
    # Nombre de lignes uniques
    unique_count = len(unique_lines)
    
    # Écrire les lignes uniques dans le fichier de sortie
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            for line in unique_lines:
                f.write(f"{line}\n")
        
        if console:
            console.print(f"[green]Déduplication terminée avec succès![/green]")
            console.print(f"[green]Nombre de lignes uniques: {unique_count}[/green]")
            console.print(f"[green]Fichier de sortie: {output_path}[/green]")
    except Exception as e:
        if console:
            console.print(f"[red]Erreur lors de l'écriture du fichier de sortie: {e}[/red]")
    
    return output_path
