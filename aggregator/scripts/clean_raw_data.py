"""
Script de nettoyage du répertoire data/raw.
Ce script parcourt les fichiers dans data/raw, corrige les noms problématiques
et améliore la structure des répertoires.
"""

import os
import shutil
import re
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, TaskID

# Initialiser la console pour les logs colorés
console = Console()

def is_valid_filename(filename):
    """
    Vérifie si un nom de fichier est valide.
    
    Args:
        filename: Nom de fichier à valider
    
    Returns:
        bool: True si le nom est valide, False sinon
    """
    # Caractères invalides dans les noms de fichiers Windows
    invalid_chars = r'[<>:"/\\|?*]'
    
    # Vérifier la longueur du chemin complet (max 255 caractères pour Windows)
    if len(filename) > 255:
        return False
        
    # Vérifier les caractères invalides
    if re.search(invalid_chars, filename):
        return False
        
    # Éviter les espaces ou points en début/fin
    if filename.startswith(' ') or filename.endswith(' ') or filename.endswith('.'):
        return False
        
    return True

def fix_filename(filename):
    """
    Corrige un nom de fichier invalide.
    
    Args:
        filename: Nom de fichier à corriger
    
    Returns:
        str: Nom de fichier corrigé
    """
    # Remplacer les caractères invalides
    invalid_chars = r'[<>:"/\\|?*]'
    clean_name = re.sub(invalid_chars, '_', filename)
    
    # Tronquer si trop long
    if len(clean_name) > 255:
        base, ext = os.path.splitext(clean_name)
        clean_name = base[:250] + ext if ext else base[:255]
        
    # Supprimer les espaces/points en début/fin
    clean_name = clean_name.strip().rstrip('.')
    
    # S'assurer que le nom n'est pas vide
    if not clean_name:
        clean_name = "unnamed_file"
        
    return clean_name

def organize_by_extension(file_path, dest_dir):
    """
    Organise les fichiers par extension.
    
    Args:
        file_path: Chemin du fichier à organiser
        dest_dir: Répertoire de destination
    
    Returns:
        Path: Nouveau chemin du fichier
    """
    ext = file_path.suffix.lower().lstrip('.')
    
    # Regrouper par type de fichier
    if ext in ('txt', 'csv', 'tsv', 'list'):
        category = 'text'
    elif ext in ('zip', 'tar', 'gz', 'tgz', 'rar', '7z'):
        category = 'archives'
    elif ext in ('jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg'):
        category = 'images'
    elif ext in ('json', 'xml', 'yaml', 'yml'):
        category = 'structured'
    elif ext in ('md', 'rst', 'html', 'pdf', 'doc', 'docx'):
        category = 'documents'
    else:
        category = 'others'
        
    # Créer le répertoire de destination s'il n'existe pas
    category_dir = dest_dir / category
    category_dir.mkdir(exist_ok=True, parents=True)
    
    # Construire le nouveau chemin
    new_path = category_dir / file_path.name
    
    # S'il y a un conflit de nom, ajouter un suffixe
    counter = 1
    base_name = file_path.stem
    while new_path.exists():
        new_name = f"{base_name}_{counter}{file_path.suffix}"
        new_path = category_dir / new_name
        counter += 1
        
    return new_path

def process_directory(dir_path, dest_dir=None, recursive=True):
    """
    Traite un répertoire pour corriger les noms de fichiers et l'organisation.
    
    Args:
        dir_path: Chemin du répertoire à traiter
        dest_dir: Répertoire de destination (si None, utilise le même répertoire)
        recursive: Si True, traite également les sous-répertoires
        
    Returns:
        tuple: (nombre de fichiers traités, nombre de fichiers renommés)
    """
    # Si pas de répertoire de destination spécifié, utiliser le même
    if dest_dir is None:
        dest_dir = dir_path
        
    # S'assurer que les répertoires existent
    dir_path = Path(dir_path)
    dest_dir = Path(dest_dir)
    
    if not dir_path.exists() or not dir_path.is_dir():
        console.print(f"[red]Le répertoire {dir_path} n'existe pas ou n'est pas un répertoire.[/red]")
        return 0, 0
        
    dest_dir.mkdir(exist_ok=True, parents=True)
    
    # Récupérer tous les fichiers et dossiers
    items = list(dir_path.glob('*'))
    processed = 0
    renamed = 0
    total_items = len(items)
    
    # Afficher l'information au lieu d'utiliser Progress
    console.print(f"[cyan]Traitement de {total_items} éléments dans {dir_path}[/cyan]")
    
    for i, item in enumerate(items):
        # Afficher un message de progression simple
        if i % 10 == 0 or i == total_items - 1:
            console.print(f"[cyan]Progression : {i+1}/{total_items} ({int((i+1)/total_items*100)}%) - Traitement de {item.name}[/cyan]")
        
        if item.is_dir() and recursive:
            # Traiter récursivement les sous-répertoires
            sub_processed, sub_renamed = process_directory(item, dest_dir / item.name, recursive)
            processed += sub_processed
            renamed += sub_renamed
        elif item.is_file():
            # Traiter le fichier
            processed += 1
            
            # Vérifier et corriger le nom si nécessaire
            if not is_valid_filename(item.name):
                new_name = fix_filename(item.name)
                renamed += 1
                console.print(f"[yellow]Renommage:[/yellow] {item.name} -> {new_name}")
            else:
                new_name = item.name
            
            # Déterminer la destination
            dest_path = organize_by_extension(item.with_name(new_name), dest_dir)
            
            try:
                # Copier/déplacer le fichier
                if dir_path == dest_dir:
                    # Si même répertoire, renommer
                    if item.name != dest_path.name:
                        shutil.move(item, dest_path)
                else:
                    # Sinon, copier
                    shutil.copy2(item, dest_path)
            except Exception as e:
                console.print(f"[red]Erreur lors du traitement de {item}: {e}[/red]")
    
    return processed, renamed

def clean_raw_directory(data_dir, organize=True):
    """
    Nettoie le répertoire data/raw.
    
    Args:
        data_dir: Répertoire data contenant le sous-dossier raw
        organize: Si True, organise également les fichiers par catégorie
        
    Returns:
        tuple: (nombre de fichiers traités, nombre de fichiers renommés)
    """
    data_dir = Path(data_dir)
    raw_dir = data_dir / 'raw'
    
    if not raw_dir.exists():
        console.print(f"[red]Le répertoire {raw_dir} n'existe pas.[/red]")
        return 0, 0
    
    # Approche plus simple et robuste : pas de sauvegarde complète
    # Nous allons plutôt travailler directement sur les fichiers
    console.print("[blue]Traitement des fichiers dans {raw_dir}...[/blue]")
    
    # Si besoin d'organiser, créer un nouveau répertoire temporaire
    if organize:
        temp_dir = data_dir / 'raw_temp'
        # Supprimer le répertoire temporaire s'il existe
        if temp_dir.exists():
            try:
                console.print("[yellow]Suppression du répertoire temporaire existant...[/yellow]")
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                console.print(f"[red]Erreur lors de la suppression du répertoire temporaire: {e}[/red]")
                # Continuer malgré l'erreur, nous réessaierons de le créer
        
        # Créer le répertoire temporaire
        try:
            temp_dir.mkdir(parents=True, exist_ok=True)
            console.print(f"[green]Répertoire temporaire créé: {temp_dir}[/green]")
        except Exception as e:
            console.print(f"[red]Impossible de créer le répertoire temporaire: {e}[/red]")
            return 0, 0
        
        # Traiter et organiser les fichiers
        try:
            processed, renamed = process_directory(raw_dir, temp_dir)
            console.print(f"[green]Traitement terminé: {processed} fichiers traités, {renamed} fichiers renommés.[/green]")
        except Exception as e:
            console.print(f"[red]Erreur lors du traitement des fichiers: {e}[/red]")
            return 0, 0
        
        # Si le traitement a réussi, remplacer le répertoire raw par le répertoire temporaire
        try:
            # Créer un backup des données brutes dans un sous-dossier
            backup_dir = raw_dir / '_original_data_backup'
            backup_dir.mkdir(exist_ok=True, parents=True)
            
            # Déplacer les fichiers originaux dans le dossier de sauvegarde un par un
            console.print("[blue]Sauvegarde des fichiers originaux...[/blue]")
            file_count = 0
            for item in raw_dir.glob('*'):
                if item != backup_dir:  # Ne pas déplacer le dossier backup dans lui-même
                    try:
                        if item.is_file():
                            # Copier chaque fichier individuellement
                            shutil.copy2(item, backup_dir / item.name)
                            file_count += 1
                    except Exception as e:
                        console.print(f"[yellow]Erreur lors de la sauvegarde de {item}: {e}[/yellow]")
            
            console.print(f"[green]{file_count} fichiers sauvegardés dans {backup_dir}[/green]")
            
            # Copier les nouveaux fichiers organisés dans le répertoire raw
            console.print("[blue]Copie des fichiers organisés...[/blue]")
            for category_dir in temp_dir.glob('*'):
                if category_dir.is_dir():
                    # Créer le répertoire de catégorie s'il n'existe pas
                    target_dir = raw_dir / category_dir.name
                    target_dir.mkdir(exist_ok=True, parents=True)
                    
                    # Copier tous les fichiers de cette catégorie
                    for file in category_dir.glob('*'):
                        if file.is_file():
                            shutil.copy2(file, target_dir / file.name)
            
            # Supprimer le répertoire temporaire
            shutil.rmtree(temp_dir, ignore_errors=True)
            console.print(f"[green]Réorganisation terminée. Les fichiers originaux ont été sauvegardés dans {backup_dir}[/green]")
        except Exception as e:
            console.print(f"[red]Erreur lors du remplacement du répertoire: {e}[/red]")
            console.print(f"[yellow]Les fichiers organisés restent disponibles dans {temp_dir}[/yellow]")
    else:
        # Traiter les fichiers sur place
        try:
            processed, renamed = process_directory(raw_dir)
            console.print(f"[green]Traitement terminé: {processed} fichiers traités, {renamed} fichiers renommés.[/green]")
        except Exception as e:
            console.print(f"[red]Erreur lors du traitement des fichiers: {e}[/red]")
            return 0, 0
    
    return processed, renamed

def main():
    """Fonction principale."""
    console.print("[bold blue]Nettoyage du répertoire data/raw[/bold blue]")
    
    # Détecter le répertoire data
    project_dir = Path(__file__).resolve().parent.parent.parent
    data_dir = project_dir / 'data'
    
    if not data_dir.exists():
        console.print(f"[red]Le répertoire {data_dir} n'existe pas.[/red]")
        return
        
    organize = True  # Par défaut, organiser les fichiers
    
    # Exécuter le nettoyage
    clean_raw_directory(data_dir, organize)
    
    console.print("[bold green]✓ Nettoyage terminé.[/bold green]")
    console.print("[blue]Pour restaurer la sauvegarde en cas de problème:[/blue]")
    console.print(f"[yellow]shutil.rmtree('{data_dir}/raw', ignore_errors=True)[/yellow]")
    console.print(f"[yellow]shutil.move('{data_dir}/raw_backup', '{data_dir}/raw')[/yellow]")

if __name__ == "__main__":
    main()
