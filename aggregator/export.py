"""
Module d'export pour aggregator Nickname.
Gère l'export des données dédupliquées vers le format final.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

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
    
    def export_with_original(self, deduped_path: Path, normalized_paths: Dict[str, Path], output_filename: str = "aggregator_nicks_with_original.parquet") -> Path:
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


    def export_emails_and_nicknames(self, emails_dir: Path, deduped_path: Path) -> Tuple[Path, Path, Path]:
        """
        Exporte séparément les adresses email et les pseudonymes.
        
        Args:
            emails_dir: Répertoire contenant les fichiers d'emails normalisés
            deduped_path: Chemin vers le fichier de pseudonymes dédupliqués
            
        Returns:
            Tuple[Path, Path, Path]: (chemin_fichier_emails, chemin_fichier_pseudos, chemin_fichier_combiné)
        """
        self.console.print(f"[bold blue]Export des emails et pseudos en fichiers séparés...[/bold blue]")
        
        # 1. Exporter les emails
        emails_output_path = self.output_dir / "emails.parquet"
        emails_txt_path = self.output_dir / "emails.txt"
        nicknames_from_emails_path = self.output_dir / "nicknames_from_emails.txt"
        
        # Collecter tous les emails
        all_emails: List[str] = []
        all_local_parts: List[str] = []
        all_domains: List[str] = []
        
        if emails_dir.exists() and emails_dir.is_dir():
            # Parcourir les fichiers d'emails
            email_files = list(emails_dir.glob("*.parquet"))
            
            with Progress() as progress:
                task = progress.add_task("[green]Traitement des sources d'emails...", total=len(email_files))
                
                for email_file in email_files:
                    try:
                        df = pl.read_parquet(email_file)
                        if "email" in df.columns and "local_part" in df.columns:
                            emails_list = df["email"].to_list()
                            local_parts_list = df["local_part"].to_list()
                            all_emails.extend([str(email) for email in emails_list])
                            all_local_parts.extend([str(local) for local in local_parts_list])
                            if "domain" in df.columns:
                                domain_list = df["domain"].to_list()
                                all_domains.extend([str(domain) for domain in domain_list])
                    except Exception as e:
                        self.console.print(f"[bold red]Erreur lors du traitement de {email_file.name}: {e}[/bold red]")
                    progress.update(task, advance=1)
        
        # Dédupliquer et créer les DataFrames
        unique_emails: List[str] = list(dict.fromkeys(all_emails))
        unique_local_parts: List[str] = list(dict.fromkeys(all_local_parts))
        unique_domains: List[str] = list(dict.fromkeys(all_domains))
        
        # Exporter les emails en parquet
        # On s'assure que tous les tableaux ont la même longueur
        local_parts_adjusted: List[str] = []
        domains_adjusted: List[str] = []
        
        for i in range(len(unique_emails)):
            if i < len(unique_local_parts):
                local_parts_adjusted.append(unique_local_parts[i])
            else:
                local_parts_adjusted.append("")
                
            if i < len(unique_domains):
                domains_adjusted.append(unique_domains[i])
            else:
                domains_adjusted.append("")
        
        email_df = pl.DataFrame({
            "email": unique_emails,
            "local_part": local_parts_adjusted,
            "domain": domains_adjusted
        })
        email_df.write_parquet(emails_output_path)
        
        # Exporter les emails en texte
        with open(emails_txt_path, "w", encoding="utf-8") as f:
            for email in unique_emails:
                f.write(f"{email}\n")
        
        # Exporter les parties locales des emails
        with open(nicknames_from_emails_path, "w", encoding="utf-8") as f:
            for local_part in unique_local_parts:
                f.write(f"{local_part}\n")
        
        # 2. Exporter les pseudos
        nicknames_output_path = self.output_dir / "nicknames.parquet"
        nicknames_txt_path = self.output_dir / "nicknames.txt"
        
        try:
            # Lecture du fichier dédupliqué de pseudos
            df = pl.read_parquet(deduped_path)
            nicks = df["nick"].to_list()
            
            # Exporter les pseudos en texte
            with open(nicknames_txt_path, "w", encoding="utf-8") as f:
                for nick in nicks:
                    f.write(f"{nick}\n")
                    
            # Sauvegarder en parquet aussi
            df.write_parquet(nicknames_output_path)
        except Exception as e:
            self.console.print(f"[bold red]Erreur lors de la lecture du fichier de pseudos: {e}[/bold red]")
            raise
            
        # 3. Créer un fichier combiné (pseudos + parties locales des emails)
        combined_output_path = self.output_dir / "combined_nicknames.txt"
        combined_parquet_path = self.output_dir / "combined_nicknames.parquet"
        
        # Fusionner les pseudos et les parties locales des emails
        all_nicknames = set(nicks + unique_local_parts)
        unique_all_nicknames = list(all_nicknames)
        
        # Exporter le fichier combiné en texte
        with open(combined_output_path, "w", encoding="utf-8") as f:
            for nick in unique_all_nicknames:
                f.write(f"{nick}\n")
                
        # Exporter en parquet
        combined_df = pl.DataFrame({"nick": unique_all_nicknames})
        combined_df.write_parquet(combined_parquet_path)
        
        # Afficher les statistiques
        self.console.print(f"[green]✓[/green] Export terminé avec succès")
        self.console.print(f"[blue]Emails exportés: {len(unique_emails)} adresses uniques[/blue]")
        self.console.print(f"[blue]Parties locales exportées: {len(unique_local_parts)} uniques[/blue]")
        self.console.print(f"[blue]Pseudos exportés: {len(nicks)} uniques[/blue]")
        self.console.print(f"[blue]Combinaison (pseudos + parties locales): {len(unique_all_nicknames)} uniques[/blue]")
        
        # Lister les fichiers produits
        self.console.print("\n[bold]Fichiers générés:[/bold]")
        self.console.print(f"- Emails (parquet): {emails_output_path}")
        self.console.print(f"- Emails (txt): {emails_txt_path}")
        self.console.print(f"- Parties locales des emails (txt): {nicknames_from_emails_path}")
        self.console.print(f"- Pseudos (parquet): {nicknames_output_path}")
        self.console.print(f"- Pseudos (txt): {nicknames_txt_path}")
        self.console.print(f"- Combinaison pseudos + parties locales (txt): {combined_output_path}")
        self.console.print(f"- Combinaison pseudos + parties locales (parquet): {combined_parquet_path}")
        
        return emails_output_path, nicknames_output_path, combined_output_path


def export_data(config_path: str, deduped_path: Path, normalized_paths: Optional[Dict[str, Path]] = None, keep_original: bool = False, export_emails: bool = False) -> Path:
    """
    Fonction principale pour exporter les données.
    
    Args:
        config_path: Chemin vers le fichier de configuration
        deduped_path: Chemin vers le fichier dédupliqué
        normalized_paths: Dictionnaire des chemins de fichiers normalisés par slug (requis si keep_original=True)
        keep_original: Conserver les formes originales
        export_emails: Exporter séparément les emails et les pseudos
        
    Returns:
        Path: Chemin vers le fichier exporté (ou le fichier combiné si export_emails=True)
    """
    from .config import load_config
    
    config = load_config(config_path)
    exporter = Exporter(config)
    
    if export_emails:
        # Exporter les emails et les pseudos séparément
        emails_dir = Path(config.defaults.cache_dir).parent / "normalized" / "emails"
        _, _, combined_path = exporter.export_emails_and_nicknames(emails_dir, deduped_path)
        return combined_path
    elif keep_original:
        if normalized_paths is None:
            raise ValueError("normalized_paths est requis lorsque keep_original=True")
        return exporter.export_with_original(deduped_path, normalized_paths)
    else:
        return exporter.export_streaming(deduped_path)
