"""
Module de normalisation pour aggregator Nickname.
Gère la normalisation des données provenant de différentes sources.
"""

from pathlib import Path
from typing import Dict, List, Optional, Callable, Any

import polars as pl
from rich.console import Console
from unidecode import unidecode
import re
import shutil

from .config import Config


class Normalizer:
    """Gestionnaire de normalisation des données."""

    def __init__(self, config: Config):
        """
        Initialise le normaliseur avec la configuration.
        
        Args:
            config: Configuration validée
        """
        self.config = config
        self.console = Console()
        
        # Créer le répertoire de cache pour les données normalisées
        self.cache_dir = Path(config.defaults.cache_dir).parent / "normalized"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def normalize_all(self, source_paths: Dict[str, Path], callback: Optional[Callable[[str], Any]] = None) -> Dict[str, Path]:
        """
        Normalise toutes les sources téléchargées.
        
        Args:
            source_paths: Dictionnaire des chemins de fichiers téléchargés par slug
            callback: Fonction de rappel pour chaque source normalisée
            
        Returns:
            Dict[str, Path]: Dictionnaire des chemins de fichiers normalisés par slug
        """
        # Log du démarrage de la normalisation
        self.console.print(f"[bold blue]Début de la normalisation de {len(source_paths)} sources...[/bold blue]")
        normalized_paths = {}
        for slug, path in source_paths.items():
            # Début du traitement d'une source
            self.console.print(f"[blue]Traitement de {slug}...[/blue]")
            try:
                normalized_path = self.normalize_source(slug, path)
                normalized_paths[slug] = normalized_path
                # Succès pour ce slug
                self.console.print(f"[green]✓ {slug} normalisée avec succès[/green]")
                if callback:
                    callback(slug)
            except Exception as e:
                self.console.print(f"[bold red]Erreur lors de la normalisation de {slug}: {e}[/bold red]")
        # Vérification des sources non traitées
        missing = set(source_paths.keys()) - set(normalized_paths.keys())
        if missing:
            self.console.print(f"[yellow]Attention: ces sources n'ont pas été normalisées: {sorted(missing)}[/yellow]")
        return normalized_paths
    
    def normalize_source(self, slug: str, source_path: Path) -> Path:
        """
        Normalise une source spécifique en fonction de son type.
        
        Args:
            slug: Identifiant de la source
            source_path: Chemin vers les données téléchargées
            
        Returns:
            Path: Chemin vers les données normalisées
        """
        # Cas particulier pour awesome_wordlists: parse README.md pour extraire les listes
        if slug == "awesome_wordlists":
            readme = source_path / "README.md"
            if not readme.exists():
                raise ValueError(f"README introuvable pour {slug} dans {source_path}")
            pattern = re.compile(r"\[(.*?)\]\((https?://[^\)]+)\)")
            names = []
            for line in readme.read_text(encoding="utf-8").splitlines():
                m = pattern.search(line)
                if m:
                    name = unidecode(m.group(1).strip().lower())
                    if name not in names:
                        names.append(name)
            df = pl.DataFrame({"nick": names})
            # Filtrer les pseudos valides via regex avec contains (supporté par polars)
            df = df.filter(pl.col("nick").str.contains(r'^[a-z0-9_.-]{1,32}$'))
            output_path = self.cache_dir / f"{slug}.parquet"
            df.write_parquet(output_path)
            return output_path

        # Cas particulier pour japanese_names: lire les fichiers male et female
        if slug == "japanese_names":
            # Chemins vers les fichiers male et female
            male_file = source_path / "male.txt"
            female_file = source_path / "female.txt"
            
            if not male_file.exists() or not female_file.exists():
                raise ValueError(f"Fichiers male.txt ou female.txt introuvables pour {slug} dans {source_path}")
            
            # Lecture des fichiers
            names = []
            for file_path in [male_file, female_file]:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        name = unidecode(line.strip().lower())
                        # Filtre par regex pour des noms valides
                        if re.match(r'^[a-z0-9_.-]{1,32}$', name) and name not in names:
                            names.append(name)
            
            # Création du DataFrame et écriture en Parquet
            df = pl.DataFrame({"nick": names})
            output_path = self.cache_dir / f"{slug}.parquet"
            df.write_parquet(output_path)
            return output_path
            male_path = source_path / "male"
            female_path = source_path / "female"
            if not male_path.exists() or not female_path.exists():
                raise ValueError(f"Fichiers male/female introuvables pour {slug} dans {source_path}")
            names = []
            for p in (male_path, female_path):
                with open(p, "r", encoding="utf-8", errors="ignore") as f:
                    for l in f:
                        l = l.strip()
                        if l:
                            names.append(unidecode(l.lower()))
            pattern = re.compile(r'^[a-z0-9_.-]{1,32}$')
            unique = list(dict.fromkeys([n for n in names if pattern.match(n)]))
            df = pl.DataFrame({"nick": unique})
            output_path = self.cache_dir / f"{slug}.parquet"
            df.write_parquet(output_path)
            return output_path

        # Cas particulier pour hypixel: fusion des listes epicube-players et hypixel-players
        if slug == "hypixel":
            pattern = re.compile(r'^[a-z0-9_.-]{1,32}$')
            player_files = []
            for name in ["epicube-players", "hypixel-players"]:
                p = source_path / name
                if not p.exists():
                    raise ValueError(f"Fichier {name} introuvable pour {slug} dans {source_path}")
                player_files.append(p)
            names = []
            for pf in player_files:
                with open(pf, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        l = line.strip()
                        if l:
                            names.append(unidecode(l.lower()))
            unique = list(dict.fromkeys([n for n in names if pattern.match(n)]))
            df = pl.DataFrame({"nick": unique})
            output_path = self.cache_dir / f"{slug}.parquet"
            df.write_parquet(output_path)
            return output_path

        # Cas particulier pour runescape_2014: lecture de tous les .txt après extraction
        if slug == "runescape_2014":
            # Extraire l'archive ZIP/TAR si nécessaire avant lecture
            if source_path.is_file() and source_path.name.endswith((".zip", ".tar.gz", ".tgz")):
                self.console.print(f"[blue]Extraction de l'archive {source_path.name} pour {slug}[/blue]")
                shutil.unpack_archive(str(source_path), str(source_path.parent))
                extracted_dir = source_path.parent / source_path.stem
                source_path = extracted_dir if extracted_dir.exists() else source_path.parent
            pattern = re.compile(r'^[a-z0-9_.-]{1,32}$')
            txt_files = list(source_path.glob("**/*.txt"))
            if not txt_files:
                raise ValueError(f"Aucun fichier .txt trouvé pour {slug} dans {source_path}")
            names = []
            for tf in txt_files:
                with open(tf, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        l = line.strip()
                        if l:
                            names.append(unidecode(l.lower()))
            unique = list(dict.fromkeys([n for n in names if pattern.match(n)]))
            df = pl.DataFrame({"nick": unique})
            output_path = self.cache_dir / f"{slug}.parquet"
            df.write_parquet(output_path)
            return output_path

        # Si la source téléchargée est une archive (zip, tar.gz, tgz), dézipper
        if source_path.is_file() and (source_path.name.endswith(".zip") or source_path.name.endswith(".tar.gz") or source_path.name.endswith(".tgz")):
            self.console.print(f"[blue]Extraction de l'archive {source_path.name}[/blue]")
            shutil.unpack_archive(str(source_path), str(source_path.parent))
            # Définir source_path sur le dossier extrait, sinon parent
            extracted_dir = source_path.parent / source_path.stem
            source_path = extracted_dir if extracted_dir.exists() else source_path.parent

        # Trouver la source correspondante dans la configuration
        source = next((s for s in self.config.sources if s.slug == slug), None)
        if not source:
            raise ValueError(f"Source non trouvée dans la configuration: {slug}")
        
        # Déterminer le chemin de sortie
        output_path = self.cache_dir / f"{slug}.parquet"
        
        # Si le fichier normalisé existe déjà et qu'on ne force pas la normalisation
        if output_path.exists() and not self.config.defaults.force:
            return output_path
        
        # Trouver les fichiers à traiter
        files_to_process = self._find_data_files(source_path)
        
        if not files_to_process:
            raise ValueError(f"Aucun fichier de données trouvé pour {slug} dans {source_path}")
        
        # Traiter chaque fichier et collecter les DataFrames
        dfs = []
        for file_path in files_to_process:
            df = self._read_file(file_path)
            if df is not None:
                # Normaliser le DataFrame
                df = self._normalize_dataframe(df)
                dfs.append(df)
        
        # Concaténer tous les DataFrames
        if dfs:
            final_df = pl.concat(dfs)
            
            # Sauvegarder le résultat
            final_df.write_parquet(output_path)
            
            return output_path
        else:
            raise ValueError(f"Aucune donnée valide trouvée pour {slug}")
    
    def _find_data_files(self, source_path: Path) -> List[Path]:
        """
        Trouve tous les fichiers de données dans le répertoire source.
        
        Args:
            source_path: Chemin vers le répertoire source
            
        Returns:
            List[Path]: Liste des chemins de fichiers de données
        """
        if source_path.is_file():
            return [source_path]
        
        # Extensions supportées
        supported_extensions = [".csv", ".txt", ".json", ".parquet", ".tsv"]
        
        # Rechercher récursivement tous les fichiers avec les extensions supportées
        files = []
        for ext in supported_extensions:
            files.extend(list(source_path.glob(f"**/*{ext}")))
        
        # Si aucun fichier supporté, prendre tous les fichiers du dossier
        if not files and source_path.is_dir():
            files = [p for p in source_path.iterdir() if p.is_file()]
        
        return files
    
    def _read_file(self, file_path: Path) -> Optional[pl.DataFrame]:
        """
        Lit un fichier et retourne un DataFrame Polars.
        
        Args:
            file_path: Chemin vers le fichier à lire
            
        Returns:
            Optional[pl.DataFrame]: DataFrame Polars ou None si le fichier ne peut pas être lu
        """
        try:
            # Déterminer le format en fonction de l'extension
            ext = file_path.suffix.lower()
            
            if ext == ".csv":
                # Lecture CSV tolérante aux erreurs de parsing avec fallback pandas
                try:
                    df = pl.read_csv(
                        file_path,
                        infer_schema_length=10000,
                        ignore_errors=True,
                    )
                    return df
                except Exception as e:
                    self.console.print(f"[yellow]Erreur lecture CSV Polars {file_path}: {e}[/yellow]")
                    # Fallback vers pandas pour les CSV malformés
                    try:
                        import pandas as pd  # type: ignore
                        df_pd = pd.read_csv(
                            file_path,
                            dtype=str,
                            engine="python",
                            on_bad_lines="skip",
                        )
                        self.console.print(f"[yellow]Fallback pandas réussi pour {file_path}[/yellow]")
                        return pl.from_pandas(df_pd)
                    except Exception as e2:
                        self.console.print(f"[bold red]Fallback pandas échoué pour {file_path}: {e2}[/bold red]")
                        return None
            elif ext == ".txt":
                # Lecture directe des fichiers texte (un pseudo par ligne)
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    lines = [line.strip() for line in f if line.strip()]
                return pl.DataFrame({"nick": lines})
            elif ext == ".json":
                return pl.read_json(file_path)
            elif ext == ".parquet":
                return pl.read_parquet(file_path)
            elif ext == ".tsv":
                return pl.read_csv(file_path, separator="\t", infer_schema_length=10000)
            elif ext in ("", ".md"):
                # Lecture des fichiers sans extension ou markdown en texte ligne par ligne
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    lines = [line.strip() for line in f if line.strip()]
                return pl.DataFrame({"nick": lines})
            else:
                self.console.print(f"[yellow]Format non supporté: {ext}[/yellow]")
                return None
        except Exception as e:
            self.console.print(f"[yellow]Erreur lors de la lecture de {file_path}: {e}[/yellow]")
            return None
    
    def _normalize_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Normalise un DataFrame selon les règles définies.
        
        Args:
            df: DataFrame à normaliser
            
        Returns:
            pl.DataFrame: DataFrame normalisé
        """
        # Renommer la colonne cible en 'nick'
        df = df.rename({self._identify_name_column(df): "nick"})


    def _normalize_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Normalise un DataFrame selon les règles définies.
        - Renomme la colonne cible en 'nick'
        - Applique la normalisation sur chaque pseudo (voir normalize_nick)
        - Filtre les pseudos vides et déduplique
        """
        # Renommer la colonne cible en 'nick'
        df = df.rename({self._identify_name_column(df): "nick"})

        # Sélectionner uniquement la colonne 'nick'
        df = df.select("nick")

        # Fonction interne de normalisation d'un pseudo
        def normalize_nick(value):
            """
            Normalise un pseudo :
            - Met en minuscules
            - Supprime les accents (unidecode)
            - Retire les caractères non autorisés (seuls a-z, 0-9, '_', '-', '.', ' ' sont conservés)
            - Supprime les espaces en trop
            """
            if value is None:
                return ""
            if not isinstance(value, str):
                try:
                    value = str(value)
                except:
                    return ""
            # Mise en minuscules et suppression des accents
            value = unidecode(value.strip().lower())
            # Filtrage des caractères autorisés uniquement
            value = re.sub(r"[^a-z0-9_.\- ]", "", value)
            # Suppression des espaces multiples
            value = re.sub(r"\s+", " ", value).strip()
            return value

        # Appliquer la normalisation et collecter les résultats
        values = df["nick"].to_list()
        normalized = [normalize_nick(v) for v in values]

        # Supprimer le filtre strict : ne conserver que les pseudos non vides
        filtered = [v for v in normalized if v]
        unique_nicks = list(dict.fromkeys(filtered))

        # Construire et retourner le DataFrame final
        return pl.DataFrame({"nick": unique_nicks})

    def _identify_name_column(self, df: pl.DataFrame) -> str:
        """
        Identifie la colonne contenant les noms/pseudos.
        - Cherche parmi une liste de noms courants pour la colonne pseudo
        - Si aucune colonne connue n'est trouvée, retourne la première colonne
        """
        possible_columns = [
            "nick", "nickname", "username", "user", "name", "pseudo", "handle",
            "forename", "firstname", "first_name", "surname", "lastname", "last_name",
            "fullname", "full_name", "display_name", "displayname"
        ]
        for col in possible_columns:
            if col in df.columns:
                return col
        return df.columns[0]

# Nettoyage : suppression des fonctions dupliquées hors classe



def normalize_sources(config_path: str, source_paths: Dict[str, Path], callback: Optional[Callable[[str], Any]] = None) -> Dict[str, Path]:
    """
    Fonction principale pour normaliser toutes les sources.
    
    Args:
        config_path: Chemin vers le fichier de configuration
        source_paths: Dictionnaire des chemins de fichiers téléchargés par slug
        callback: Fonction de rappel pour chaque source normalisée
        
    Returns:
        Dict[str, Path]: Dictionnaire des chemins de fichiers normalisés par slug
    """
    from .config import load_config
    
    config = load_config(config_path)
    normalizer = Normalizer(config)
    return normalizer.normalize_all(source_paths, callback)
