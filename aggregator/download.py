"""
Module de téléchargement asynchrone pour aggregator Nickname.
Gère le téléchargement des sources de données depuis différentes origines.
"""

import asyncio
import hashlib
import os
import shutil
import stat
import subprocess
import time
import traceback
import mimetypes
from pathlib import Path
from typing import Dict, Any, Callable, List, Tuple, Set, Optional

import aiohttp
import git
from rich.progress import Progress, TaskID
from tqdm import tqdm
from rich.console import Console

from .config import Config, Source
from .utils import has_valid_data_files

class Downloader:
    """Gestionnaire de téléchargement asynchrone pour les sources de données."""

    def __init__(self, config: Config, log_level: str = "INFO", log_file: str = None):
        """
        Initialise le téléchargeur avec la configuration et options de logging.
        
        Args:
            config: Configuration validée
            log_level: Niveau de log ("INFO", "WARNING", "ERROR")
            log_file: Chemin vers un fichier de log (optionnel)
        """
        self.config = config
        self.cache_dir = Path(config.defaults.cache_dir)
        self.semaphore = asyncio.BoundedSemaphore(config.defaults.workers)
        self.force = config.defaults.force
        self.data_file_exts = config.defaults.data_file_exts
        self.log_level = log_level
        self.log_file = log_file
        
        # Créer le répertoire de cache s'il n'existe pas
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    async def download_all(self, callback: Callable[[str], Any] = None) -> Dict[str, Path]:
        """
        Télécharge toutes les sources configurées séquentiellement (une par une).
        
        Args:
            callback: Fonction de callback à appeler après chaque téléchargement
        
        Returns:
            Dict[str, Path]: Dictionnaire des chemins de fichiers téléchargés par slug
        """
        downloaded_paths = {}
        error_count = 0
        failed_sources = []  # Liste pour stocker les sources en échec avec leurs infos complètes
        total_sources = len(self.config.sources)
        current_source = 0
        
        for source in self.config.sources:
            current_source += 1
            slug = source.slug
            print(f"\n[INFO] Téléchargement de {slug} ({current_source}/{total_sources})...")
            
            try:
                # Télécharger une source à la fois
                result = await self.download_source(source)
                downloaded_paths[slug] = result
                print(f"[SUCCÈS] Téléchargement de {slug} terminé avec succès.")
                
                # Appeler le callback si fourni
                if callback:
                    try:
                        await callback(slug)
                    except Exception as e:
                        print(f"[ERREUR] Callback pour {slug} a échoué: {e}")
            except Exception as e:
                error_count += 1
                
                # Collecter les informations sur la source en échec
                # Déterminer l'URL réelle selon le type de source
                if source.type == "git" and source.repo:
                    # Formater l'URL GitHub complète
                    source_url = f"https://github.com/{source.repo}"
                elif source.type == "http" and source.url:
                    source_url = source.url
                elif source.type == "kaggle" and source.dataset:
                    source_url = f"https://www.kaggle.com/datasets/{source.dataset}"
                elif source.type == "wikidata":
                    source_url = f"https://www.wikidata.org/wiki/{source.access}"
                else:
                    source_url = "URL non disponible"
                
                # Ajouter le tuple (slug, ref, url) à la liste des sources en échec
                failed_sources.append((slug, source.ref, source_url))
                
                error_message = f"[ERREUR] Téléchargement de {slug} a échoué: {e}"
                print(error_message)
                
                # Créer un fichier d'erreur dans le répertoire de cache pour debugging
                error_path = self.cache_dir / slug / "download_error.log"
                error_path.parent.mkdir(parents=True, exist_ok=True)
                with open(error_path, "w", encoding="utf-8") as f:
                    f.write(f"{error_message}\n")
                    f.write(f"Type d'erreur: {type(e).__name__}\n")
                    f.write(f"Heure: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    import traceback
                    f.write(f"Trace: {traceback.format_exc()}\n")
        
        if error_count > 0:
            # Message détaillé avec la liste des sources en échec
            error_msg = f"\n[ATTENTION] {error_count} sources sur {total_sources} n'ont pas pu être téléchargées:\n"
            
            # Ajouter chaque source en échec avec son identifiant de citation et son URL
            for slug, ref, url in failed_sources:
                error_msg += f"  - {slug}: {ref} - [{url}]\n"
            
            error_msg += "\nVérifiez les fichiers d'erreur dans les dossiers correspondants."
            print(error_msg)
        else:
            print(f"\n[SUCCÈS] Toutes les {total_sources} sources ont été téléchargées avec succès!")
        
        return downloaded_paths
    
    async def download_source(self, source: Source, interactive: bool = False) -> Path:
        """
        Télécharge une source spécifique en fonction de son type.
        
        Args:
            source: Source à télécharger
            interactive: Si True, demande à l'utilisateur s'il veut forcer le téléchargement si un cache valide est détecté.
        Returns:
            Path: Chemin vers les données téléchargées
        """
        async with self.semaphore:
            cache_path = self.cache_dir / source.slug
            
            # Vérification du cache avec extensions personnalisables
            if cache_path.exists() and not self.force:
                has_valid_data = has_valid_data_files(cache_path, self.data_file_exts)
                if has_valid_data:
                    if interactive:
                        # Interaction utilisateur pour forcer ou non le téléchargement
                        try:
                            choix = input(f"[INTERACTIF] Un cache valide existe pour {source.slug}. Voulez-vous forcer le téléchargement ? (o/N) : ").strip().lower()
                        except Exception:
                            choix = ""
                        if choix == "o":
                            print(f"Téléchargement forcé pour {source.slug} malgré un cache valide.")
                        else:
                            print(f"Utilisation du cache existant pour {source.slug} (au moins un fichier de données détecté)")
                            return cache_path
                    else:
                        print(f"Utilisation du cache existant pour {source.slug} (au moins un fichier de données détecté)")
                        return cache_path
                else:
                    print(f"Cache existant pour {source.slug} mais aucun fichier de données valide trouvé, téléchargement forcé.")
                    # Continuer avec le téléchargement
            
            # Créer le répertoire de destination
            cache_path.mkdir(parents=True, exist_ok=True)
            
            if source.type == "git":
                return await self._download_git(source, cache_path)
            elif source.type == "kaggle":
                return await self._download_kaggle(source, cache_path)
            elif source.type == "http":
                return await self._download_http(source, cache_path)
            elif source.type == "wikidata":
                return await self._download_wikidata(source, cache_path)
            elif source.type == "local":
                return Path(source.url) if source.url else cache_path
            else:
                raise ValueError(f"Type de source non supporté: {source.type}")
    
    async def _download_git(self, source: Source, dest_path: Path) -> Path:
        """
        Télécharge un dépôt Git en clonant d'abord localement dans un répertoire temporaire,
        puis copie le contenu vers dest_path (utile pour les chemins réseau UNC sur Windows).
        """
        import os
        import tempfile
        import shutil
        import asyncio
        from pathlib import Path

        if not source.repo:
            raise ValueError(f"Le champ 'repo' est requis pour les sources git: {source.slug}")
        repo_url = f"https://github.com/{source.repo}.git"

        # Créer un répertoire temporaire local
        temp_root = tempfile.mkdtemp(prefix=f"git_clone_{source.slug}_", dir=os.environ.get('TEMP'))
        temp_repo = Path(temp_root) / source.slug
        temp_repo.parent.mkdir(parents=True, exist_ok=True)

        # Préparer les commandes de clonage (branche spécifiée puis branche par défaut)
        branch = getattr(source, 'ref', 'main')
        clone_cmds = [
            f'git clone -v --depth=1 --branch {branch} -- {repo_url} "{temp_repo}"',
            f'git clone -v --depth=1 -- {repo_url} "{temp_repo}"'
        ]
        last_err = ""
        for cmd in clone_cmds:
            print(f"[INFO] Exécution de la commande: {cmd}")
            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            _, err = await proc.communicate()
            if proc.returncode == 0:
                print(f"[SUCCÈS] Clonage réussi pour {source.slug}")
                break
            last_err = err.decode('utf-8', errors='replace')
            print(f"[ERREUR] Échec du clonage: {last_err}")
        else:
            # Aucun clonage n'a réussi
            shutil.rmtree(temp_root, ignore_errors=True)
            raise RuntimeError(f"Impossible de cloner {repo_url}: {last_err}")

        # Copier vers dest_path
        if dest_path.exists():
            await self.robust_rmtree(dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"[INFO] Copie du contenu vers {dest_path}")
        await asyncio.to_thread(
            shutil.copytree,
            temp_repo,
            dest_path,
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns('.git')
        )
        # Nettoyer
        shutil.rmtree(temp_root, ignore_errors=True)
        return dest_path
    
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
                for item in path.glob("*"):
                    await make_writable(item)
        except Exception as e:
            print(f"[AVERTISSEMENT] Impossible de modifier les permissions de {path}: {e}")
    
    # Fonction pour supprimer un répertoire avec plusieurs tentatives
    async def robust_rmtree(self, path, max_attempts=5):
        """Supprime un répertoire de manière robuste avec plusieurs tentatives et stratégies."""
        if not path.exists():
            return True
            
        print(f"[INFO] Tentative de suppression robuste de {path}")
        
        # Fonction pour renommer puis supprimer
        async def rename_and_remove(p):
            temp_name = f"old_{random.randint(1000, 9999)}_{int(time.time())}"
            temp_path = p.parent / temp_name
            os.rename(str(p), str(temp_path))
            await asyncio.sleep(0.5)
            shutil.rmtree(temp_path, ignore_errors=True)
        
        # Fonction pour supprimer fichier par fichier
        async def remove_files_individually(p):
            if not p.is_dir():
                return
                
            # D'abord supprimer les fichiers
            for item in p.glob("**/*"):
                if item.is_file() or item.is_symlink():
                    with suppress(Exception):
                        os.chmod(item, stat.S_IWRITE | stat.S_IREAD)
                        item.unlink(missing_ok=True)
            
            # Puis supprimer les répertoires de bas en haut
            dirs_to_remove = list(p.glob("**/*"))
            dirs_to_remove = [d for d in dirs_to_remove if d.is_dir()]
            dirs_to_remove.sort(key=lambda x: len(str(x)), reverse=True)  # Trier par profondeur
            
            for d in dirs_to_remove:
                with suppress(Exception):
                    os.chmod(d, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
                    d.rmdir()
            
            # Enfin, supprimer le répertoire racine
            with suppress(Exception):
                os.chmod(p, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
                p.rmdir()
        
        # Stratégies de suppression à essayer dans l'ordre
        strategies = [
            # Stratégie 1: shutil.rmtree standard
            lambda p: shutil.rmtree(p, ignore_errors=True),
            
            # Stratégie 2: Rendre accessible en écriture puis supprimer
            lambda p: (self.make_writable(p), shutil.rmtree(p, ignore_errors=True)),
            
            # Stratégie 3: Commande rmdir de Windows
            lambda p: subprocess.run(f'rmdir /s /q "{str(p)}"', shell=True, check=False),
            
            # Stratégie 4: Renommer puis supprimer
            rename_and_remove,
            
            # Stratégie 5: Supprimer fichier par fichier
            remove_files_individually
        ]
        
        # Essayer chaque stratégie jusqu'à ce qu'une fonctionne
        for attempt in range(max_attempts):
            strategy_index = min(attempt, len(strategies) - 1)
            strategy = strategies[strategy_index]
            
            try:
                if asyncio.iscoroutinefunction(strategy):
                    await strategy(path)
                else:
                    await asyncio.to_thread(strategy, path)
                
                # Vérifier si le répertoire existe encore
                if not path.exists():
                    print(f"[INFO] Suppression réussie de {path} (tentative {attempt+1})")
                    return True
                
                # Si le répertoire existe mais est vide, essayer de le supprimer directement
                if path.is_dir() and not any(path.iterdir()):
                    try:
                        path.rmdir()
                        print(f"[INFO] Suppression réussie du répertoire vide {path}")
                        return True
                    except Exception:
                        pass
                
                print(f"[AVERTISSEMENT] Tentative {attempt+1} échouée, essai d'une autre stratégie...")
                time.sleep(1)  # Pause avant la prochaine tentative
            except Exception as e:
                print(f"[AVERTISSEMENT] Erreur lors de la tentative {attempt+1}: {e}")
        
        print(f"[ATTENTION] Échec de toutes les tentatives de suppression pour {path}")
        return False
        
    async def _download_kaggle(self, source: Source, dest_path: Path) -> Path:
        """Télécharge un dataset Kaggle."""
        if not source.dataset:
            raise ValueError(f"Le champ 'dataset' est requis pour les sources de type kaggle: {source.slug}")
        
        # Utiliser l'API Kaggle via subprocess
        # Note: Nécessite une configuration Kaggle valide
        
        cmd = ["kaggle", "datasets", "download", "-p", str(dest_path), "--unzip", source.dataset]
        
        # Exécuter la commande de manière asynchrone
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            raise RuntimeError(f"Erreur lors du téléchargement Kaggle: {stderr.decode()}")
        
        return dest_path
    
    async def _download_http(self, source: Source, dest_path: Path) -> Path:
        """
        Téléchargement depuis une URL HTTP avec mécanisme de retry.
        
        Args:
            source: Source de données à télécharger
            dest_path: Chemin de destination pour les fichiers téléchargés
            
        Returns:
            Path: Chemin du fichier téléchargé
        """
        url = getattr(source, 'url', None)
        if not url:
            raise ValueError(f"L'URL n'est pas spécifiée pour la source {source.slug}")
            
        url = str(url)
        target_dir = dest_path
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Déterminer le nom du fichier à partir de l'URL
        filename = url.split('/')[-1]
        if not filename or filename == 'raw':
            filename = f'{source.slug}_download.dat'
            
        target_path = target_dir / filename
        
        # Vérifier si le fichier existe déjà et si on ne doit pas forcer le téléchargement
        if target_path.exists() and not self.force:
            print(f"[INFO] Le fichier {target_path} existe déjà. Utilisation du cache.")
            return target_path
        
        print(f"[INFO] Téléchargement depuis {url}...")
        
        # Paramètres pour les retries
        max_retries = 3
        retry_delay = 5  # secondes
        
        for retry in range(max_retries):
            try:
                # Configurer la session avec des timeouts et les headers appropriés
                timeout = aiohttp.ClientTimeout(total=180)  # 3 minutes de timeout total
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                }
                
                async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                    # Utiliser un timeout spécifique pour cette requête
                    async with session.get(url) as response:
                        if response.status != 200:
                            # Si on obtient une erreur 403 ou 404, changer d'URL alternative si disponible
                            if response.status in (403, 404) and retry == max_retries - 1:
                                # Chercher des URLs alternatives ou des solutions aux erreurs fréquentes
                                if 'github' in url and '/master/' in url:
                                    # Essayer avec 'main' au lieu de 'master'
                                    new_url = url.replace('/master/', '/main/')
                                    print(f"[INFO] Tentative avec URL alternative: {new_url}")
                                    url = new_url
                                    continue
                            
                            # Pour les autres erreurs, continuer avec les retries
                            if retry < max_retries - 1:
                                wait_time = retry_delay * (retry + 1)
                                print(f"[AVERTISSEMENT] Erreur HTTP {response.status} en téléchargeant {url}. Nouvelle tentative dans {wait_time}s...")
                                await asyncio.sleep(wait_time)
                                continue
                            else:
                                raise ValueError(f"Erreur HTTP {response.status} en téléchargeant {url} après {max_retries} tentatives")
                        
                        # Créer le fichier cible
                        with open(target_path, 'wb') as f:
                            # Récupérer la taille totale si disponible
                            total_size = int(response.headers.get('content-length', 0))
                            
                            if total_size > 0:
                                # Téléchargement avec barre de progression si la taille est connue
                                with tqdm(total=total_size, unit='B', unit_scale=True, desc=filename) as pbar:
                                    # Télécharger par morceaux
                                    chunk_size = 8192
                                    downloaded = 0
                                    async for chunk in response.content.iter_chunked(chunk_size):
                                        if not chunk:  # Vérifier si le chunk est vide
                                            continue
                                        f.write(chunk)
                                        downloaded += len(chunk)
                                        pbar.update(len(chunk))
                                        
                                        # Vérifier si on a téléchargé toute la taille attendue
                                        if downloaded >= total_size:
                                            break
                            else:
                                # Téléchargement sans barre de progression si la taille est inconnue
                                async for chunk in response.content.iter_chunked(8192):
                                    if chunk:  # Vérifier si le chunk est vide
                                        f.write(chunk)
                        
                print(f"[SUCCÈS] Téléchargement terminé: {target_path}")
                
                # Décompresser si c'est une archive
                if filename.endswith((".zip", ".tar.gz", ".tgz")):
                    await self._extract_archive(target_path, target_dir)
                
                return target_path
                
            except aiohttp.ClientError as e:
                if retry < max_retries - 1:
                    wait_time = retry_delay * (retry + 1)
                    print(f"[AVERTISSEMENT] Erreur réseau lors du téléchargement de {url}: {e}. Nouvelle tentative dans {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    raise Exception(f"Erreur lors du téléchargement depuis {url} après {max_retries} tentatives: {e}") from e
            except Exception as e:
                # Capturer les traces d'erreur pour le debugging
                error_trace = traceback.format_exc()
                print(f"[ERREUR] Exception non gérée pendant le téléchargement de {url}: {e}\n{error_trace}")
                raise Exception(f"Erreur lors du téléchargement depuis {url}: {e}") from e
                
    async def _download_wikidata(self, source: Source, dest_path: Path) -> Path:
        """Télécharge des données depuis Wikidata ou Zenodo."""
        # Pour ParaNames, on utilise Zenodo comme indiqué dans le cahier des charges
        if source.access == "zenodo":
            # URL réelle de ParaNames sur Zenodo
            url = "https://zenodo.org/records/5596875/files/paranames-1.0.zip"
            
            # Créer une source HTTP temporaire
            http_source = Source(
                slug=source.slug,
                type="http",
                ref=source.ref,
                url=url
            )
            
            # Utiliser la méthode de téléchargement HTTP
            return await self._download_http(http_source, dest_path)
        else:
            raise ValueError(f"Méthode d'accès non supportée pour Wikidata: {source.access}")
    
    async def _extract_archive(self, archive_path: Path, dest_path: Path) -> None:
        """Extrait une archive dans le répertoire de destination."""
        loop = asyncio.get_event_loop()
        
        if archive_path.name.endswith(".zip"):
            import zipfile
            await loop.run_in_executor(
                None,
                lambda: zipfile.ZipFile(archive_path).extractall(path=dest_path)
            )
        elif archive_path.name.endswith((".tar.gz", ".tgz")):
            import tarfile
            await loop.run_in_executor(
                None,
                lambda: tarfile.open(archive_path).extractall(path=dest_path)
            )


async def download_sources(config_path: str, callback=None) -> Dict[str, Path]:
    """
    Fonction principale pour télécharger toutes les sources.
    
    Args:
        config_path: Chemin vers le fichier de configuration
        callback: Fonction de callback à appeler après chaque téléchargement
        
    Returns:
        Dict[str, Path]: Dictionnaire des chemins de fichiers téléchargés par slug
    """
    from .config import load_config
    
    config = load_config(config_path)
    downloader = Downloader(config)
    return await downloader.download_all(callback=callback)


def is_binary_file(file_path: Path) -> bool:
    """
    Détermine si un fichier est binaire ou texte.
    
    Args:
        file_path: Chemin vers le fichier à tester
        
    Returns:
        bool: True si le fichier est binaire, False s'il est texte
    """
    # Liste des extensions connues pour être binaires
    binary_exts = {
        '.zip', '.rar', '.gz', '.tar', '.7z', '.exe', '.bin', '.dat', 
        '.pdf', '.png', '.jpg', '.jpeg', '.gif', '.mp3', '.mp4', '.avi',
        '.mov', '.mkv', '.iso', '.dll', '.so', '.dylib', '.jar',
        '.class', '.pyc', '.pyd', '.obj', '.o', '.lib', '.a'
    }
    
    # Vérifier l'extension en premier
    if file_path.suffix.lower() in binary_exts:
        return True
        
    # Utiliser les types MIME si possible
    mime_type, _ = mimetypes.guess_type(str(file_path))
    if mime_type and not mime_type.startswith('text/') and not mime_type == 'application/json':
        if 'xml' not in mime_type and 'javascript' not in mime_type and 'application/x-executable' not in mime_type:
            return True
    
    # Pour les petits fichiers, vérifier la présence de caractères nuls
    # qui indiquent généralement un fichier binaire
    if file_path.exists() and file_path.is_file():
        try:
            file_size = file_path.stat().st_size
            # Ne tester que pour les fichiers de taille raisonnable
            if file_size < 5 * 1024 * 1024:  # 5 MB max
                with open(file_path, 'rb') as f:
                    # Lire les premiers 1024 octets pour détecter des caractères nuls
                    chunk = f.read(1024)
                    if b'\x00' in chunk:
                        return True
                        
                    # Si un fichier contient des caractères non-UTF8, il est probablement binaire
                    try:
                        chunk.decode('utf-8')
                    except UnicodeDecodeError:
                        return True
        except (IOError, OSError):
            # En cas d'erreur, on considère par défaut comme texte
            pass
    
    # Par défaut, considérer comme un fichier texte
    return False

def validate_downloaded_files(paths: List[Path], expected_count: int = 0, console=None) -> Tuple[List[Path], List[Path]]:
    """
    Valide les fichiers téléchargés en vérifiant leur existence et leur accessibilité.
    
    Args:
        paths: Liste des chemins de fichiers à valider
        expected_count: Nombre attendu de fichiers (0 = pas de vérification)
        console: Console Rich pour les logs
        
    Returns:
        Tuple[List[Path], List[Path]]: (fichiers valides, fichiers invalides)
    """
    valid_files = []
    invalid_files = []
    
    # Vérifier chaque chemin
    for path in paths:
        if not path.exists():
            if console:
                console.print(f"[yellow]Avertissement: Le fichier {path} n'existe pas.[/yellow]")
            invalid_files.append(path)
            continue
            
        if not path.is_file():
            if path.is_dir():
                # Pour les répertoires, on vérifie s'ils contiennent des fichiers
                files = list(path.glob('**/*'))
                files = [f for f in files if f.is_file()]
                
                if not files:
                    if console:
                        console.print(f"[yellow]Avertissement: Le répertoire {path} ne contient aucun fichier.[/yellow]")
                    invalid_files.append(path)
                else:
                    valid_files.append(path)
            else:
                if console:
                    console.print(f"[yellow]Avertissement: {path} n'est ni un fichier ni un répertoire.[/yellow]")
                invalid_files.append(path)
        else:
            # Vérifier que le fichier est accessible en lecture
            try:
                # Pour les fichiers binaires, vérifier juste qu'ils sont accessibles
                if is_binary_file(path):
                    with open(path, 'rb') as f:
                        f.read(1)  # Lire juste un octet pour vérifier l'accès
                else:
                    # Pour les fichiers texte, essayer de les lire comme UTF-8
                    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                        f.readline()  # Lire juste une ligne
                        
                valid_files.append(path)
            except Exception as e:
                if console:
                    console.print(f"[yellow]Avertissement: Impossible de lire le fichier {path}: {e}[/yellow]")
                invalid_files.append(path)
    
    # Vérifier le nombre de fichiers si spécifié
    if expected_count > 0 and len(valid_files) != expected_count:
        if console:
            console.print(f"[yellow]Avertissement: Nombre de fichiers téléchargés ({len(valid_files)}) différent de l'attendu ({expected_count}).[/yellow]")
    
    return valid_files, invalid_files

async def download_source_data(source: Source, dest_dir: Path, console=None) -> List[Path]:
    """
    Télécharge une source spécifique dans le répertoire de destination.
    
    Args:
        source: Source à télécharger
        dest_dir: Répertoire de destination
        console: Console Rich pour l'affichage (optionnel)
        
    Returns:
        List[Path]: Liste des chemins de fichiers téléchargés
    """
    # Créer une configuration minimale pour le téléchargement de cette source uniquement
    from .config import Config, Defaults
    
    # Configurer les valeurs par défaut pour ce téléchargement spécifique
    defaults = Defaults(cache_dir=str(dest_dir), force=True)
    config = Config(sources=[source], defaults=defaults)
    
    # Créer un téléchargeur
    downloader = Downloader(config)
    
    # Télécharger la source
    paths = []
    try:
        path = await downloader.download_source(source)
        
        # Convertir en liste si c'est un chemin unique
        if isinstance(path, Path):
            source_paths = [path]
        else:
            # Si c'est déjà un itérable (liste, tuple), le conserver
            source_paths = list(path)
            
        # Valider les fichiers téléchargés
        valid_files, invalid_files = validate_downloaded_files(source_paths, console=console)
        
        if valid_files:
            paths.extend(valid_files)
            if console:
                console.print(f"[green]✓ Téléchargement de {source.slug} terminé avec succès.[/green]")
                console.print(f"[blue]✓ {len(valid_files)} fichiers valides.[/blue]")
        else:
            if console:
                console.print(f"[red]Avertissement: Aucun fichier valide trouvé pour {source.slug}.[/red]")
                
        if invalid_files and console:
            console.print(f"[yellow]⚠ {len(invalid_files)} fichiers invalides ou inaccessibles.[/yellow]")
            
    except Exception as e:
        if console:
            console.print(f"[red]Erreur lors du téléchargement de {source.slug}: {e}[/red]")
        # Pour éviter de bloquer le processus, on continue même en cas d'erreur
        pass
    
    return paths
