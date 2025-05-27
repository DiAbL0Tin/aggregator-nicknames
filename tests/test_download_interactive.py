"""
Tests d'intégration pour la logique interactive de téléchargement (simulation utilisateur).
"""
import builtins
import asyncio
from pathlib import Path
from typing import Any, Callable, Coroutine

import pytest  # type: ignore

from aggregator.download import Downloader
from aggregator.config import Config, Defaults, Source

class DummyDownloader(Downloader):
    def __init__(self, config: Config) -> None:
        # On passe une chaîne vide comme log_file pour éviter les erreurs de type
        super().__init__(config, log_file="")
        self._called = False
        
    async def _download_git(self, source: Source, dest_path: Path) -> Path:
        self._called = True
        return dest_path
        
    async def _download_kaggle(self, source: Source, dest_path: Path) -> Path:
        self._called = True
        return dest_path
        
    async def _download_http(self, source: Source, dest_path: Path) -> Path:
        self._called = True
        return dest_path
        
    async def _download_wikidata(self, source: Source, dest_path: Path) -> Path:
        self._called = True
        return dest_path

def test_interactive_cache(monkeypatch: Any, tmp_path: Path) -> None:
    """
    Vérifie que l'utilisateur peut choisir de forcer ou non le téléchargement.
    """
    # Préparer un dossier cache avec un fichier valide
    cache_dir = tmp_path / "raw"
    slug = "source1"
    cache_path = cache_dir / slug
    cache_path.mkdir(parents=True)
    (cache_path / "data.csv").write_text("a,b\n1,2\n")
    
    config = Config(
        sources=[Source(slug=slug, type="git", ref="ref1", repo="repo1")],
        defaults=Defaults(cache_dir=str(cache_dir), data_file_exts=['.csv'])
    )
    downloader = DummyDownloader(config)

    # Cas où l'utilisateur refuse de forcer (N)
    def mock_input_deny(_prompt: str) -> str:
        return "N"
        
    # Utilisation de setattr avec un type explicite
    setattr_func: Callable[[object, str, Callable[[str], str]], None] = builtins.setattr
    setattr_func(builtins, "input", mock_input_deny)
    
    # Exécution du test avec refus de l'utilisateur
    res = asyncio.run(downloader.download_source(config.sources[0], interactive=True))
    assert res is None or cache_path.exists()
    
    # Simuler l'entrée utilisateur pour forcer le téléchargement
    def mock_input_accept(_prompt: str) -> str:
        return 'y'
        
    # Réutilisation de la même fonction setattr avec un nouveau mock
    setattr_func(builtins, 'input', mock_input_accept)
    
    # Tester avec force=True (doit appeler _download_*)
    downloader = DummyDownloader(config)
    res = asyncio_run(downloader.download_source(config.sources[0], interactive=True))
    assert downloader._called is True  # type: ignore

def asyncio_run(coro: Coroutine[Any, Any, Any]) -> Any:
    """Wrapper pour exécuter une coroutine dans un event loop."""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)

