"""
Module de configuration pour aggregator Nickname.
Définit les modèles Pydantic pour la validation des configurations.
"""

from typing import Dict, List, Literal, Optional
from pydantic import BaseModel, Field, HttpUrl


class Source(BaseModel):
    """Modèle pour une source de données."""
    slug: str = Field(..., description="Identifiant unique de la source")
    type: Literal["git", "kaggle", "wikidata", "http", "local"] = Field(
        ..., description="Type de source"
    )
    ref: str = Field(..., description="Identifiant de citation")
    repo: Optional[str] = Field(None, description="Dépôt Git (pour type=git)")
    dataset: Optional[str] = Field(None, description="Dataset Kaggle (pour type=kaggle)")
    access: Optional[str] = Field(None, description="Méthode d'accès spécifique")
    url: Optional[HttpUrl] = Field(None, description="URL pour téléchargement direct")
    is_email: Optional[bool] = Field(False, description="Indique si la source contient des emails")
    path: Optional[str] = Field(None, description="Chemin spécifique dans le dépôt")


class Defaults(BaseModel):
    """Paramètres par défaut pour l'application."""
    cache_dir: str = Field("data/raw", description="Répertoire de cache pour les données brutes")
    force: bool = Field(False, description="Forcer le téléchargement même si le cache existe")
    workers: int = Field(32, description="Nombre de workers pour les opérations asynchrones")
    data_file_exts: List[str] = Field(default_factory=lambda: ['.txt', '.csv', '.parquet', '.json', '.tsv'], description="Extensions de fichiers de données valides (avec le point)")


class Config(BaseModel):
    """Configuration globale de l'application."""
    sources: List[Source] = Field(..., description="Liste des sources de données")
    defaults: Defaults = Field(default_factory=Defaults, description="Paramètres par défaut")


def load_config(config_path: str) -> Config:
    """
    Charge la configuration depuis un fichier YAML.
    
    Args:
        config_path: Chemin vers le fichier de configuration YAML
        
    Returns:
        Config: Configuration validée
    """
    import yaml
    
    with open(config_path, "r", encoding="utf-8") as f:
        config_data = yaml.safe_load(f)
    
    # Créer l'instance de Config
    config = Config(**config_data)
    # Convertir les URLs en str pour correspondre aux attentes des tests
    for src in config.sources:
        if src.url is not None:
            src.url = str(src.url)
    return config
