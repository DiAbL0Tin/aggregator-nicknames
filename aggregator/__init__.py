"""
aggregator Nickname - Un outil pour agréger des pseudos, prénoms et noms à partir de multiples sources.
"""

__version__ = "0.1.0"


def run(config_path: str = "config.yaml", lang: str = "fr") -> None:
    """
    Lance l'interface interactive de l'aggregator.
    
    Args:
        config_path: Chemin vers le fichier de configuration YAML
        lang: Langue de l'interface (fr ou en)
    """
    from aggregator.orchestration.interactive_runner import run_interactive
    return run_interactive(config_path=config_path, lang=lang)
