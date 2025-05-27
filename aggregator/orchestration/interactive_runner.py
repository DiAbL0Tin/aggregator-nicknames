"""
Module dédié au lancement interactif de l'orchestrateur.
Séparé pour éviter tout circular import entre InteractiveMixin et CombinedOrchestrator.
"""
import argparse

def run_interactive(config_path: str = "config.yaml", lang: str = "fr"):
    """
    Point d'entrée pour l'orchestrateur interactif.
    Args:
        config_path: Chemin vers le fichier de configuration YAML
        lang: Langue de l'interface (fr ou en)
    """
    # Import local pour éviter le circular import
    from .combined import CombinedOrchestrator
    import asyncio
    
    # Vérification de la langue
    if lang not in ["fr", "en"]:
        print(f"Langue non supportée: {lang}. Utilisation du français par défaut.")
        lang = "fr"
    
    orchestrator = CombinedOrchestrator(config_path)
    # Stockage de la langue pour l'utiliser dans run_interactive
    # Nous utiliserons un attribut dynamique qui sera récupéré dans la méthode run_interactive
    setattr(orchestrator, 'lang', lang)
    
    try:
        # Lancement de l'interface interactive principale
        asyncio.run(orchestrator.run_interactive())
    except KeyboardInterrupt:
        # Gestion de l'interruption utilisateur (CTRL+C)
        if hasattr(orchestrator, 'console'):
            if lang == "fr":
                message = "\n[bold yellow]Programme interrompu par l'utilisateur (CTRL+C). Arrêt propre...[/bold yellow]"
            else:
                message = "\n[bold yellow]Program interrupted by user (CTRL+C). Clean shutdown...[/bold yellow]"
            orchestrator.console.print(message)


# Fonction pour lancer le script depuis la ligne de commande
def main():
    parser = argparse.ArgumentParser(description="Aggregator Nickname - Interface Interactive")
    parser.add_argument("--lang", "-l", type=str, choices=["fr", "en"], default="fr", 
                        help="Langue de l'interface (fr ou en, défaut: fr)")
    parser.add_argument("--config", "-c", type=str, default="config.yaml",
                        help="Chemin vers le fichier de configuration (défaut: config.yaml)")
    
    args = parser.parse_args()
    run_interactive(config_path=args.config, lang=args.lang)


if __name__ == "__main__":
    main()
