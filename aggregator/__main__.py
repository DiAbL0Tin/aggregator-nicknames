"""
Point d'entrée principal pour l'application aggregator Nickname.
Permet de lancer l'application avec la commande `python -m aggregator`.
"""

def main():
    """Point d'entrée principal pour l'application."""
    config_path = "config.yaml"
    
    # Demande de la langue à l'utilisateur
    print("\n===== Aggregator Nickname =====\n")
    print("Choisissez la langue / Choose language:")
    print("1. Français (par défaut/default)")
    print("2. English")
    
    try:
        choice = input("Choix/Choice [1-2]: ").strip()
        # Par défaut, utiliser le français
        lang = "fr"
        
        if choice == "2":
            lang = "en"
            print("\nEnglish interface selected.\n")
        else:
            print("\nInterface française sélectionnée.\n")
    except (KeyboardInterrupt, EOFError):
        # Si l'utilisateur interrompt, utiliser le français par défaut
        print("\nUtilisation de l'interface en français (par défaut).\n")
        lang = "fr"
    
    # Utilisation de la fonction run() du module aggregator
    from aggregator import run
    
    # Lancement de l'interface interactive
    run(config_path=config_path, lang=lang)

if __name__ == "__main__":
    main()
