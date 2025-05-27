#!/usr/bin/env python3
"""
Point d'entrée alternatif pour l'application aggregator Nickname.
Permet de lancer l'application avec la commande `python run_menu.py`.
"""

import sys
import traceback

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
    
    try:
        # Utilisation de la fonction run() du module aggregator
        from aggregator import run
        
        # Lancement de l'interface interactive
        run(config_path=config_path, lang=lang)
    except KeyboardInterrupt:
        print("\n\nProgramme interrompu par l'utilisateur. Au revoir !")
        sys.exit(0)
    except EOFError:
        print("\n\nErreur de lecture de l'entrée utilisateur. Vérifiez votre terminal.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nErreur inattendue: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nErreur critique: {e}")
        traceback.print_exc()
        sys.exit(1)
