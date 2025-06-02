"""
Module de traduction pour l'interface interactive.
Contient les traductions des textes de l'interface.
"""

TRANSLATIONS = {
    "fr": {
        # Messages généraux
        "startup_message": "[bold green]Démarrage de l'interface interactive d'aggregator Nickname...[/bold green]",
        "goodbye_message": "[bold green]Au revoir ![/bold green]",
        "invalid_choice": "[bold red]Choix invalide. Veuillez réessayer.[/bold red]",
        
        # Titre du menu
        "menu_title": "[bold cyan]Menu Interactif aggregator Nickname[/bold cyan]\n\n",
        "auto_menu_title": "[bold green]Menu automatique :[/bold green]\n",
        "auto_option_1": "1. Exporter tout (pseudonymes, emails, mots de passe)",
        "auto_option_2": "2. Exporter uniquement les pseudonymes",
        "auto_option_3": "3. Exporter uniquement les emails",
        "auto_option_4": "4. Exporter uniquement les mots de passe",
        "auto_option_0": "0. Quitter\n",
        "manual_menu_title": "[bold cyan]Menu manuel :[/bold cyan]\n",
        "manual_option_5": "5. Télécharger tout",
        "manual_option_6": "6. Exporter tout",
        "manual_option_7": "7. Split / déduplication",
        "manual_option_8": "8. Exporter uniquement les pseudonymes",
        "manual_option_9": "9. Exporter uniquement les emails",
        "manual_option_10": "10. Exporter uniquement les mots de passe",
        "manual_option_11": "11. Nettoyer entièrement le projet (suppression TOTALE)",
        "manual_option_12": "12. Afficher les statistiques",
        "manual_option_0": "0. Quitter",
        "panel_title": "[bold]aggregator Nickname[/bold]",
        
        # Options du menu
        "menu_item_1": "1. Télécharger les sources",
        "menu_item_2": "2. Normaliser les données",
        "menu_item_3": "3. Créer des chunks depuis les données normalisées",
        "menu_item_4": "4. Dédupliquer les chunks",
        "menu_item_5": "5. Exporter les données",
        "menu_item_6": "6. Diviser le fichier dédupliqué",
        "menu_item_7": "7. [bold yellow]Diviser le fichier dédupliqué en mode test[/bold yellow]",
        "menu_item_8": "8. [bold red]Nettoyage strict (whitelist)[/bold red]",
        "menu_item_9": "9. Vider le dossier final",
        "menu_item_10": "10. Vider le dossier raw",
        "menu_item_11": "11. Vider le cache normalisé",
        "menu_item_12": "12. Vider les fichiers temporaires",
        "menu_item_13": "13. [bold green]Lancer le programme en mode automatique complet[/bold green]",
        "menu_item_14": "14. [bold cyan]Lancer le programme en mode automatique avec choix du nombre de lignes par fichier[/bold cyan]",
        "menu_item_15": "15. [bold magenta]Afficher les statistiques[/bold magenta]",
        "menu_item_16": "16. [bold blue]Exporter séparément les emails et pseudonymes[/bold blue]",
        "menu_item_0": "0. Quitter",
        
        # Entrée utilisateur
        "enter_choice": "Entrez votre choix (0-16): ",
        "confirm_strict_clean": "[bold red]ATTENTION: Cette opération va supprimer tous les fichiers non essentiels du projet. Confirmer? (oui/non): [/bold red]",
        "enter_lines_per_file": "Entrez le nombre de lignes par fichier (appuyez sur Entrée pour 1 000 000) : ",
        "modify_lines_per_file": "Souhaitez-vous modifier ce nombre de lignes par fichier ? (Entrez un nombre ou appuyez sur Entrée pour conserver la valeur par défaut) : ",
        
        # Statistiques
        "stats_title": "Statistiques aggregator Nickname",
        "stats_column_1": "Métrique",
        "stats_column_2": "Valeur",
        "stats_row_1": "Sources téléchargées",
        "stats_row_2": "Entrées brutes",
        "stats_row_3": "Entrées normalisées",
        "stats_row_4": "Entrées dédupliquées",
        
        # Erreurs
        "errors_title": "Erreurs rencontrées pendant l'exécution",
        "errors_column_phase": "Phase",
        "errors_column_type": "Type",
        "errors_column_message": "Message",
        "errors_column_time": "Heure",
        "no_errors": "Aucune erreur rencontrée pendant l'exécution",
        "welcome_title": "Bienvenue dans aggregator Nickname!"
    },
    "en": {
        # Messages généraux
        "startup_message": "[bold green]Starting aggregator Nickname interactive interface...[/bold green]",
        "goodbye_message": "[bold green]Goodbye![/bold green]",
        "invalid_choice": "[bold red]Invalid choice. Please try again.[/bold red]",
        
        # Titre du menu
        "menu_title": "[bold cyan]aggregator Nickname Interactive Menu[/bold cyan]\n\n",
        "auto_menu_title": "[bold green]Automatic menu:[/bold green]\n",
        "auto_option_1": "1. Export all (nicknames, emails, passwords)",
        "auto_option_2": "2. Export only nicknames",
        "auto_option_3": "3. Export only emails",
        "auto_option_4": "4. Export only passwords",
        "auto_option_0": "0. Exit\n",
        "manual_menu_title": "[bold cyan]Manual menu:[/bold cyan]\n",
        "manual_option_5": "5. Download all",
        "manual_option_6": "6. Export all",
        "manual_option_7": "7. Split / deduplication",
        "manual_option_8": "8. Export only nicknames",
        "manual_option_9": "9. Export only emails",
        "manual_option_10": "10. Export only passwords",
        "manual_option_11": "11. Clear entire project (FULL removal)",
        "manual_option_12": "12. Show statistics",
        "manual_option_0": "0. Exit",
        "panel_title": "[bold]aggregator Nickname[/bold]",
        
        # Options du menu
        "menu_item_1": "1. Download sources",
        "menu_item_2": "2. Normalize data",
        "menu_item_3": "3. Create chunks from normalized data",
        "menu_item_4": "4. Deduplicate chunks",
        "menu_item_5": "5. Export data",
        "menu_item_6": "6. Split deduplicated file",
        "menu_item_7": "7. [bold yellow]Split deduplicated file in test mode[/bold yellow]",
        "menu_item_8": "8. [bold red]Strict cleaning (whitelist)[/bold red]",
        "menu_item_9": "9. Clear final folder",
        "menu_item_10": "10. Clear raw folder",
        "menu_item_11": "11. Clear normalized cache",
        "menu_item_12": "12. Clear temporary files",
        "menu_item_13": "13. [bold green]Run program in complete automatic mode[/bold green]",
        "menu_item_14": "14. [bold cyan]Run program in automatic mode with custom lines per file[/bold cyan]",
        "menu_item_15": "15. [bold magenta]Show statistics[/bold magenta]",
        "menu_item_16": "16. [bold blue]Export emails and nicknames separately[/bold blue]",
        "menu_item_0": "0. Exit",
        
        # Entrée utilisateur
        "enter_choice": "Enter your choice (0-16): ",
        "confirm_strict_clean": "[bold red]WARNING: This operation will delete all non-essential files from the project. Confirm? (yes/no): [/bold red]",
        "enter_lines_per_file": "Enter the number of lines per file (press Enter for 1,000,000): ",
        "modify_lines_per_file": "Would you like to modify the number of lines per file? (Enter a number or press Enter to keep the default value): ",
        
        # Statistiques
        "stats_title": "aggregator Nickname Statistics",
        "stats_column_1": "Metric",
        "stats_column_2": "Value",
        "stats_row_1": "Downloaded sources",
        "stats_row_2": "Raw entries",
        "stats_row_3": "Normalized entries",
        "stats_row_4": "Deduplicated entries"
    }
}

def get_translation(key: str, lang: str = "fr") -> str:
    """
    Récupère la traduction d'une clé dans la langue spécifiée.
    
    Args:
        key: Clé de traduction
        lang: Code de langue (fr ou en)
        
    Returns:
        str: Texte traduit
    """
    if lang not in TRANSLATIONS:
        lang = "fr"  # Fallback to French
        
    if key not in TRANSLATIONS[lang]:
        # Si la clé n'existe pas, retourner la clé elle-même
        return key
        
    return TRANSLATIONS[lang][key]
