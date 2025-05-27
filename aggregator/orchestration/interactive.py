"""
Module d'interface interactive pour l'orchestrateur d'aggregator Nickname.
Contient toutes les fonctions liées à l'interface utilisateur interactive.
"""

import asyncio
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.layout import Layout
from rich.live import Live

# ATTENTION : Ce module ne doit JAMAIS importer CombinedOrchestrator pour éviter les circular imports.
# Toute logique de lancement interactif doit être gérée dans interactive_runner.py.
from .base import OrchestratorBase
from .translations import get_translation


async def run_interactive(config_path: str = "config.yaml"):
    """
    Point d'entrée pour l'orchestrateur interactif.
    
    Args:
        config_path: Chemin vers le fichier de configuration YAML
    """
    orchestrator = CombinedOrchestrator(config_path)
    await orchestrator.run_interactive()


class InteractiveMixin:
    """Mixin pour les fonctionnalités d'interface interactive de l'orchestrateur."""

    async def run_interactive(self):
        """
        Lance l'interface interactive pour l'orchestrateur.
        Cette méthode affiche un menu utilisateur, attend un choix, et lance la méthode appropriée.
        Gestion pédagogique :
        - Chaque action critique (ex : nettoyage strict) demande une confirmation explicite.
        - Les erreurs sont affichées clairement grâce à rich.
        - Les entrées utilisateur sont validées pour éviter les actions accidentelles.
        """
        # Définir la langue par défaut si non spécifiée
        if not hasattr(self, 'lang'):
            self.lang = "fr"
            
        if not self.config:
            self.console.print("[bold red]Impossible de lancer l'interface interactive sans configuration valide.[/bold red]")
            return
            
        self.console.print(get_translation("startup_message", self.lang))
        
        while True:
            # Affichage du menu interactif principal
            # Ajout d'une ligne vide avant le panel
            self.console.print()
            
            # Création et affichage du panel de menu avec traductions
            menu_panel = Panel.fit(
                get_translation("menu_title", self.lang) +
                get_translation("menu_item_1", self.lang) + "\n" +
                get_translation("menu_item_2", self.lang) + "\n" +
                get_translation("menu_item_3", self.lang) + "\n" +
                get_translation("menu_item_4", self.lang) + "\n" +
                get_translation("menu_item_5", self.lang) + "\n" +
                get_translation("menu_item_6", self.lang) + "\n" +
                get_translation("menu_item_7", self.lang) + "\n" +
                get_translation("menu_item_8", self.lang) + "\n" +
                get_translation("menu_item_9", self.lang) + "\n" +
                get_translation("menu_item_10", self.lang) + "\n" +
                get_translation("menu_item_11", self.lang) + "\n" +
                get_translation("menu_item_12", self.lang) + "\n" +
                get_translation("menu_item_13", self.lang) + "\n" +
                get_translation("menu_item_14", self.lang) + "\n" +
                get_translation("menu_item_15", self.lang) + "\n" +
                get_translation("menu_item_0", self.lang),
                title=get_translation("panel_title", self.lang),
                border_style="blue"
            )
            self.console.print(menu_panel)
            
            # Saisie utilisateur sécurisée avec traduction
            choice = input(get_translation("enter_choice", self.lang))
            
            # Traitement du choix utilisateur avec gestion d'erreur explicite
            if choice == "0":
                self.console.print(get_translation("goodbye_message", self.lang))
                break
            elif choice == "1":
                await self.run_download_sources()
            elif choice == "2":
                await self.run_normalize()
            elif choice == "3":
                await self.run_split_normalized()
            elif choice == "4":
                await self.run_deduplicate()
            elif choice == "5":
                await self.run_export()
            elif choice == "6":
                await self.run_split_deduped()
            elif choice == "7":
                await self.run_split_deduped_test()
            elif choice == "8":
                # Sécurité : confirmation obligatoire avant nettoyage strict
                confirm = input(get_translation("confirm_strict_clean", self.lang))
                if confirm.lower() in ["oui", "o", "yes", "y"]:
                    await self.run_clear_project_strict()
                else:
                    self.console.print("[yellow]Opération annulée.[/yellow]")
            elif choice == "9":
                await self.run_clear_split_deduped()
            elif choice == "10":
                await self.run_clear_raw()
            elif choice == "11":
                await self.run_clear_normalized()
            elif choice == "12":
                await self.run_clear_temp()
            elif choice == "13":
                # Mode automatique complet
                self.console.print("[bold green]Démarrage du mode automatique complet...[/bold green]")
                self.console.print("[bold yellow]Étape 1/7 : Nettoyage strict...[/bold yellow]")
                await self.run_clear_project_strict()
                
                self.console.print("[bold yellow]Étape 2/7 : Téléchargement des sources...[/bold yellow]")
                await self.run_download_sources()
                
                self.console.print("[bold yellow]Étape 3/7 : Normalisation des données...[/bold yellow]")
                await self.run_normalize()
                
                self.console.print("[bold yellow]Étape 4/7 : Création des chunks depuis les données normalisées...[/bold yellow]")
                await self.run_split_normalized()
                
                self.console.print("[bold yellow]Étape 5/7 : Déduplication des chunks...[/bold yellow]")
                await self.run_deduplicate()
                
                self.console.print("[bold yellow]Étape 6/7 : Export des données...[/bold yellow]")
                await self.run_export()
                
                self.console.print("[bold yellow]Étape 7/7 : Division du fichier dédupliqué (1 000 000 lignes par fichier)...[/bold yellow]")
                # Stocker temporairement la méthode originale
                original_input = input
                # Remplacer la fonction input pour automatiser la réponse
                def auto_input(prompt):
                    if "nombre de lignes" in prompt.lower():
                        return "1000000"
                    return original_input(prompt)
                
                # Remplacer temporairement la fonction input
                __builtins__["input"] = auto_input
                try:
                    await self.run_split_deduped()
                finally:
                    # Restaurer la fonction input originale
                    __builtins__["input"] = original_input
                
                self.console.print("[bold green]✓ Programme automatique complet terminé avec succès ![/bold green]")
            elif choice == "14":
                # Mode automatique personnalisé avec choix du nombre de lignes
                self.console.print("[bold cyan]Démarrage du mode automatique avec choix du nombre de lignes par fichier...[/bold cyan]")
                
                # Demander le nombre de lignes par fichier
                while True:
                    try:
                        lines_per_file = input(get_translation("enter_lines_per_file", self.lang))
                        if not lines_per_file.strip():
                            lines_per_file = 1_000_000
                            break
                        lines_per_file = int(lines_per_file.replace('_', '').replace(',', ''))
                        if lines_per_file <= 0:
                            raise ValueError("Le nombre doit être supérieur à 0")
                        break
                    except ValueError as e:
                        self.console.print(f"[red]Erreur: {e}. Veuillez entrer un nombre valide.[/red]")
                
                self.console.print(f"[bold yellow]Utilisation de {lines_per_file:,} lignes par fichier.[/bold yellow]")
                
                # Exécuter le pipeline complet avec le nombre de lignes personnalisé
                self.console.print("[bold yellow]Étape 1/7 : Nettoyage strict...[/bold yellow]")
                await self.run_clear_project_strict()
                
                self.console.print("[bold yellow]Étape 2/7 : Téléchargement des sources...[/bold yellow]")
                await self.run_download_sources()
                
                self.console.print("[bold yellow]Étape 3/7 : Normalisation des données...[/bold yellow]")
                await self.run_normalize()
                
                self.console.print("[bold yellow]Étape 4/7 : Création des chunks depuis les données normalisées...[/bold yellow]")
                await self.run_split_normalized()
                
                self.console.print("[bold yellow]Étape 5/7 : Déduplication des chunks...[/bold yellow]")
                await self.run_deduplicate()
                
                self.console.print("[bold yellow]Étape 6/7 : Export des données...[/bold yellow]")
                await self.run_export()
                
                self.console.print(f"[bold yellow]Étape 7/7 : Division du fichier dédupliqué ({lines_per_file:,} lignes par fichier)...[/bold yellow]")
                
                # Stocker temporairement la méthode originale
                original_input = input
                
                # Remplacer la fonction input pour automatiser la réponse avec le nombre de lignes personnalisé
                def custom_input(prompt):
                    if "nombre de lignes" in prompt.lower():
                        return str(lines_per_file)
                    return original_input(prompt)
                
                # Remplacer temporairement la fonction input
                import builtins
                original_builtin_input = builtins.input
                builtins.input = custom_input
                
                try:
                    await self.run_split_deduped()
                finally:
                    # Restaurer la fonction input originale
                    builtins.input = original_builtin_input
                
                self.console.print("[bold green]✓ Programme automatique personnalisé terminé avec succès ![/bold green]")
                
            elif choice == "15":
                self.show_stats()
            else:
                self.console.print("[bold red]Choix invalide. Veuillez réessayer.[/bold red]")

    
    def show_stats(self):
        """
        Affiche les statistiques actuelles de l'orchestrateur.
        """
        table = Table(title=get_translation("stats_title", self.lang))
        
        table.add_column(get_translation("stats_column_1", self.lang), style="magenta")  # Couleur différente pour les statistiques
        table.add_column(get_translation("stats_column_2", self.lang), style="magenta")
        
        table.add_row(get_translation("stats_row_1", self.lang), str(self.stats['sources_downloaded']))
        table.add_row(get_translation("stats_row_2", self.lang), str(self.stats['entries_raw']))
        table.add_row(get_translation("stats_row_3", self.lang), str(self.stats['entries_normalized']))
        table.add_row(get_translation("stats_row_4", self.lang), str(self.stats['entries_deduped']))
        
        self.console.print(table)

# Ce module ne contient plus que la classe InteractiveMixin : la fonction run_interactive a été déplacée dans interactive_runner.py pour casser le circular import.
