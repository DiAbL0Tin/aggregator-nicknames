"""
Module d'interface interactive pour l'orchestrateur d'aggregator Nickname.
Contient toutes les fonctions liées à l'interface utilisateur interactive.
"""

from typing import Dict, Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import Config
    from rich.console import Console as RichConsoleForTyping # Alias pour le type checking
from pathlib import Path

# Imports utilisés directement dans ce module
from rich.panel import Panel
from rich.table import Table

# ATTENTION : Ce module ne doit JAMAIS importer CombinedOrchestrator pour éviter les circular imports.
# Toute logique de lancement interactif doit être gérée dans interactive_runner.py.
from .translations import get_translation

# Imports conditionnels pour les annotations de type
if TYPE_CHECKING:
    from rich.console import Console


# Cette fonction a été déplacée dans interactive_runner.py
# pour éviter les circular imports


class InteractiveMixin:
    # Déclarations de type pour les attributs et méthodes attendus d'autres Mixins ou classes de base
    # Déclarations de type pour les attributs attendus
    config: 'Config'  # Sera résolu par le bloc TYPE_CHECKING
    stats: Dict[str, Any]
    console: 'RichConsoleForTyping' # Sera résolu par le bloc TYPE_CHECKING

    # Déclarations de méthodes attendues (implémentées dans d'autres mixins)
    async def run_clear_project_strict(self) -> None:
        ...

    async def run_download_sources(self) -> None:
        ...

    async def run_normalize(self) -> None:
        ...
    # D'autres méthodes comme run_create_chunks, etc., pourraient nécessiter des déclarations similaires
    # si des erreurs de lint apparaissent pour elles ou si elles ont des implémentations vides ici.

    """
    Mixin pour les fonctionnalités d'interface interactive de l'orchestrateur.
    
    Note: Cette classe est conçue pour être utilisée avec OrchestratorBase qui fournit
    les attributs suivants:
        - console: Instance de rich.console.Console
        - config: Configuration chargée
        - stats: Dictionnaire des statistiques
    
    Cette classe est destinée à être utilisée avec d'autres mixins qui fournissent
    les méthodes suivantes:
        - CleaningMixin: run_clear_project_strict
        - SplittingMixin: run_split_normalized, run_split_deduped, run_deduplicate
        - UtilsMixin: run_download_sources, run_normalize, run_export_all, etc.
    """
    
    # Attributs attendus (fournis par OrchestratorBase)
    console: 'Console'
    config: Any
    stats: Dict[str, int]
    lang: str
    
    # Attributs de chemins (fournis par OrchestratorBase)
    normalized_dir: Path
    output_dir: Path
    deduped_dir: Path
    deduped_path: Optional[Path] = None
    
    # Déclarations des méthodes fournies par d'autres mixins
    # Ces déclarations sont uniquement pour le linter et ne sont pas implémentées ici
    
    # Méthodes de CleaningMixin
    async def run_clear_project_strict(self) -> None: ...
    
    # Méthodes de SplittingMixin
    async def run_split_normalized(self) -> None: ...
    async def run_split_deduped(self, auto_mode: bool = False) -> None: ...
    async def run_deduplicate(self) -> None: ...
    
    # Méthodes de UtilsMixin
    async def run_export_all(self) -> None: ...
    async def run_export_nicknames(self) -> None: ...
    async def run_export_emails(self) -> None: ...
    async def run_export_passwords(self) -> None: ...
    async def reload_config(self) -> bool: ...

    async def run_interactive(self) -> None:
        """
        Lance l'interface interactive pour l'orchestrateur.
        Cette méthode affiche un menu utilisateur, attend un choix, et lance la méthode appropriée.
        Gestion pédagogique :
        - Chaque action critique (ex : nettoyage strict) demande une confirmation explicite.
        - Les erreurs sont affichées clairement grâce à rich.
        - Les entrées utilisateur sont validées pour éviter les actions accidentelles.
        
        Requires:
            - self.console: Instance de rich.console.Console (fournie par OrchestratorBase)
            - self.config: Configuration chargée (fournie par OrchestratorBase)
        """
        # Vérification que les attributs requis sont disponibles
        if not hasattr(self, 'console'):
            from rich.console import Console
            self.console = Console()
            print("[WARNING] L'attribut 'console' n'était pas disponible et a été créé dynamiquement.")
        # Définir la langue par défaut si non spécifiée
        if not hasattr(self, 'lang'):
            self.lang = "fr"
            
        if not self.config:
            self.console.print("[bold red]Impossible de lancer l'interface interactive sans configuration valide.[/bold red]")
            return
            
        self.console.print(get_translation("startup_message", self.lang))

        self.console.print(f"[bold red]DEBUG INTERACTIVE: self.run_download_sources est {self.run_download_sources}[/bold red]")
        import inspect
        try:
            self.console.print(f"[bold red]DEBUG INTERACTIVE: Fichier de self.run_download_sources: {inspect.getfile(self.run_download_sources)}[/bold red]")
        except TypeError:
            self.console.print(f"[bold red]DEBUG INTERACTIVE: Impossible d'obtenir le fichier pour self.run_download_sources (pourrait être une méthode C ou non trouvée).[/bold red]")
    
        while True:
            # Affichage du menu interactif principal
            # Ajout d'une ligne vide avant le panel
            self.console.print()
            
            # Affichage du menu automatique
            panel_auto = Panel.fit(
                get_translation("auto_menu_title", self.lang) +
                get_translation("auto_option_1", self.lang) + "\n" +
                get_translation("auto_option_2", self.lang) + "\n" +
                get_translation("auto_option_3", self.lang) + "\n" +
                get_translation("auto_option_4", self.lang) + "\n" +
                get_translation("auto_option_0", self.lang),
                title=get_translation("panel_title", self.lang),
                border_style="green"
            )
            self.console.print(panel_auto)

            # Affichage du menu manuel
            panel_man = Panel.fit(
                get_translation("manual_menu_title", self.lang) +
                get_translation("manual_option_5", self.lang) + "\n" +
                get_translation("manual_option_7", self.lang) + "\n" +
                get_translation("manual_option_6", self.lang) + "\n" +
                get_translation("manual_option_8", self.lang) + "\n" +
                get_translation("manual_option_9", self.lang) + "\n" +
                get_translation("manual_option_10", self.lang) + "\n" +
                get_translation("manual_option_11", self.lang) + "\n" +
                get_translation("manual_option_12", self.lang) + "\n" +
                get_translation("manual_option_0", self.lang),
                title=get_translation("panel_title", self.lang),
                border_style="cyan"
            )
            self.console.print(panel_man)

            # Saisie utilisateur sécurisée avec traduction
            choice = input(get_translation("enter_choice", self.lang))

            # Traitement du choix utilisateur pour les deux menus fusionnés
            if choice == "0":
                self.console.print(get_translation("goodbye_message", self.lang))
                break
            elif choice == "1":
                # Menu automatique - exécute toutes les étapes nécessaires
                try:
                    # Réinitialiser le log d'erreurs au début du processus automatique
                    self.errors_log = []
                    # Étape 1: Nettoyage du projet (obligatoire)
                    self.console.print("[bold yellow]Étape 1/7 : Nettoyage complet du projet...[/bold yellow]")
                    await self.run_clear_project_strict()
                    
                    
                    # Étape 2: Téléchargement des sources (obligatoire)
                    self.console.print("[bold yellow]Étape 2/7 : Téléchargement des sources...[/bold yellow]")
                    self.console.print("[bold magenta]DEBUG INTERACTIVE: JUSTE APRÈS L'AFFICHAGE DE L'ÉTAPE 2/7 (Processus Auto)[/bold magenta]")
                    await self.run_download_sources()
                    
                    # Vérifier si des sources ont été téléchargées
                    if self.stats['sources_downloaded'] == 0:
                        self.console.print("[bold red]Erreur : Aucune source n'a été téléchargée. Processus automatique interrompu.[/bold red]")
                        continue
                    
                    # Étape 3: Normalisation des données (obligatoire)
                    self.console.print("[bold yellow]Étape 3/7 : Normalisation des données...[/bold yellow]")
                    await self.run_normalize()
                    
                    # Vérifier si des données ont été normalisées
                    if self.stats['entries_normalized'] == 0:
                        self.console.print("[bold red]Erreur : Aucune donnée n'a été normalisée. Processus automatique interrompu.[/bold red]")
                        continue
                    
                    # Étape 4: Création des chunks (peut échouer si pas de données normalisées)
                    self.console.print("[bold yellow]Étape 4/7 : Création des chunks depuis les données normalisées...[/bold yellow]")
                    # Vérifier d'abord si des données normalisées existent
                    if not self.normalized_dir.exists() or not any(self.normalized_dir.glob("*.*")):
                        self.console.print("[bold red]Erreur : Aucune donnée normalisée trouvée. Processus automatique interrompu.[/bold red]")
                        continue
                    await self.run_split_normalized()
                    
                    # Étape 5: Déduplication (peut échouer si pas de chunks)
                    self.console.print("[bold yellow]Étape 5/7 : Déduplication des chunks...[/bold yellow]")
                    split_dir = self.output_dir / "splits"
                    if not split_dir.exists() or not any(split_dir.glob("*.txt")):
                        self.console.print("[bold red]Erreur : Aucun chunk à dédupliquer. Processus automatique interrompu.[/bold red]")
                        continue
                    await self.run_deduplicate()
                    
                    # Vérifier si la déduplication a produit des résultats
                    if self.stats['entries_deduped'] == 0:
                        self.console.print("[bold red]Erreur : La déduplication n'a produit aucun résultat. Processus automatique interrompu.[/bold red]")
                        continue
                    
                    # Étape 6: Export des données (peut continuer même en cas d'échec)
                    self.console.print("[bold yellow]Étape 6/7 : Export des données...[/bold yellow]")
                    await self.run_export_all()  # Exporte tous les types de données
                    
                    # Étape 7: Division du fichier dédupliqué (peut échouer si pas de fichier dédupliqué)
                    self.console.print("[bold yellow]Étape 7/7 : Division du fichier dédupliqué...[/bold yellow]")
                    if not self.deduped_path or not self.deduped_path.exists():
                        self.console.print("[bold red]Erreur : Aucun fichier dédupliqué trouvé. Processus automatique interrompu.[/bold red]")
                        continue
                    await self.run_split_deduped(auto_mode=True)
                    
                    # Afficher les statistiques à la fin du processus
                    self.console.print("[bold green]✓ Processus automatique complet terminé avec succès ![/bold green]")
                    self.show_stats()
                except Exception as e:
                    self.console.print(f"[bold red]Erreur inattendue lors du processus automatique : {str(e)}[/bold red]")
                    self.console.print("[bold red]Le processus automatique a été interrompu en raison d'une erreur.[/bold red]")
            elif choice == "2":
                # Menu automatique - export pseudonymes
                try:
                    # Étape 1: Nettoyage du projet (obligatoire)
                    self.console.print("[bold yellow]Étape 1/7 : Nettoyage complet du projet...[/bold yellow]")
                    await self.run_clear_project_strict()
                    
                    
                    # Étape 2: Téléchargement des sources (obligatoire)
                    self.console.print("[bold yellow]Étape 2/7 : Téléchargement des sources...[/bold yellow]")
                    await self.run_download_sources()
                    
                    # Vérifier si des sources ont été téléchargées
                    if self.stats['sources_downloaded'] == 0:
                        self.console.print("[bold red]Erreur : Aucune source n'a été téléchargée. Processus automatique interrompu.[/bold red]")
                        continue
                    
                    # Étape 3: Normalisation des données (obligatoire)
                    self.console.print("[bold yellow]Étape 3/7 : Normalisation des données...[/bold yellow]")
                    await self.run_normalize()
                    
                    # Vérifier si des données ont été normalisées
                    if self.stats['entries_normalized'] == 0:
                        self.console.print("[bold red]Erreur : Aucune donnée n'a été normalisée. Processus automatique interrompu.[/bold red]")
                        continue
                    
                    # Étape 4: Création des chunks (peut échouer si pas de données normalisées)
                    self.console.print("[bold yellow]Étape 4/7 : Création des chunks depuis les données normalisées...[/bold yellow]")
                    # Vérifier d'abord si des données normalisées existent
                    if not self.normalized_dir.exists() or not any(self.normalized_dir.glob("*.*")):
                        self.console.print("[bold red]Erreur : Aucune donnée normalisée trouvée. Processus automatique interrompu.[/bold red]")
                        continue
                    await self.run_split_normalized()
                    
                    # Étape 5: Déduplication (peut échouer si pas de chunks)
                    self.console.print("[bold yellow]Étape 5/7 : Déduplication des chunks...[/bold yellow]")
                    split_dir = self.output_dir / "splits"
                    if not split_dir.exists() or not any(split_dir.glob("*.txt")):
                        self.console.print("[bold red]Erreur : Aucun chunk à dédupliquer. Processus automatique interrompu.[/bold red]")
                        continue
                    await self.run_deduplicate()
                    
                    # Vérifier si la déduplication a produit des résultats
                    if self.stats['entries_deduped'] == 0:
                        self.console.print("[bold red]Erreur : La déduplication n'a produit aucun résultat. Processus automatique interrompu.[/bold red]")
                        continue
                    
                    # Étape 6: Export des pseudonymes (peut continuer même en cas d'échec)
                    self.console.print("[bold yellow]Étape 6/7 : Export des pseudonymes...[/bold yellow]")
                    await self.run_export_nicknames()
                    
                    # Étape 7: Division du fichier dédupliqué (peut échouer si pas de fichier dédupliqué)
                    self.console.print("[bold yellow]Étape 7/7 : Division du fichier dédupliqué...[/bold yellow]")
                    if not self.deduped_path or not self.deduped_path.exists():
                        self.console.print("[bold red]Erreur : Aucun fichier dédupliqué trouvé. Processus automatique interrompu.[/bold red]")
                        continue
                    await self.run_split_deduped(auto_mode=True)
                    
                    # Afficher les statistiques à la fin du processus
                    self.console.print("[bold green]✓ Processus automatique complet terminé avec succès ![/bold green]")
                    self.show_stats()
                except Exception as e:
                    self.console.print(f"[bold red]Erreur inattendue lors du processus automatique : {str(e)}[/bold red]")
                    self.console.print("[bold red]Le processus automatique a été interrompu en raison d'une erreur.[/bold red]")
            elif choice == "3":
                # Menu automatique - export emails
                try:
                    # Étape 1: Nettoyage du projet (obligatoire)
                    self.console.print("[bold yellow]Étape 1/7 : Nettoyage complet du projet...[/bold yellow]")
                    await self.run_clear_project_strict()
                    
                    
                    # Étape 2: Téléchargement des sources (obligatoire)
                    self.console.print("[bold yellow]Étape 2/7 : Téléchargement des sources...[/bold yellow]")
                    await self.run_download_sources()
                    
                    # Vérifier si des sources ont été téléchargées
                    if self.stats['sources_downloaded'] == 0:
                        self.console.print("[bold red]Erreur : Aucune source n'a été téléchargée. Processus automatique interrompu.[/bold red]")
                        continue
                    
                    # Étape 3: Normalisation des données (obligatoire)
                    self.console.print("[bold yellow]Étape 3/7 : Normalisation des données...[/bold yellow]")
                    await self.run_normalize()
                    
                    # Vérifier si des données ont été normalisées
                    if self.stats['entries_normalized'] == 0:
                        self.console.print("[bold red]Erreur : Aucune donnée n'a été normalisée. Processus automatique interrompu.[/bold red]")
                        continue
                    
                    # Étape 4: Création des chunks (peut échouer si pas de données normalisées)
                    self.console.print("[bold yellow]Étape 4/7 : Création des chunks depuis les données normalisées...[/bold yellow]")
                    # Vérifier d'abord si des données normalisées existent
                    if not self.normalized_dir.exists() or not any(self.normalized_dir.glob("*.*")):
                        self.console.print("[bold red]Erreur : Aucune donnée normalisée trouvée. Processus automatique interrompu.[/bold red]")
                        continue
                    await self.run_split_normalized()
                    
                    # Étape 5: Déduplication (peut échouer si pas de chunks)
                    self.console.print("[bold yellow]Étape 5/7 : Déduplication des chunks...[/bold yellow]")
                    split_dir = self.output_dir / "splits"
                    if not split_dir.exists() or not any(split_dir.glob("*.txt")):
                        self.console.print("[bold red]Erreur : Aucun chunk à dédupliquer. Processus automatique interrompu.[/bold red]")
                        continue
                    await self.run_deduplicate()
                    
                    # Vérifier si la déduplication a produit des résultats
                    if self.stats['entries_deduped'] == 0:
                        self.console.print("[bold red]Erreur : La déduplication n'a produit aucun résultat. Processus automatique interrompu.[/bold red]")
                        continue
                    
                    # Étape 6: Export des emails (peut continuer même en cas d'échec)
                    self.console.print("[bold yellow]Étape 6/7 : Export des emails...[/bold yellow]")
                    await self.run_export_emails()
                    
                    # Étape 7: Division du fichier dédupliqué (peut échouer si pas de fichier dédupliqué)
                    self.console.print("[bold yellow]Étape 7/7 : Division du fichier dédupliqué...[/bold yellow]")
                    if not self.deduped_path or not self.deduped_path.exists():
                        self.console.print("[bold red]Erreur : Aucun fichier dédupliqué trouvé. Processus automatique interrompu.[/bold red]")
                        continue
                    await self.run_split_deduped(auto_mode=True)
                    
                    # Afficher les statistiques à la fin du processus
                    self.console.print("[bold green]✓ Processus automatique complet terminé avec succès ![/bold green]")
                    self.show_stats()
                except Exception as e:
                    self.console.print(f"[bold red]Erreur inattendue lors du processus automatique : {str(e)}[/bold red]")
                    self.console.print("[bold red]Le processus automatique a été interrompu en raison d'une erreur.[/bold red]")
            elif choice == "4":
                # Menu automatique - export mots de passe
                try:
                    # Étape 1: Nettoyage du projet (obligatoire)
                    self.console.print("[bold yellow]Étape 1/7 : Nettoyage complet du projet...[/bold yellow]")
                    await self.run_clear_project_strict()
                    
                    
                    # Étape 2: Téléchargement des sources (obligatoire)
                    self.console.print("[bold yellow]Étape 2/7 : Téléchargement des sources...[/bold yellow]")
                    await self.run_download_sources()
                    
                    # Vérifier si des sources ont été téléchargées
                    if self.stats['sources_downloaded'] == 0:
                        self.console.print("[bold red]Erreur : Aucune source n'a été téléchargée. Processus automatique interrompu.[/bold red]")
                        continue
                    
                    # Étape 3: Normalisation des données (obligatoire)
                    self.console.print("[bold yellow]Étape 3/7 : Normalisation des données...[/bold yellow]")
                    await self.run_normalize()
                    
                    # Vérifier si des données ont été normalisées
                    if self.stats['entries_normalized'] == 0:
                        self.console.print("[bold red]Erreur : Aucune donnée n'a été normalisée. Processus automatique interrompu.[/bold red]")
                        continue
                    
                    # Étape 4: Création des chunks (peut échouer si pas de données normalisées)
                    self.console.print("[bold yellow]Étape 4/7 : Création des chunks depuis les données normalisées...[/bold yellow]")
                    # Vérifier d'abord si des données normalisées existent
                    if not self.normalized_dir.exists() or not any(self.normalized_dir.glob("*.*")):
                        self.console.print("[bold red]Erreur : Aucune donnée normalisée trouvée. Processus automatique interrompu.[/bold red]")
                        continue
                    await self.run_split_normalized()
                    
                    # Étape 5: Déduplication (peut échouer si pas de chunks)
                    self.console.print("[bold yellow]Étape 5/7 : Déduplication des chunks...[/bold yellow]")
                    split_dir = self.output_dir / "splits"
                    if not split_dir.exists() or not any(split_dir.glob("*.txt")):
                        self.console.print("[bold red]Erreur : Aucun chunk à dédupliquer. Processus automatique interrompu.[/bold red]")
                        continue
                    await self.run_deduplicate()
                    
                    # Vérifier si la déduplication a produit des résultats
                    if self.stats['entries_deduped'] == 0:
                        self.console.print("[bold red]Erreur : La déduplication n'a produit aucun résultat. Processus automatique interrompu.[/bold red]")
                        continue
                    
                    # Étape 6: Export des mots de passe (peut continuer même en cas d'échec)
                    self.console.print("[bold yellow]Étape 6/7 : Export des mots de passe...[/bold yellow]")
                    await self.run_export_passwords()  # Exporter passwords (auto)
                    
                    # Étape 7: Division du fichier dédupliqué (peut échouer si pas de fichier dédupliqué)
                    self.console.print("[bold yellow]Étape 7/7 : Division du fichier dédupliqué...[/bold yellow]")
                    if not self.deduped_path or not self.deduped_path.exists():
                        self.console.print("[bold red]Erreur : Aucun fichier dédupliqué trouvé. Processus automatique interrompu.[/bold red]")
                        continue
                    await self.run_split_deduped(auto_mode=True)
                    
                    # Afficher les statistiques à la fin du processus
                    self.console.print("[bold green]✓ Processus automatique complet terminé avec succès ![/bold green]")
                    self.show_stats()
                except Exception as e:
                    self.console.print(f"[bold red]Erreur inattendue lors du processus automatique : {str(e)}[/bold red]")
                    self.console.print("[bold red]Le processus automatique a été interrompu en raison d'une erreur.[/bold red]")
            elif choice == "5":
                await self.run_download_sources()  # Télécharger tout (manuel)
            elif choice == "7":
                await self.run_split_deduped()  # Split/dedup (manuel)
            elif choice == "6":
                await self.run_export_all()  # Exporter tout (manuel)
            elif choice == "8":
                await self.run_export_nicknames()  # Exporter nicknames (manuel)
            elif choice == "9":
                await self.run_export_emails()  # Exporter emails (manuel)
            elif choice == "10":
                await self.run_export_passwords()  # Exporter passwords (manuel)
            elif choice == "11":
                await self.run_clear_project_strict()  # Nettoyer entièrement le projet (manuel)
                # Recharger la configuration après le nettoyage manuel aussi
                await self.reload_config()
            elif choice == "12":
                self.show_stats()  # Afficher les statistiques
            else:
                # Afficher un message d'erreur et continuer la boucle
                self.console.print(f"[bold red]{get_translation('invalid_choice', self.lang)}[/bold red]")
                # Ne rien faire d'autre - la boucle continuera et réaffichera le menu
                

                

    
    def show_stats(self) -> None:
        """
        Affiche les statistiques actuelles de l'orchestrateur.
        
        Requires:
            - self.console: Instance de rich.console.Console (fournie par OrchestratorBase)
            - self.stats: Dictionnaire des statistiques (fourni par OrchestratorBase)
            - self.lang: Code de langue (fourni par interactive_runner)
        """
        # Vérification que les attributs requis sont disponibles
        if not hasattr(self, 'console'):
            from rich.console import Console
            self.console = Console()
            print("[WARNING] L'attribut 'console' n'était pas disponible et a été créé dynamiquement.")
            
        if not hasattr(self, 'stats'):
            self.stats = {
                'sources_downloaded': 0,
                'entries_raw': 0,
                'entries_normalized': 0,
                'entries_deduped': 0
            }
            print("[WARNING] L'attribut 'stats' n'était pas disponible et a été créé dynamiquement.")
        table = Table(title=get_translation("stats_title", self.lang))
        
        table.add_column(get_translation("stats_column_1", self.lang), style="magenta")  # Couleur différente pour les statistiques
        table.add_column(get_translation("stats_column_2", self.lang), style="magenta")
        
        table.add_row(get_translation("stats_row_1", self.lang), str(self.stats['sources_downloaded']))
        table.add_row(get_translation("stats_row_2", self.lang), str(self.stats['entries_raw']))
        table.add_row(get_translation("stats_row_3", self.lang), str(self.stats['entries_normalized']))
        table.add_row(get_translation("stats_row_4", self.lang), str(self.stats['entries_deduped']))
        
        self.console.print(table)
        
    def show_errors(self) -> None:
        """
        Affiche les erreurs collectées pendant l'exécution.
        
        Requires:
            - self.console: Instance de rich.console.Console (fournie par OrchestratorBase)
            - self.errors_log: Liste des erreurs (fournie par OrchestratorBase)
            - self.lang: Code de langue (fourni par interactive_runner)
        """
        # Vérification que les attributs requis sont disponibles
        if not hasattr(self, 'console'):
            from rich.console import Console
            self.console = Console()
            print("[WARNING] L'attribut 'console' n'était pas disponible et a été créé dynamiquement.")
            
        if not hasattr(self, 'errors_log'):
            self.errors_log = []
            print("[WARNING] L'attribut 'errors_log' n'était pas disponible et a été créé dynamiquement.")
        
        # Si aucune erreur n'a été enregistrée, afficher un message simple
        if not self.errors_log:
            self.console.print(f"[bold green]{get_translation('no_errors', self.lang)}[/bold green]")
            return
        
        # Créer un tableau pour afficher les erreurs
        table = Table(title=get_translation("errors_title", self.lang))
        
        # Ajouter les colonnes
        table.add_column(get_translation("errors_column_time", self.lang), style="cyan")
        table.add_column(get_translation("errors_column_phase", self.lang), style="blue")
        table.add_column(get_translation("errors_column_type", self.lang), style="yellow")
        table.add_column(get_translation("errors_column_message", self.lang), style="red")
        
        # Ajouter les lignes d'erreurs
        for error in self.errors_log:
            table.add_row(
                error.get('time', '-'),
                error.get('phase', '-'),
                error.get('type', '-'),
                error.get('message', '-')
            )
        
        self.console.print(table)

# Ce module ne contient plus que la classe InteractiveMixin : la fonction run_interactive a été déplacée dans interactive_runner.py pour casser le circular import.
