"""
Point d'entrée pour l'orchestrateur interactif d'aggregator Nickname.
Ce fichier sert de point d'entrée et délègue les fonctionnalités aux modules spécialisés.
"""

# Import des modules d'orchestration
from aggregator.orchestration.base import OrchestratorBase
from aggregator.orchestration.cleaning import CleaningMixin
from aggregator.orchestration.splitting import SplittingMixin
from aggregator.orchestration.interactive import InteractiveMixin
from aggregator.orchestration.utils import UtilsMixin
from aggregator.orchestration.combined import CombinedOrchestrator


async def run_orchestrator(config_path: str = "config.yaml"):
    """
    Point d'entrée pour l'orchestrateur.
    
    Args:
        config_path: Chemin vers le fichier de configuration YAML
    """
    orchestrator = CombinedOrchestrator(config_path)
    await orchestrator.run_interactive()


# Classe d'orchestration complète
class Orchestrator(OrchestratorBase, CleaningMixin, SplittingMixin, InteractiveMixin, UtilsMixin):
    """
    Classe d'orchestration complète qui combine toutes les fonctionnalités.
    Cette classe est maintenue pour la compatibilité avec le code existant.
    Pour les nouveaux développements, utilisez plutôt CombinedOrchestrator.
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialise l'orchestrateur avec le chemin du fichier de configuration.
        
        Args:
            config_path: Chemin vers le fichier de configuration YAML
        """
        super().__init__(config_path)

