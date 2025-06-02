"""
Module combiné pour l'orchestrateur d'aggregator Nickname.
Combine tous les mixins pour former la classe complète de l'orchestrateur.
"""

from .base import OrchestratorBase
from .cleaning import CleaningMixin
from .splitting import SplittingMixin
from .interactive import InteractiveMixin
from .utils import UtilsMixin


class CombinedOrchestrator(
    CleaningMixin,
    SplittingMixin,
    UtilsMixin,        # UtilsMixin déplacé avant InteractiveMixin pour corriger le MRO
    InteractiveMixin,
    OrchestratorBase  # Placer la classe de base en dernier pour éviter des problèmes d'ordre de résolution
):
    """
    Classe combinée de l'orchestrateur qui intègre toutes les fonctionnalités.
    Cette classe hérite de tous les mixins pour former une classe complète
    avec toutes les fonctionnalités nécessaires.
    """
    pass
