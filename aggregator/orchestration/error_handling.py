"""
Module contenant les décorateurs et utilitaires pour la gestion des erreurs
dans l'orchestrateur d'aggregator Nickname.
"""

import functools
import traceback
from datetime import datetime
from typing import Any, Callable, Dict, TypeVar, cast, Optional

# Type variables pour les annotations
F = TypeVar('F', bound=Callable[..., Any])

def log_errors(phase_name: str) -> Callable[[F], F]:
    """
    Décorateur qui capture les exceptions, les logue avec loguru et les ajoute
    au collecteur d'erreurs de l'orchestrateur.
    
    Args:
        phase_name: Nom de la phase pour l'identification dans les logs
        
    Returns:
        Décorateur configuré avec le nom de phase
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                return await func(self, *args, **kwargs)
            except Exception as e:
                # Récupérer la stack trace
                stack_trace = traceback.format_exc()
                
                # Créer une entrée d'erreur
                error_entry = {
                    'time': datetime.now().strftime('%H:%M:%S'),
                    'phase': phase_name,
                    'type': type(e).__name__,
                    'message': str(e),
                    'stack_trace': stack_trace
                }
                
                # Ajouter à la liste des erreurs de l'orchestrateur
                if hasattr(self, 'errors_log'):
                    self.errors_log.append(error_entry)
                
                # Logger avec loguru si disponible
                if hasattr(self, 'logger'):
                    self.logger.error(f"Erreur dans {phase_name}: {str(e)}")
                    self.logger.debug(f"Stack trace: {stack_trace}")
                
                # Propager l'exception pour la gestion au niveau supérieur
                raise
                
        return cast(F, wrapper)
    return decorator

def capture_errors(phase_name: str) -> Callable[[F], F]:
    """
    Décorateur qui capture les exceptions sans les propager, les logue avec loguru
    et les ajoute au collecteur d'erreurs de l'orchestrateur.
    Utile pour les opérations non-critiques où l'échec d'une étape ne doit pas
    arrêter le processus global.
    
    Args:
        phase_name: Nom de la phase pour l'identification dans les logs
        
    Returns:
        Décorateur configuré avec le nom de phase
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Optional[Any]:
            try:
                return await func(self, *args, **kwargs)
            except Exception as e:
                # Récupérer la stack trace
                stack_trace = traceback.format_exc()
                
                # Créer une entrée d'erreur
                error_entry = {
                    'time': datetime.now().strftime('%H:%M:%S'),
                    'phase': phase_name,
                    'type': type(e).__name__,
                    'message': str(e),
                    'stack_trace': stack_trace
                }
                
                # Ajouter à la liste des erreurs de l'orchestrateur
                if hasattr(self, 'errors_log'):
                    self.errors_log.append(error_entry)
                
                # Logger avec loguru si disponible
                if hasattr(self, 'logger'):
                    self.logger.error(f"Erreur non-critique dans {phase_name}: {str(e)}")
                    self.logger.debug(f"Stack trace: {stack_trace}")
                
                # Retourner None au lieu de propager l'exception
                return None
                
        return cast(F, wrapper)
    return decorator
