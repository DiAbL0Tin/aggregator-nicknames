# Conftest pour ajouter le répertoire racine au PYTHONPATH
import sys
from pathlib import Path

# Ajouter le répertoire racine au début du chemin pour permettre l'import du package aggregator
sys.path.insert(0, str(Path(__file__).parent.parent))

