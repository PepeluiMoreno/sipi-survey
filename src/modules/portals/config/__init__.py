"""
Configuración común para todos los portales
"""
from .keywords import POSITIVE, NEGATIVE, EXPLICIT
from .scoring import WEIGHTS, PROXIMITY, SURFACE, DETECTION_THRESHOLD
from .matcher import MATCH_THRESHOLD, MAX_DISTANCE_METERS, MIN_CONFIDENCE
from .provinces import ANDALUCIA, CATALUNA, VALENCIA
from .typologies import COMPATIBLE_TYPES, MAX_TYPOLOGY_SCORE

__all__ = [
    # Keywords
    'POSITIVE',
    'NEGATIVE',
    'EXPLICIT',
    
    # Scoring
    'WEIGHTS',
    'PROXIMITY',
    'SURFACE',
    'DETECTION_THRESHOLD',
    
    # Matching
    'MATCH_THRESHOLD',
    'MAX_DISTANCE_METERS',
    'MIN_CONFIDENCE',
    
    # Provincias
    'ANDALUCIA',
    'CATALUNA',
    'VALENCIA',
    
    # Tipologías
    'COMPATIBLE_TYPES',
    'MAX_TYPOLOGY_SCORE'
]