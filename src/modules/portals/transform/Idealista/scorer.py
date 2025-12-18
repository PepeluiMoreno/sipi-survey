"""
Scorer específico para Idealista
Hereda del scorer base
"""
from portals.transform import ReligiousPropertyScorer


class IdealistaScorer(ReligiousPropertyScorer):
    """
    Scorer para Idealista
    Por ahora usa la implementación base sin modificaciones
    Se puede extender con lógica específica si es necesario
    """
    
    def __init__(self):
        super().__init__()