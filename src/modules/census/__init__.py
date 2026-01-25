"""
Census Loader Module

Carga datos del censo de inmuebles inmatriculados desde archivos CSV.
"""

from .loader import CensusLoader
from .mapper import CensusMapper

__all__ = ["CensusLoader", "CensusMapper"]
