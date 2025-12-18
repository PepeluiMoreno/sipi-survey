"""
Keywords comunes para detección de patrimonio religioso
Usadas por todos los portales
"""

# Keywords explícitas (100% de confianza)
EXPLICIT = [
    "iglesia",
    "convento",
    "capilla",
    "templo",
    "ermita",
    "basílica",
    "catedral",
    "monasterio",
    "parroquia",
    "santuario",
    "claustro",
    "abadía",
    "colegiata",
    "priorato",
    "cartuja"
]

# Keywords positivas (indicadores)
POSITIVE = [
    "reformado",
    "histórico",
    "patrimonio",
    "monumental",
    "singular",
    "protegido",
    "catalogado",
    "bic",  # Bien de Interés Cultural
    "señorial",
    "palacete"
]

# Keywords negativas (descartables)
NEGATIVE = [
    "sin ascensor",
    "a reformar",
    "sin amueblar",
    "obra nueva",
    "promoción",
    "vivienda protegida",
    "vpo"
]