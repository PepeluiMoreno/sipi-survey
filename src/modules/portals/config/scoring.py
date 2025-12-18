"""
Configuración de scoring común para todos los portales
"""

# Pesos de los componentes del score
WEIGHTS = {
    "keywords": 70,      # Peso de keywords en título/descripción
    "proximity": 20,     # Peso de proximidad a iglesias OSM
    "surface": 10        # Peso de superficie/características
}

# Configuración de proximidad OSM
PROXIMITY = {
    "enabled": True,
    "radius_meters": 200,  # Radio de búsqueda de iglesias
    "max_score": 20,       # Puntuación máxima por proximidad
    
    # Puntos según distancia a iglesia más cercana
    "distance_scores": {
        "0-50": 20,      # Muy cerca (0-50m) → máxima puntuación
        "50-150": 15,    # Cerca (50-150m)
        "150-300": 10,   # Distancia media (150-300m)
        "300-500": 5     # Lejos pero en rango (300-500m)
    }
}

# Configuración de superficie
SURFACE = {
    "enabled": True,
    "min_size_m2": 300,  # Superficie mínima para considerar
    "max_score": 10,     # Puntuación máxima por superficie
    
    # Bonificaciones por características arquitectónicas
    "bonus": {
        "high_ceilings": 3,      # Techos altos/doble altura
        "multiple_floors": 3     # Varias plantas
    }
}

# Threshold de detección
DETECTION_THRESHOLD = 50.0  # Score mínimo para considerar candidato