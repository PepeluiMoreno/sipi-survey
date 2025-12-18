# Pesos y umbrales de scoring
WEIGHTS = {"keywords": 70, "proximity": 20, "surface": 10}

PROXIMITY = {
    "enabled": True,
    "radius_meters": 200,
    "max_score": 20,
    "distance_scores": {"0-50": 20, "50-150": 15, "150-300": 10, "300-500": 5},
}

SURFACE = {
    "enabled": True,
    "min_size_m2": 300,
    "max_score": 10,
    "bonus": {"high_ceilings": 3, "multiple_floors": 3},
}

[osm]
overpass_url = "https://overpass-api.de/api/interpreter"
timeout = 10
default_search_radius_m = 200