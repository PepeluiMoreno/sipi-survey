"""
Sistema de scoring BASE para detección de patrimonio religioso
COMÚN para todos los portales
"""
from typing import Dict, List, Tuple, Any
import logging

from shared.osm import OverpassClient
from portals.config import POSITIVE, NEGATIVE, EXPLICIT
from portals.config import WEIGHTS, PROXIMITY, SURFACE

logger = logging.getLogger(__name__)


class ReligiousPropertyScorer:
    """
    Scorer base que evalúa inmuebles y asigna puntuación basada en:
    1. Keywords en título/descripción
    2. Proximidad a edificios religiosos (Overpass API)
    3. Características físicas (superficie, tipo)
    """
    
    def __init__(self):
        # Keywords
        self.keywords_high = EXPLICIT
        self.keywords_positive = POSITIVE
        self.keywords_negative = NEGATIVE
        
        # Configuración
        self.weights = WEIGHTS
        self.proximity_config = PROXIMITY
        self.surface_config = SURFACE
        
        # Cliente Overpass
        self.overpass_client = OverpassClient()
    
    def score(self, inmueble: Dict[str, Any]) -> Tuple[float, List[str]]:
        """
        Calcula score de un inmueble
        
        Args:
            inmueble: Dict con:
                - titulo: str
                - descripcion: str
                - tipo: str
                - superficie: float
                - lat: float (opcional)
                - lon: float (opcional)
                - caracteristicas_basicas: List[str]
                - caracteristicas_extras: List[str]
            
        Returns:
            Tuple (score, evidences)
        """
        score = 0.0
        evidences = []
        
        # Preparar texto completo
        titulo = (inmueble.get('titulo') or '').lower()
        descripcion = (inmueble.get('descripcion') or '').lower()
        caracteristicas = ' '.join(
            inmueble.get('caracteristicas_basicas', []) +
            inmueble.get('caracteristicas_extras', [])
        ).lower()
        
        texto_completo = f"{titulo} {descripcion} {caracteristicas}"
        
        # ================================================================
        # 1. KEYWORDS EXPLÍCITAS (100% → retorno inmediato)
        # ================================================================
        for keyword in self.keywords_high:
            if keyword.lower() in titulo:
                score = 100.0
                evidences.append(f"🔴 Keyword explícita: '{keyword}' → 100%")
                logger.info(f"✓ Score 100% por keyword explícita: {keyword}")
                return score, evidences
        
        # ================================================================
        # 2. KEYWORDS POSITIVAS
        # ================================================================
        peso_por_keyword = self.weights['keywords'] / len(self.keywords_positive)
        
        found_positive = []
        for keyword in self.keywords_positive:
            if keyword.lower() in texto_completo:
                score += peso_por_keyword
                found_positive.append(keyword)
        
        if found_positive:
            evidences.append(
                f"✓ Keywords positivas ({len(found_positive)}): "
                f"{', '.join(found_positive[:3])} → +{len(found_positive) * peso_por_keyword:.1f}"
            )
        
        # ================================================================
        # 3. KEYWORDS NEGATIVAS
        # ================================================================
        peso_negativo = self.weights['keywords'] / len(self.keywords_negative)
        
        found_negative = []
        for keyword in self.keywords_negative:
            if keyword.lower() in texto_completo:
                score -= peso_negativo
                found_negative.append(keyword)
        
        if found_negative:
            evidences.append(
                f"⚠ Keywords negativas ({len(found_negative)}): "
                f"{', '.join(found_negative[:3])} → -{len(found_negative) * peso_negativo:.1f}"
            )
        
        # ================================================================
        # 4. PROXIMIDAD A EDIFICIOS RELIGIOSOS
        # ================================================================
        if self.proximity_config['enabled']:
            lat = inmueble.get('lat')
            lon = inmueble.get('lon')
            
            if lat is not None and lon is not None:
                proximity_score, proximity_evidences = self._score_proximity(lat, lon)
                score += proximity_score
                evidences.extend(proximity_evidences)
            else:
                evidences.append("○ Sin coordenadas → sin scoring de proximidad")
        
        # ================================================================
        # 5. SUPERFICIE GRANDE
        # ================================================================
        if self.surface_config['enabled']:
            superficie = inmueble.get('superficie')
            
            if superficie and superficie >= self.surface_config['min_size_m2']:
                puntos = self.surface_config['max_score']
                score += puntos
                evidences.append(f"✓ Superficie grande ({superficie}m²) → +{puntos}")
            
            # Bonificaciones arquitectónicas
            if any(term in caracteristicas for term in ['techos altos', 'doble altura']):
                bonus = self.surface_config['bonus']['high_ceilings']
                score += bonus
                evidences.append(f"✓ Techos altos → +{bonus}")
            
            if any(term in caracteristicas for term in ['varias plantas', 'múltiples niveles', 'dos plantas']):
                bonus = self.surface_config['bonus']['multiple_floors']
                score += bonus
                evidences.append(f"✓ Múltiples plantas → +{bonus}")
        
        # ================================================================
        # 6. TIPO DE INMUEBLE
        # ================================================================
        tipo = (inmueble.get('tipo') or '').lower()
        if 'edificio' in tipo or 'singular' in tipo:
            bonus = 10
            score += bonus
            evidences.append(f"✓ Tipo relevante: {tipo} → +{bonus}")
        
        # Limitar a [0, 100]
        score = max(0.0, min(score, 100.0))
        
        return score, evidences
    
    def _score_proximity(self, lat: float, lon: float) -> Tuple[float, List[str]]:
        """Calcula score de proximidad a edificios religiosos"""
        evidences = []
        
        try:
            radius_m = self.proximity_config['radius_meters']
            
            logger.debug(f"Buscando edificios religiosos en {radius_m}m de ({lat:.4f}, {lon:.4f})")
            
            churches = self.overpass_client.find_churches_nearby(
                lat=lat,
                lon=lon,
                radius_m=radius_m
            )
            
            if not churches:
                evidences.append(f"○ No hay edificios religiosos en {radius_m}m")
                return 0.0, evidences
            
            # Edificio más cercano
            closest = churches[0]
            distance = closest.distance
            
            # Traducir tipo
            type_names = {
                'church': 'iglesia', 'cathedral': 'catedral', 'chapel': 'capilla',
                'monastery': 'monasterio', 'convent': 'convento', 'hermitage': 'ermita',
                'basilica': 'basílica', 'cross': 'cruz', 'wayside_shrine': 'humilladero',
                'lourdes_grotto': 'gruta', 'place_of_worship': 'lugar de culto',
                'unknown': 'edificio religioso'
            }
            
            type_name = type_names.get(closest.building_type, closest.building_type)
            
            # Score según distancia
            distance_scores = self.proximity_config['distance_scores']
            
            if distance <= 50:
                proximity_score = distance_scores['0-50']
                evidences.append(
                    f"🟢 {type_name.capitalize()} a {distance:.0f}m: "
                    f"'{closest.name}' → +{proximity_score}"
                )
            elif distance <= 150:
                proximity_score = distance_scores['50-150']
                evidences.append(
                    f"🟡 {type_name.capitalize()} a {distance:.0f}m: "
                    f"'{closest.name}' → +{proximity_score}"
                )
            elif distance <= 300:
                proximity_score = distance_scores['150-300']
                evidences.append(
                    f"🟠 {type_name.capitalize()} a {distance:.0f}m: "
                    f"'{closest.name}' → +{proximity_score}"
                )
            elif distance <= 500:
                proximity_score = distance_scores['300-500']
                evidences.append(
                    f"🔴 {type_name.capitalize()} a {distance:.0f}m: "
                    f"'{closest.name}' → +{proximity_score}"
                )
            else:
                proximity_score = 0.0
                evidences.append(
                    f"○ {type_name.capitalize()} más cercana a {distance:.0f}m (>500m) → +0"
                )
            
            # Resumen si hay más de uno
            if len(churches) > 1:
                types_count = {}
                for church in churches:
                    t = type_names.get(church.building_type, church.building_type)
                    types_count[t] = types_count.get(t, 0) + 1
                
                types_summary = ', '.join(
                    f"{count} {t}{'s' if count > 1 else ''}"
                    for t, count in sorted(types_count.items(), key=lambda x: -x[1])
                )
                evidences.append(f"📍 Total en {radius_m}m: {len(churches)} edificios ({types_summary})")
            
            return proximity_score, evidences
        
        except Exception as e:
            logger.error(f"Error en proximity scoring: {e}")
            evidences.append(f"⚠ Error buscando edificios religiosos: {e}")
            return 0.0, evidences