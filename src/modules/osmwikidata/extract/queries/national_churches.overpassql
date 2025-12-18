[out:json][timeout:300];
{{geocodeArea:España}}->.searchArea;
(
  // Edificios religiosos cristianos
  node["amenity"="place_of_worship"]["religion"="christian"](area.searchArea);
  way["amenity"="place_of_worship"]["religion"="christian"](area.searchArea);
  relation["amenity"="place_of_worship"]["religion"="christian"](area.searchArea);
  // building=church|cathedral|...
  node["building"~"^(church|cathedral|chapel|monastery|convent|hermitage|basilica)$"]["religion"="christian"](area.searchArea);
  way["building"~"^(church|cathedral|chapel|monastery|convent|hermitage|basilica)$"]["religion"="christian"](area.searchArea);
  relation["building"~"^(church|cathedral|chapel|monastery|convent|hermitage|basilica)$"]["religion"="christian"](area.searchArea);
);
out tags center qt;