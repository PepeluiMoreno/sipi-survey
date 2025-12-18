CREATE SCHEMA IF NOT EXISTS osm_staging;
CREATE TABLE osm_staging.raw_churches (
    osm_id         TEXT PRIMARY KEY,
    name           TEXT,
    all_names      TEXT[],          -- nombres alternativos
    building_type  TEXT,
    denomination   TEXT,
    lat            FLOAT,
    lon            FLOAT,
    wikidata       TEXT,
    source         TEXT,
    raw_tags       JSONB,
    load_ts        TIMESTAMP DEFAULT NOW()
);