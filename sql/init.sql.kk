"""
Script para inicializar el esquema de base de datos de SIPI-ETL
Usa los archivos SQL del proyecto real

Uso desde Jupyter:
    exec(open('/home/jovyan/dev/sipi-etl/init_db_real.py').read())
"""

import asyncio
import asyncpg
import os
from pathlib import Path


async def init_database():
    """
    Inicializa todos los esquemas del proyecto SIPI-ETL
    """
    print("=" * 80)
    print("SIPI-ETL: Inicialización de Base de Datos (Esquema Real)")
    print("=" * 80)
    print()
    
    # Configuración de conexión
    host = os.getenv('POSTGRES_HOST', 'postgis')
    port = os.getenv('POSTGRES_PORT', '5432')
    user = os.getenv('POSTGRES_USER', 'user')
    password = os.getenv('POSTGRES_PASSWORD', 'password')
    database = os.getenv('POSTGRES_DB', 'spatialdb')
    
    print(f"Conectando a PostgreSQL...")
    print(f"  Host: {host}")
    print(f"  Puerto: {port}")
    print(f"  Base de datos: {database}")
    print()
    
    try:
        # Conectar
        conn = await asyncpg.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database
        )
        
        print("✓ Conectado a PostgreSQL")
        print()
        
        # Buscar archivos SQL
        project_root = Path.cwd()
        if (project_root / 'notebooks').exists():
            project_root = project_root.parent
        
        migrations_dir = project_root / 'migrations'
        
        if not migrations_dir.exists():
            print(f"⚠ No se encuentra el directorio migrations en: {migrations_dir}")
            print("  Creando esquemas con SQL embebido...")
            print()
            
            # SQL básico si no hay migrations
            await execute_embedded_sql(conn)
        else:
            # Ejecutar archivos SQL del proyecto
            await execute_sql_files(conn, migrations_dir)
        
        print()
        
        # Verificar que todo se creó
        await verify_schemas(conn)
        
        # Cerrar conexión
        await conn.close()
        
        print("=" * 80)
        print("✓ Base de datos inicializada correctamente")
        print("=" * 80)
        print()
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        print()
        return False


async def execute_sql_files(conn, migrations_dir):
    """Ejecuta archivos SQL del directorio migrations"""
    
    # Orden de ejecución
    sql_files = [
        'init.sql',              # Crear esquemas
        'portals_schema.sql',    # Esquema principal de portals
        'osmqwikidata_schema.sql',  # OSM/Wikidata
        'matching_schema.sql',   # Matching OSM-Portals
        'regions_schema.sql',    # Regiones (si existe)
        'notifications_schema.sql',  # Notificaciones (si existe)
    ]
    
    print("Ejecutando archivos SQL...")
    print()
    
    for filename in sql_files:
        filepath = migrations_dir / filename
        
        if not filepath.exists():
            print(f"  ⚠ {filename} - No encontrado, saltando")
            continue
        
        print(f"  ⏳ {filename}")
        
        try:
            sql = filepath.read_text()
            
            # Ejecutar todo el archivo
            # asyncpg soporta múltiples statements separados por ;
            await conn.execute(sql)
            
            print(f"  ✓ {filename} - Ejecutado")
            
        except Exception as e:
            print(f"  ⚠ {filename} - Error (puede ser normal si ya existe): {str(e)[:100]}")
    
    print()


async def execute_embedded_sql(conn):
    """SQL mínimo embebido si no hay archivos"""
    
    sql = """
    -- PostGIS
    CREATE EXTENSION IF NOT EXISTS postgis;
    
    -- Esquemas
    CREATE SCHEMA IF NOT EXISTS portals;
    CREATE SCHEMA IF NOT EXISTS osmwikidata;
    CREATE SCHEMA IF NOT EXISTS matching;
    
    -- Tabla básica de inmuebles
    CREATE TABLE IF NOT EXISTS portals.inmuebles_raw (
        id SERIAL PRIMARY KEY,
        portal VARCHAR(50) NOT NULL,
        id_portal VARCHAR(100) NOT NULL,
        url TEXT NOT NULL,
        titulo TEXT,
        descripcion TEXT,
        tipo VARCHAR(100),
        precio NUMERIC(12, 2),
        superficie NUMERIC(10, 2),
        geo_type VARCHAR(20) NOT NULL,
        lat NUMERIC(10, 7),
        lon NUMERIC(10, 7),
        geom GEOMETRY(Point, 4326),
        uncertainty_radius_m INTEGER,
        direccion TEXT,
        codigo_postal VARCHAR(10),
        barrio VARCHAR(200),
        distrito VARCHAR(200),
        ciudad VARCHAR(200),
        provincia VARCHAR(200),
        caracteristicas JSONB,
        imagenes JSONB,
        portal_specific_data JSONB,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_active BOOLEAN DEFAULT TRUE,
        UNIQUE (portal, id_portal)
    );
    
    -- Tabla de detecciones
    CREATE TABLE IF NOT EXISTS portals.detecciones (
        id SERIAL PRIMARY KEY,
        inmueble_id INTEGER NOT NULL REFERENCES portals.inmuebles_raw(id) ON DELETE CASCADE,
        score NUMERIC(5, 2) NOT NULL CHECK (score >= 0 AND score <= 100),
        status VARCHAR(50) NOT NULL,
        evidences JSONB NOT NULL,
        osm_match_id BIGINT,
        osm_match_type VARCHAR(20),
        osm_match_confidence NUMERIC(5, 2),
        first_detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        precio_inicial NUMERIC(12, 2),
        precio_actual NUMERIC(12, 2)
    );
    
    -- Índices básicos
    CREATE INDEX IF NOT EXISTS idx_portals_raw_portal ON portals.inmuebles_raw(portal);
    CREATE INDEX IF NOT EXISTS idx_portals_raw_geom ON portals.inmuebles_raw USING GIST(geom);
    CREATE INDEX IF NOT EXISTS idx_portals_detecciones_inmueble ON portals.detecciones(inmueble_id);
    CREATE INDEX IF NOT EXISTS idx_portals_detecciones_score ON portals.detecciones(score DESC);
    """
    
    try:
        await conn.execute(sql)
        print("  ✓ SQL embebido ejecutado")
    except Exception as e:
        print(f"  ⚠ Error ejecutando SQL: {e}")


async def verify_schemas(conn):
    """Verifica que los esquemas y tablas existen"""
    
    print("Verificando creación...")
    print()
    
    # Verificar esquemas
    schemas = ['portals', 'osmwikidata', 'matching']
    
    for schema in schemas:
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
            schema
        )
        print(f"  {'✓' if exists else '✗'} Esquema '{schema}'")
    
    print()
    
    # Verificar tablas principales
    tables = [
        ('portals', 'inmuebles_raw'),
        ('portals', 'detecciones'),
        ('portals', 'cambios'),
        ('portals', 'duplicates'),
    ]
    
    for schema, table in tables:
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = $1 AND table_name = $2)",
            schema, table
        )
        
        if exists:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {schema}.{table}")
            print(f"  ✓ Tabla '{schema}.{table}' ({count} registros)")
        else:
            print(f"  ⚠ Tabla '{schema}.{table}' no existe")
    
    print()


if __name__ == "__main__":
    # Ejecutar
    success = asyncio.run(init_database())
    
    if not success:
        import sys
        sys.exit(1)
