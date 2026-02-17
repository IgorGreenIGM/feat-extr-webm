import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon

# 1. CONFIGURATION DES CHEMINS
files = {
    "country": "cmr_admin_boundaries.shp/cmr_admin0.shp",
    "regions": "cmr_admin_boundaries.shp/cmr_admin1.shp",
    "depts": "cmr_admin_boundaries.shp/cmr_admin2.shp",
    "arronds": "cmr_admin_boundaries.shp/cmr_admin3.shp"
}

output_sql = "rebuild_geo_structure.sql"

def to_multi_polygon_wkt(geom):
    """Force la géométrie en MultiPolygon pour PostGIS"""
    if isinstance(geom, Polygon):
        return MultiPolygon([geom]).wkt
    return geom.wkt

def escape_sql(text):
    """Protège les apostrophes pour éviter les erreurs de syntaxe SQL"""
    if pd.isna(text):
        return ""
    return str(text).replace("'", "''")

# Chargement des GeoDataFrames
print("📂 Chargement des Shapefiles...")
gdf0 = gpd.read_file(files["country"])
gdf1 = gpd.read_file(files["regions"])
gdf2 = gpd.read_file(files["depts"])
gdf3 = gpd.read_file(files["arronds"])

# Initialisation des mappings pour les parent_id
pcode_to_id = {}
current_id = 1

with open(output_sql, "w", encoding="utf-8") as f:
    f.write("-- RECONSTRUCTION ADMINISTRATIVE ET GÉOGRAPHIQUE\n")
    f.write("BEGIN;\n\n")
    f.write("TRUNCATE public.administrative_zones RESTART IDENTITY CASCADE;\n")
    f.write("TRUNCATE public.temp_departements CASCADE;\n")
    f.write("TRUNCATE public.temp_arrondissements CASCADE;\n\n")

    # --- ÉTAPE 0 : COUNTRY ---
    print("📍 Traitement du Pays...")
    row = gdf0.iloc[0]
    wkt = to_multi_polygon_wkt(row.geometry)
    # Le nom peut être dans adm0_name ou adm0_name1 selon le fichier
    nom_pays = row.get('adm0_name1', row.get('adm0_name', 'Cameroun'))
    nom_pays_esc = escape_sql(nom_pays)
    
    f.write(f"INSERT INTO public.administrative_zones (id, name, level, parent_id, code, geometry) VALUES \n")
    f.write(f"({current_id}, '{nom_pays_esc}', 'COUNTRY', NULL, '{row['adm0_pcode']}', ST_Multi(ST_GeomFromText('{wkt}', 4326)));\n\n")
    pcode_to_id[row['adm0_pcode']] = current_id
    current_id += 1

    # --- ÉTAPE 1 : REGIONS ---
    print("📍 Traitement des Régions...")
    for _, row in gdf1.iterrows():
        wkt = to_multi_polygon_wkt(row.geometry)
        parent_id = pcode_to_id[row['adm0_pcode']]
        name = escape_sql(row['adm1_name1'])
        
        f.write(f"INSERT INTO public.administrative_zones (id, name, level, parent_id, code, geometry) VALUES \n")
        f.write(f"({current_id}, '{name}', 'REGION', '{parent_id}', '{row['adm1_pcode']}', ST_Multi(ST_GeomFromText('{wkt}', 4326)));\n")
        pcode_to_id[row['adm1_pcode']] = current_id
        current_id += 1
    f.write("\n")

    # --- ÉTAPE 2 : DEPARTEMENTS ---
    print("📍 Traitement des Départements...")
    for _, row in gdf2.iterrows():
        wkt = to_multi_polygon_wkt(row.geometry)
        parent_id = pcode_to_id[row['adm1_pcode']]
        name = escape_sql(row['adm2_name1'])
        p_name = escape_sql(row['adm1_name1'])
        
        f.write(f"INSERT INTO public.administrative_zones (id, name, level, parent_id, code, geometry) VALUES \n")
        f.write(f"({current_id}, '{name}', 'DEPARTEMENT', '{parent_id}', '{row['adm2_pcode']}', ST_Multi(ST_GeomFromText('{wkt}', 4326)));\n")
        
        # Remplissage temp_departements
        f.write(f"INSERT INTO public.temp_departements (name, p_name, level, geometry) VALUES \n")
        f.write(f"('{name}', '{p_name}', 'DEPARTEMENT', ST_Multi(ST_GeomFromText('{wkt}', 4326)));\n")
        
        pcode_to_id[row['adm2_pcode']] = current_id
        current_id += 1
    f.write("\n")

    # --- ÉTAPE 3 : ARRONDISSEMENTS ---
    print("📍 Traitement des Arrondissements...")
    for _, row in gdf3.iterrows():
        wkt = to_multi_polygon_wkt(row.geometry)
        parent_id = pcode_to_id[row['adm2_pcode']]
        name = escape_sql(row['adm3_name1'])
        p_name = escape_sql(row['adm2_name1'])
        
        f.write(f"INSERT INTO public.administrative_zones (id, name, level, parent_id, code, geometry) VALUES \n")
        f.write(f"({current_id}, '{name}', 'ARRONDISSEMENT', '{parent_id}', '{row['adm3_pcode']}', ST_Multi(ST_GeomFromText('{wkt}', 4326)));\n")
        
        # Remplissage temp_arrondissements
        f.write(f"INSERT INTO public.temp_arrondissements (name, p_name, level, geometry) VALUES \n")
        f.write(f"('{name}', '{p_name}', 'ARRONDISSEMENT', ST_Multi(ST_GeomFromText('{wkt}', 4326)));\n")
        
        current_id += 1

    f.write("\nCOMMIT;\n")

print(f"✅ Terminé ! Fichier {output_sql} généré.")