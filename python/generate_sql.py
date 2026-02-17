import pandas as pd

# 1. MAPPING DES SOUS-SECTEURS (Traduction + Couleur)
# Clé : Nom dans le CSV IA -> Valeur : [Nom en Français, Couleur]
mapping_agri = {
    "Bambara groundnut production's": ["Voandzou", "#D4AC0D"],
    "Banana production's": ["Banane", "#F1C40F"],
    "Bean production's": ["Haricot", "#3E2723"],
    "Cassava production's": ["Manioc", "#2ECC71"],
    "Cocoa production": ["Cacao", "#8B4513"],
    "Colocasia production's": ["Macabo", "#C0CA33"],
    "Cowpea production's": ["Niébé", "#E59866"],
    "Cuncumber production's": ["Concombre", "#27AE60"],
    "Ginger production's": ["Gingembre", "#BDB76B"],
    "Groundnut production's": ["Arachide", "#D4AC0D"],
    "Irish potato production's": ["Pomme de terre", "#D35400"],
    "Maize production's": ["Maïs", "#F1C40F"],
    "Okrah production's": ["Gombo", "#8BC34A"],
    "Onion production's": ["Oignon", "#8E44AD"],
    "Palm oil production's": ["Palmier à huile", "#C0392B"],
    "Pepper production's": ["Poivre", "#212121"],
    "Pineaple production's": ["Ananas", "#F4D03F"],
    "Plantain production's": ["Plantain", "#AFB42B"],
    "Rice production's": ["Riz", "#BDC3C7"],
    "Robusta coffee production's": ["Café", "#6F4E37"],
    "Arabica coffe production's": ["Café", "#6F4E37"],
    "Sesame production's": ["Sésame", "#FFF9C4"],
    "Sorghum production's": ["Sorgho", "#E59866"],
    "Soya production's": ["Soja", "#E6EE9C"],
    "Tomato production's": ["Tomate", "#F44336"],
    "Watermelon production's": ["Pastèque", "#81C784"],
    "Yam production's": ["Igname", "#D7CCC8"]
}

mapping_elevage = {
    "beaf meat production": ["Bovins", "#C0392B"],
    "Goat meat production": ["Petits Ruminants", "#A04000"],
    "Sheep meat production": ["Petits Ruminants", "#A04000"],
    "Pig meat production": ["Porcins", "#F1948A"],
    "Poultry meat production": ["Volailles", "#E67E22"],
    "Milk production": ["Lait", "#E0E0E0"],
    "Eggs production": ["Oeufs", "#FFFFFF"],
    "Honey production": ["Miel", "#FFD54F"]
}

mapping_peche = {
    "Aquaculture production": ["Aquaculture", "#3498DB"],
    "Industrial fishing production": ["Pêche Maritime", "#2980B9"],
    "Inland fishing production": ["Pêche Continentale", "#3498DB"],
    "Smal scale fishing production": ["Pêche Maritime", "#2980B9"]
}

# 2. CHARGEMENT DES DONNÉES
df = pd.read_csv("cmr-infered-datas.csv")
df = df[df['volume'] > 0]

output_file = "rebuild_db_with_ai_data.sql"

with open(output_file, "w", encoding="utf-8") as f:
    # A. PRÉAMBULE ET NETTOYAGE
    f.write("-- RECONSTRUCTION TOTALE DE LA BASE DE DONNÉES BRAYANNE\n")
    f.write("SET statement_timeout = 0; SET client_encoding = 'UTF8';\n")
    f.write("DROP TABLE IF EXISTS public.production_stats CASCADE;\n")
    f.write("DROP TABLE IF EXISTS public.sub_sectors CASCADE;\n")
    f.write("DROP TABLE IF EXISTS public.sectors CASCADE;\n\n")

    # B. CRÉATION DES TABLES (SCHEMA EXACT)
    f.write("""CREATE TABLE public.sectors (
    id integer NOT NULL PRIMARY KEY,
    name character varying(100) NOT NULL
);

CREATE TABLE public.sub_sectors (
    id integer NOT NULL PRIMARY KEY,
    sector_id integer REFERENCES public.sectors(id) ON DELETE CASCADE,
    name character varying(100) NOT NULL,
    color character varying(20)
);

CREATE TABLE public.production_stats (
    id SERIAL PRIMARY KEY,
    sub_sector_id integer REFERENCES public.sub_sectors(id) ON DELETE CASCADE,
    zone_code character varying(50),
    volume numeric(15,2),
    unit character varying(20),
    year integer DEFAULT 2023,
    surface_area numeric(15,2) DEFAULT 0,
    yield numeric(10,2) DEFAULT 0,
    producer_count integer DEFAULT 0,
    average_price numeric(15,2) DEFAULT 0,
    description text
);\n\n""")

    # C. INSERTION DES RÉFÉRENTIELS
    f.write("INSERT INTO public.sectors (id, name) VALUES (1, 'Agriculture'), (2, 'Elevage'), (3, 'Peche');\n\n")

    # On prépare le mapping ID pour les stats
    sub_id_counter = 1
    indicator_to_id = {}

    print("🏗️ Création des sous-secteurs...")
    for category, mapping in [("Agriculture", mapping_agri), ("Elevage", mapping_elevage), ("Peche", mapping_peche)]:
        sec_id = 1 if category == "Agriculture" else (2 if category == "Elevage" else 3)
        for eng_name, details in mapping.items():
            fr_name, color = details[0], details[1]
            # On évite de créer plusieurs fois le même sous-secteur (ex: Pêche Maritime)
            if eng_name not in indicator_to_id:
                f.write(f"INSERT INTO public.sub_sectors (id, sector_id, name, color) VALUES ({sub_id_counter}, {sec_id}, '{fr_name}', '{color}') ON CONFLICT DO NOTHING;\n")
                indicator_to_id[eng_name] = sub_id_counter
                sub_id_counter += 1

    # D. INSERTION DES STATS (81 000+ LIGNES)
    print("📥 Génération des lignes de production...")
    values = []
    for _, row in df.iterrows():
        sid = indicator_to_id.get(row['indicator'])
        if sid:
            # Format: (sub_sector_id, zone_code, volume, unit, year, description)
            val = f"({sid}, '{row['adm3_pcode']}', {row['volume']}, 'tonne', {row['year']}, 'Inferred via ML + Dasymetric Mapping')"
            values.append(val)
            
            if len(values) >= 1000:
                f.write("INSERT INTO public.production_stats (sub_sector_id, zone_code, volume, unit, year, description) VALUES\n")
                f.write(",\n".join(values) + ";\n\n")
                values = []

    if values:
        f.write("INSERT INTO public.production_stats (sub_sector_id, zone_code, volume, unit, year, description) VALUES\n")
        f.write(",\n".join(values) + ";\n")

    # E. RÉTABLISSEMENT DES INDEX POUR LA PERFORMANCE
    f.write("\nCREATE INDEX idx_prod_year ON public.production_stats (year);\n")
    f.write("CREATE INDEX idx_prod_zone ON public.production_stats (zone_code);\n")
    f.write("CREATE INDEX idx_prod_subsector ON public.production_stats (sub_sector_id);\n")

print(f"✅ Fichier SQL généré : {output_file}")