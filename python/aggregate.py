import pandas as pd

print("Chargement des données...")
df_gee = pd.read_csv("cmr-arronds-final-features-2010_2024.csv")  # Google Earth Engine output
df_osm = pd.read_csv("cmr-arronds-OpenStreetMap-Features.csv")    # OpenStreetMap output

df_gee['adm3_pcode'] = df_gee['adm3_pcode'].str.strip()
df_osm['adm3_pcode'] = df_osm['adm3_pcode'].str.strip()

print("Fusion des données sur le code ADM3...")
master_df = pd.merge(df_gee, df_osm, on='adm3_pcode', how='left')

missing = master_df.isnull().sum()
if missing.sum() > 0:
    print("missing datas")
    print(missing[missing > 0])

master_df.to_csv("cmr-arronds-final-features-aggregated.csv", index=False)

print(master_df.head())