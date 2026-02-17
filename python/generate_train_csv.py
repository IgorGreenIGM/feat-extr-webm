import pandas as pd
import numpy as np

# 1. CHARGEMENT DES DONNÉES
df_features = pd.read_csv("cmr-arronds-final-features-aggregated.csv")
df_agri = pd.read_csv("opendataforafrica-dataset/opendata-for-africa-agriculture.csv")
df_elev = pd.read_csv("opendataforafrica-dataset/opendata-for-africa-elevage.csv")
df_pech = pd.read_csv("opendataforafrica-dataset/opendata-for-africa-peche.csv")
import pandas as pd
import numpy as np

# 2. NETTOYAGE ET HARMONISATION DES NOMS DE RÉGIONS
# On aligne tout sur le format GEE : "Far-North", "North-West", etc.
region_map = {
    'ADAMAWA': 'Adamawa', 'ADAMOUA': 'Adamawa',
    'CENTRE': 'Centre',
    'EAST': 'East', 'EST': 'East',
    'FAR NORTH': 'Far-North', 'EXTREME-NORD': 'Far-North', 'Far North': 'Far-North',
    'LITTORAL': 'Littoral',
    'NORTH': 'North', 'NORD': 'North',
    'NORTH WEST': 'North-West', 'NORD-OUEST': 'North-West', 'North West': 'North-West',
    'WEST': 'West', 'OUEST': 'West',
    'SOUTH': 'South', 'SUD': 'South',
    'SOUTH WEST': 'South-West', 'SUD-OUEST': 'South-West', 'South West': 'South-West'
}

for df in [df_agri, df_elev, df_pech]:
    df['region'] = df['region'].str.upper().map(lambda x: region_map.get(x, x))
    df['Date'] = df['Date'].astype(int)

# Fusion des 3 fichiers de stats
df_stats = pd.concat([df_agri, df_elev, df_pech], ignore_index=True)
# On retire les lignes "CAMEROON" (total national) pour l'entraînement
df_stats = df_stats[df_stats['region'] != 'CAMEROON']

# 3. AGGRÉGATION DES FEATURES (ARRONDISSEMENT -> RÉGION)
print("🧮 Agrégation spatiale des features (formules mathématiques exactes)...")

def aggregate_regionally(group):
    res = {}
    # Liste des colonnes de base (moyennes et stdDev de GEE)
    # On identifie les colonnes par leur suffixe
    mean_cols = [c for c in group.columns if c.endswith('_mean')]
    std_cols = [c for c in group.columns if c.endswith('_stdDev')]
    sum_cols = [c for c in group.columns if c.endswith('_sum')]
    static_cols = ['center_lat', 'center_lon', 'is_coastal_zone', 'road_density', 
                   'dist_to_main_road_km', 'dist_to_coast_km', 'river_density', 
                   'flood_plain_pct', 'dist_to_port_km', 'market_accessibility_km',
                   'dist_to_permanent_water_km', 'shrubland_pct']

    # A. Moyenne simple pour les colonnes de moyennes et statiques
    for col in mean_cols + static_cols:
        if col in group.columns:
            res[col] = group[col].mean()

    # B. Somme pour les colonnes de population/pluie sum
    for col in sum_cols:
        if col in group.columns:
            res[col] = group[col].sum()

    # C. FORMULE EXACTE POUR LES ÉCARTS-TYPES RÉGIONAUX
    # On itère sur les racines (ex: 'elevation') pour recalculer le stdDev régional
    roots = [c.replace('_mean', '') for c in mean_cols]
    for root in roots:
        m_col = root + '_mean'
        s_col = root + '_stdDev'
        if m_col in group.columns and s_col in group.columns:
            mu_reg = res[m_col]
            # Sigma_reg = sqrt( mean(sigma_i^2 + mu_i^2) - mu_reg^2 )
            combined_var = (group[s_col]**2 + group[m_col]**2).mean() - mu_reg**2
            res[s_col] = np.sqrt(max(0, combined_var)) # max(0) pour éviter les erreurs flottantes

    return pd.Series(res)

# On agrège par région et par année
df_region_features = df_features.groupby(['adm1_name', 'year']).apply(aggregate_regionally).reset_index()

# 4. CRÉATION DU DATASET FINAL (JOIN STATS + FEATURES)
print("🔗 Fusion avec les statistiques de production...")

# On prépare le dataset d'entraînement
train_data = []

for index, row in df_stats.iterrows():
    # On cherche les features correspondant à la région et l'année de la statistique
    feat = df_region_features[(df_region_features['adm1_name'] == row['region']) & 
                               (df_region_features['year'] == row['Date'])]
    
    if not feat.empty:
        combined = feat.iloc[0].to_dict()
        combined['target_species'] = row['indicateur']
        combined['target_value'] = row['Value']
        train_data.append(combined)

df_final_train = pd.DataFrame(train_data)

# 5. ENCODAGE ONE-HOT DES ESPÈCES
# Le modèle a besoin de savoir de quelle espèce on parle
df_final_train = pd.get_dummies(df_final_train, columns=['target_species'], prefix='is')

# 6. EXPORT
df_final_train.to_csv("training_dataset_final.csv", index=False)
print(f"✅ Terminé ! Dataset d'entraînement généré : {df_final_train.shape[0]} lignes.")