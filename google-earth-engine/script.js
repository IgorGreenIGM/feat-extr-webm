/**
 * GENERATEUR DE FEATURES BASSINS DE PRODUCTION CAMEROUN
 * Granularité : Arrondissements
 * Période : 2021 - 2024
 * sources des shapefiles (régions et arrondissements) : https://data.humdata.org/dataset/cod-ab-cmr
 */

// 1. Charger l'asset Arrondissements
var arrondissements = ee.FeatureCollection("projects/cmr-arrond-features-getter/assets/arrondissements");

// 2. Paramètres temporels
var years = [2010, 2011, 2012, 2013, 2014, 2015, 
             2016, 2017, 2018, 2019, 2020, 2021, 
             2022, 2023, 2024];
var quarters = [[1, 3], [4, 6], [7, 9], [10, 12]];

// --- A. DONNÉES STATIQUES ---

// Topographie
var elevation = ee.Image("USGS/SRTMGL1_003").select(['elevation']).rename('elevation');
var slope = ee.Terrain.slope(elevation).rename('slope');

// Sols (ISRIC SoilGrids) - pH, Carbone et ARGILE (Clay)
var soil_ph = ee.Image("projects/soilgrids-isric/phh2o_mean").select(['phh2o_0-5cm_mean']).divide(10).rename('soil_ph');
var soil_carbon = ee.Image("projects/soilgrids-isric/soc_mean").select(['soc_0-5cm_mean']).rename('soil_carbon');
var soil_clay = ee.Image("projects/soilgrids-isric/clay_mean").select(['clay_0-5cm_mean']).rename('soil_clay');

// Population (WorldPop 2020)
var popDataset = ee.ImageCollection("WorldPop/GP/100m/pop")
  .filter(ee.Filter.eq('country', 'CMR'))
  .filter(ee.Filter.eq('year', 2020));

var pop = ee.Image(ee.Algorithms.If(
  popDataset.size().gt(0),
  popDataset.first().select(['population']).rename('pop_density'),
  ee.Image.constant(0).rename('pop_density')
));

// Occupation du sol (ESA WorldCover 2020)
var worldcover = ee.Image("ESA/WorldCover/v100/2020");
var cropland = worldcover.eq(40).rename('cropland_pct');
var grassland = worldcover.eq(30).rename('grassland_pct');
var trees = worldcover.eq(10).rename('tree_cover_pct');
var water = worldcover.eq(80).rename('water_surface_pct');

// Indice d'Aridité Global
// Note: Des valeurs faibles = zones très arides (désert), valeurs hautes = humide
var terraClimate = ee.ImageCollection("IDAHO_EPSCOR/TERRACLIMATE")
  .filter(ee.Filter.date('2021-01-01', '2021-12-31')) // On prend 2021 comme base
  .select(['pr', 'pet']);

var annualStats = terraClimate.reduce(ee.Reducer.sum());
// AI = Précipitations / PET
var aridityIndex = annualStats.select('pr_sum')
  .divide(annualStats.select('pet_sum'))
  .rename('aridity_index');

// --- B. FONCTION DE TRAITEMENT PAR ANNÉE ---

var processYear = function(year) {
  var start = ee.Date.fromYMD(year, 1, 1);
  var end = ee.Date.fromYMD(year, 12, 31);

  // Collections Dynamiques
  var ndviCol = ee.ImageCollection("MODIS/061/MOD13Q1").filterDate(start, end).select('NDVI');
  var rainCol = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY").filterDate(start, end).select('precipitation');
  
  // Température avec sécurité pour 2024
  var tempCol = ee.ImageCollection("ECMWF/ERA5_LAND/MONTHLY_AGGR").filterDate(start, end).select('temperature_2m_max');
  var temp = ee.Image(ee.Algorithms.If(
    tempCol.size().gt(0),
    tempCol.mean().subtract(273.15).rename('temp_max'),
    ee.Image.constant(25).rename('temp_max')
  ));

  var ndvi_std_temp = ndviCol.reduce(ee.Reducer.stdDev()).rename('ndvi_seasonal_std');

  // Assemblage de l'image (Inclus l'argile désormais)
  var yearlyImage = ee.Image([
    elevation, slope, soil_ph, soil_carbon, soil_clay, pop, 
    cropland, grassland, trees, water, temp, ndvi_std_temp, 
    aridityIndex
  ]);

  // Boucle trimestrielle
  for (var i = 0; i < 4; i++) {
    var qStart = ee.Date.fromYMD(year, quarters[i][0], 1);
    var qEnd = qStart.advance(3, 'month');
    
    var qNdviImg = ndviCol.filterDate(qStart, qEnd);
    var qNdvi = ee.Image(ee.Algorithms.If(qNdviImg.size().gt(0), qNdviImg.mean().divide(10000), ee.Image.constant(0))).rename('ndvi_q' + (i+1));
    
    var qRainImg = rainCol.filterDate(qStart, qEnd);
    var qRain = ee.Image(ee.Algorithms.If(qRainImg.size().gt(0), qRainImg.sum(), ee.Image.constant(0))).rename('precip_q' + (i+1));
    
    yearlyImage = yearlyImage.addBands([qNdvi, qRain]);
  }

  // Triple Reducer
  var combinedReducer = ee.Reducer.mean()
    .combine({reducer2: ee.Reducer.sum(), sharedInputs: true})
    .combine({reducer2: ee.Reducer.stdDev(), sharedInputs: true});

  return yearlyImage.reduceRegions({
    collection: arrondissements,
    reducer: combinedReducer,
    scale: 100
  }).map(function(f) {
    return f.set('year', year);
  });
};

// --- C. EXÉCUTION ET EXPORTATION ---

var finalCollection = ee.FeatureCollection(years.map(processYear)).flatten();

// LISTE DES COLONNES COMPLETE (Inclus Argile)
var columns = [
  'adm1_name', 'adm3_name1', 'adm3_pcode', 'center_lat', 'center_lon', 'year',
  
  // Population
  'pop_density_mean', 'pop_density_sum', 'pop_density_stdDev',
  
  // Topographie
  'elevation_mean', 'elevation_stdDev',
  'slope_mean', 'slope_stdDev',
  
  // Sols (pH, Carbone, Argile)
  'soil_ph_mean', 'soil_ph_stdDev',
  'soil_carbon_mean', 'soil_carbon_stdDev',
  'soil_clay_mean', 'soil_clay_stdDev', // Ajouté ici
  
  // Occupation du sol
  'cropland_pct_mean', 'cropland_pct_stdDev',
  'grassland_pct_mean', 'grassland_pct_stdDev',
  'tree_cover_pct_mean', 'tree_cover_pct_stdDev',
  'water_surface_pct_mean', 'water_surface_pct_stdDev',
  
  // Climat
  'temp_max_mean', 'temp_max_stdDev',
  'precip_q1_mean', 'precip_q1_sum', 'precip_q1_stdDev',
  'precip_q2_mean', 'precip_q2_sum', 'precip_q2_stdDev',
  'precip_q3_mean', 'precip_q3_sum', 'precip_q3_stdDev',
  'precip_q4_mean', 'precip_q4_sum', 'precip_q4_stdDev',
  
  // Végétation
  'ndvi_q1_mean', 'ndvi_q1_stdDev',
  'ndvi_q2_mean', 'ndvi_q2_stdDev',
  'ndvi_q3_mean', 'ndvi_q3_stdDev',
  'ndvi_q4_mean', 'ndvi_q4_stdDev',
  'ndvi_seasonal_std_mean',
  
  // indice d'aridité
  'aridity_index'
];

Export.table.toDrive({
  collection: finalCollection,
  description: 'cmr-arrondissements-final-features-2010_2024',
  fileFormat: 'CSV',
  selectors: columns
});