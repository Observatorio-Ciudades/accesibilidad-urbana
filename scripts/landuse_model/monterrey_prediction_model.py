import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio as ro

from joblib import Parallel, delayed
from tqdm import tqdm

import pickle

import distancerasters as dr

import momepy as mp
from shapely import wkt

from spatial_kde import spatial_kernel_density

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
from sklearn.decomposition import PCA

from keras import utils                                   # tools for creating one-hot encoding
from keras.models import Sequential                       # Type of model we wish to use
from keras.layers import Dense, Dropout, Activation
from sklearn.preprocessing import LabelEncoder
# from scikeras.wrappers import KerasClassifier, KerasRegressor
from keras import utils
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.preprocessing import LabelEncoder
# from sklearn.pipeline import Pipeline

from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error,confusion_matrix

from prediction_functions import *

import os
import sys
module_path = os.path.abspath(os.path.join('../../'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup
else:
    import aup


def main(gdf_mun, city_name, metropolitan_area_name):
    ################
    # DENUE TO BLOCK AND SPATIAL KDE

    # Download DENUE data
    schema = "denue"
    table = "denue_2022"

    aup.log(f"Downloading DENUE data for {city_name}")

    denue_gdf = aup.gdf_from_polygon(gdf_mun, schema, table)

    denue_gdf = denue_gdf[['cve_ent','cve_mun','cve_loc',
                           'ageb','manzana',
                           'codigo_act','per_ocu','geometry']].copy()

    aup.log(f"Downloaded {denue_gdf.shape[0]} rows")

    # Download Census data
    schema = "sociodemografico"
    table = "censo_inegi_20_mza"

    poly_geom = gdf_mun.dissolve().geometry.iloc[0]
    poly_wkt = poly_geom.wkt  # Este sí es un string

    aup.log(f"Downloading Census data for {city_name}")

    # Consulta que devuelve WKT en lugar de geometría nativa
    query_censo = f"""
    SELECT
    "cvegeo_mza",
    "pobtot","geometry" FROM {schema}.{table}
    WHERE ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')
    """

    block_gdf = aup.gdf_from_query(query_censo)

    aup.log(f"Downloaded {block_gdf.shape[0]} rows")

    # Change crs
    gdf_mun = gdf_mun.to_crs("EPSG:6372")
    denue_gdf = denue_gdf.to_crs("EPSG:6372")
    block_gdf = block_gdf.to_crs("EPSG:6372")


    denue_gdf['tipo_act'] = denue_gdf['codigo_act'].apply(asignar_tipo)
    denue_gdf['per_ocu_num'] = denue_gdf.per_ocu.apply(lambda per_ocu: number_of_jobs(per_ocu))

    # Create block CVEGEO column from denue
    denue_gdf['cvegeo_mza'] = (
        denue_gdf['cve_ent'].astype(str).str[:2] +
        denue_gdf['cve_mun'].astype(str).str[:3] +
        denue_gdf['cve_loc'].astype(str).str[:4] +
        denue_gdf['ageb'].astype(str).str[:4] +
        denue_gdf['manzana'].astype(str).str[:3]
    )

    aup.log("Created CVEGEO column in DENUE")

    # Create centroid from blocks
    block_cnt = block_gdf.copy()
    block_cnt['cnt_geometry'] = block_cnt['geometry'].centroid

    # Merge block centroid geometry to DENUE gdf
    denue_gdf = pd.merge(
        denue_gdf,
        block_cnt[['cvegeo_mza', 'cnt_geometry']],
        on=['cvegeo_mza'],
        how='inner'
    )

    aup.log("Merged block centroid geometry to DENUE gdf")

    # Calcular la distancia de cada punto al centroide de su manzana
    denue_gdf['distancia'] = denue_gdf['geometry'].distance(denue_gdf['cnt_geometry'])

    aup.log("Calculated distance from each point to its block centroid")

    # Calcular d_mean por manzana
    denue_to_cnt = denue_gdf.groupby(['cvegeo_mza'])['distancia'].mean()
    denue_to_cnt = denue_to_cnt.reset_index(name='d_mean')

    # Add average distance to each block centroid
    denue_gdf = denue_gdf.merge(denue_to_cnt, on='cvegeo_mza')

    aup.log("Added average distance to each block centroid")

    # Execute in parallel
    output_dir = '../../data/processed/prediccion_uso_suelo/monterrey/kde_output/'

    aup.log("Executing KDE activities by block in parallel")

    results = Parallel(n_jobs=16, verbose=1)(
        delayed(process_block_activities)(idx, manzana, denue_gdf, output_dir)
        for idx, manzana in tqdm(block_gdf.iterrows(), desc="Preparing tasks")
    )

    del results
    del denue_gdf
    del denue_to_cnt
    del block_gdf

    aup.log("Finished executing KDE activities by block in parallel and deleted variables")

    #########################
    # CREATE AREA OF PREDICTION

    aup.log("Reading building data")

    # Read data
    bld_gdf = pd.read_csv('../../data/processed/prediccion_uso_suelo/867_buildings.csv')
    bld_gdf['geometry'] = bld_gdf['geometry'].apply(wkt.loads)
    bld_gdf = gpd.GeoDataFrame(bld_gdf, crs='epsg:4326')
    bld_gdf = bld_gdf.to_crs("EPSG:6372")
    aup.log(f"Finished reading {len(bld_gdf)} building data")
    bld_gdf = bld_gdf.loc[bld_gdf.confidence>=0.7].copy()
    aup.log(f"Finished filtering building data with {len(bld_gdf)} buildings")

    # clip buildings to municipality
    gdf_mun = gdf_mun.to_crs("EPSG:6372")
    bld_clip = gpd.clip(bld_gdf, gdf_mun)
    bld_gdf = bld_gdf.loc[bld_gdf.full_plus_code.isin(list(bld_clip.full_plus_code.unique()))].copy()

    aup.log(f"Finished reading and clipping building data with {len(bld_gdf)} buildings")

    # Download block data
    schema = 'marco'
    table = 'mza_2020'
    query = f"SELECT * FROM {schema}.{table} WHERE ST_Intersects(geometry, 'SRID=4326;{poly_wkt}')"
    block_gdf = aup.gdf_from_query(query, geometry_col='geometry')

    block_gdf = block_gdf.to_crs("EPSG:6372")
    block_gdf = block_gdf[['CVEGEO','geometry']].copy()

    # Building data to blocks
    bld_block = gpd.overlay(bld_gdf, block_gdf, how='intersection')

    aup.log(f"Finished overlaying building data with {len(bld_block)} buildings")

    bld_block['area_in_meters'] = bld_block.area
    bld_block = bld_block.loc[bld_block.area_in_meters>=50].copy()
    aup.log(f"Filtered clipped building data by area to {len(bld_block)} buildings")


    # Get unique CVEGEOs to process
    unique_cvegeos = bld_block.CVEGEO.unique()

    aup.log(f"Create building tesselation with parallel processing")

    # Execute in parallel - FIXED: Remove extra brackets and pass correct parameters
    results = Parallel(n_jobs=20, verbose=1)(
        delayed(building_tesselation)(cvegeo, block_gdf, bld_block)
        for cvegeo in tqdm(unique_cvegeos, desc="Processing blocks")
    )

    # Filter out None results and concatenate all DataFrames
    valid_results = [result[0] for result in results if result is not None]
    tess_gdf = pd.concat(valid_results, ignore_index=True)

    aup.log(f"Finished creating building tesselation with {len(tess_gdf)} polygons")

    # block to area of prediction
    block_gdf['block_area_m2'] = block_gdf.to_crs("EPSG:6372").area
    tess_gdf = tess_gdf.merge(block_gdf[['CVEGEO','block_area_m2']],
                             on='CVEGEO')

    # convert to GeoDataFrame
    tess_gdf = gpd.GeoDataFrame(tess_gdf)
    tess_gdf = tess_gdf.set_crs("EPSG:6372")

    tess_gdf = tess_gdf.rename(columns={'area_in_meters':'bld_area_m2'})

    tess_gdf = tess_gdf.reset_index(drop=True).reset_index().rename(columns={'index':'fid'})

    tess_gdf['area_m2'] = tess_gdf.area

    # update area
    tess_gdf['pred_area_m2'] = tess_gdf.area
    tess_gdf['pred_area_pct'] = tess_gdf['pred_area_m2'] / tess_gdf['block_area_m2']

    tess_gdf['bld_pred_area_pct'] = tess_gdf['bld_area_m2'] / tess_gdf['pred_area_m2']

    tess_gdf.to_file('../../data/processed/prediccion_uso_suelo/monterrey/area_of_prediction.gpkg')

    aup.log(f"Finished saving building tesselation to file")

    # del tess_gdf
    # del block_gdf
    del bld_clip
    del results
    del valid_results

    ########################
    # SPATIAL KDE TO AREA OF PREDICTION

    aup.log(f"Starting spatial KDE to area of prediction")

    kde_dir = '../../data/processed/prediccion_uso_suelo/monterrey/kde_output/'
    aop_gdf = tess_gdf.copy()
    del tess_gdf

    if 'fid' not in list(aop_gdf.columns):
        aop_gdf = aop_gdf.reset_index().rename(columns={'index':'fid'})

    # Download census data
    poly_wkt = gdf_mun.to_crs("EPSG:4326").dissolve().geometry.to_wkt()[0]
    schema = "sociodemografico"
    table = "censo_inegi_20_mza"

    # Consulta que devuelve WKT en lugar de geometría nativa
    query_censo = f"""
    SELECT
    "cvegeo_mza",
    "pobtot","geometry" FROM {schema}.{table}
    WHERE ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')
    """

    block_gdf = aup.gdf_from_query(query_censo)

    # add population data to area of prediction
    aop_data = aop_gdf.merge(block_gdf[['cvegeo_mza','pobtot']], left_on='CVEGEO',
                            right_on='cvegeo_mza')
    aop_data = aop_data.drop(columns=['cvegeo_mza'])

    # Execute in single process

    aop_kde = gpd.GeoDataFrame()

    cvegeo_list = list(aop_gdf.CVEGEO.unique())

    aup.log(f"Starting spatial KDE to area of prediction")

    for cvegeo in tqdm(cvegeo_list, total=len(cvegeo_list), desc="Processing blocks"):
        kde_block = f'kde_mnz_{cvegeo}'
        aop_tmp = aop_gdf.loc[aop_gdf.CVEGEO==cvegeo].copy()

        # iterate over every file
        for filename in os.listdir(kde_dir):

            # gather those corresponding to the specific block
            if filename.startswith(kde_block):

                # skip complementary raster files
                if filename.endswith('.aux.xml'):
                    continue

                kde_act = filename.replace(kde_block+'_',"").replace('.tif',"").lower()

                # read file
                raster_kde = ro.open(kde_dir+filename)

                aop_tmp[kde_act] = aop_tmp.geometry.apply(lambda geom: aup.clean_mask(geom, raster_kde)).apply(np.ma.mean)

        aop_kde = pd.concat([aop_kde, aop_tmp])

    aop_kde = aop_kde.fillna(0)

    aup.log(f"Finished spatial KDE to area of prediction")

    uso_list = ['agropecuario','industria',
        'servicios','alojamiento','comercio',
        'cultural_recreativo','educacion','salud','gobierno','otros']

    for uso in uso_list:
        if uso not in aop_kde.columns:
            aop_kde[uso] = 0
            aup.log(f"Added column {uso} to aop_kde")

    aop_data = aop_data.merge(aop_kde[['fid','agropecuario','industria',
        'servicios','alojamiento','comercio',
        'cultural_recreativo','educacion','salud','gobierno','otros']])

    aop_area = aop_data[['CVEGEO','area_m2']].groupby('CVEGEO').sum().reset_index().rename(columns={'area_m2':'area_m2_tot'})
    aop_data = aop_data.merge(aop_area, on='CVEGEO')
    aop_data['pobtot_relative'] = aop_data['pobtot'] * (aop_data['area_m2']/aop_data['area_m2_tot'])

    aop_data = aop_data.rename(columns={'pobtot_relative':'habitacional',
                                       'educación':'educacion'})

    aop_data['uso_tot'] = aop_data[uso_list].sum(axis=1)

    for us in uso_list:
        aop_data['pct_'+us] = aop_data[us]/aop_data['uso_tot']

    aop_data = aop_data.fillna(0)

    aop_data.to_file('../../data/processed/prediccion_uso_suelo/monterrey/area_of_prediction_kde.gpkg')

    del aop_kde
    del block_gdf

    aup.log("Finished processing KDE to Area of Prediction")

    ##########################################
    # ENVIRONMENTAL DATA

    aup.log("Started processing environmental data")


    gdf = aop_data.copy()
    del aop_data

    aup.log("Downloading NDVI data")

    schema = 'raster_analysis'
    table = 'ndvi_analysis_hex'
    res = 11

    query = f'SELECT hex_id,ndvi_mean FROM {schema}.{table} WHERE \"city\" = \'{metropolitan_area_name}\' and \"res\"={res}'

    ndvi_gdf = aup.df_from_query(query)

    aup.log(f"Downloaded {len(ndvi_gdf)} rows")

    aup.log("Downloading NDMI data")

    schema = 'raster_analysis'
    table = 'ndmi_analysis_hex'
    res = 11

    query = f'SELECT hex_id,ndmi_diff FROM {schema}.{table} WHERE \"city\" = \'{metropolitan_area_name}\' and \"res\"={res}'

    ndmi_gdf = aup.df_from_query(query)

    aup.log(f"Downloaded {len(ndmi_gdf)} rows")

    aup.log("Downloading Temperature data")

    schema = 'raster_analysis'
    table = 'temperature_analysis_hex'
    res = 11

    query = f'SELECT hex_id,temperature_mean,geometry FROM {schema}.{table} WHERE \"city\" = \'{metropolitan_area_name}\' and \"res\"={res}'

    temp_gdf = aup.gdf_from_query(query, geometry_col='geometry')

    aup.log(f"Downloaded {len(temp_gdf)} rows")

    # calculate the variation from the mean
    temp_gdf = temp_gdf[~temp_gdf.temperature_mean.isin([float('inf')])].copy()
    temp_gdf['temperature_mean_diff'] = temp_gdf.temperature_mean.mean() - temp_gdf.temperature_mean
    temp_gdf = temp_gdf.drop(columns=['temperature_mean'])

    env_gdf = temp_gdf.copy()
    env_gdf = env_gdf.merge(ndvi_gdf, on='hex_id')
    env_gdf = env_gdf.merge(ndmi_gdf, on='hex_id')

    aup.log("Merged environment data")

    del ndvi_gdf
    del ndmi_gdf
    del temp_gdf

    env_gdf = env_gdf.set_crs("EPSG:4326")
    env_gdf = env_gdf.to_crs("EPSG:6372")

    gdf_int = gdf.overlay(env_gdf, how='intersection')
    gdf_int = gdf_int[['full_plus_code','temperature_mean_diff',
            'ndvi_mean','ndmi_diff']].copy()

    gdf_int = gdf_int.groupby('full_plus_code').mean().reset_index()
    gdf = gdf.merge(gdf_int, on='full_plus_code')

    gdf.to_file('../../data/processed/prediccion_uso_suelo/monterrey/area_of_prediction_env.gpkg')

    del gdf_int
    del env_gdf

    aup.log(f"Finished transfering environmental data to {len(gdf)} polygons")

    #########################################
    # DISTANCE TO ROADS

    aup.log("Started processing distance to roads")

    aop_gdf = gdf.copy()
    del gdf

    aup.log("Downloading edges data")

    schema = "osmnx"
    table = "edges_osmnx_23_line"

    edges = aup.gdf_from_polygon(gdf_mun, schema, table)

    aup.log(f"Downloaded {len(edges)} edges")

    aup.log("Processing edges data")

    edges['highway'] = edges.highway.apply(lambda row: check_for_lists(row))

    edges.loc[edges.highway.map(lambda x:
                                isinstance(x, list)),'highway'] = edges.loc[
        edges.highway.map(lambda x: isinstance(x, list))].apply(
            lambda row: row['highway'][0], axis=1)

    edges.loc[edges['highway'].str.contains(
        "_link"),'highway'] = edges[edges['highway'].str.contains(
        "_link")].highway.apply(lambda x: x.replace('_link',''))

    road_dict = {
        'motorway':['motorway'],
        'primary':['primary'],
        'secondary':['secondary'],
        'tertiary':['tertiary'],
        'residential':['residential','living_street'],
        'other':[]
    }

    road_list = ['motorway','primary','secondary',
                'tertiary','residential','living_street']

    for road in edges.highway.unique():
        if road not in road_list:
            road_dict['other'].append(road)

    aup.log(f"Preprocessed edges")

    edges = edges.to_crs("EPSG:4326")
    aop_gdf = aop_gdf.to_crs("EPSG:4326")

    pixel_size = 0.00023 # 0.00023° -> 25m

    output_dir = '../../data/processed/prediccion_uso_suelo/monterrey/prox_vialidades/'

    if 'fid' not in list(aop_gdf.columns):
        aop_gdf = aop_gdf.reset_index().rename(columns={'index':'fid'})

    # define bounds according to area of prediction
    bounds = []
    for c in aop_gdf.bounds:
        try:
            if 'min' in c:
                bounds.append(aop_gdf.bounds[c].min().item()-0.05)
            else:
                bounds.append(aop_gdf.bounds[c].max().item()+0.05)
        except AttributeError:
            if 'min' in c:
                bounds.append(aop_gdf.bounds[c].min()-0.05)
            else:
                bounds.append(aop_gdf.bounds[c].max()+0.05)
    bounds = tuple(bounds)

    results = []

    aup.log("Starting parallel processing for proximity to roads")

    for road_class in tqdm(road_dict.keys(), total=len(road_dict.keys()), desc="Processing blocks"):

        road_type = road_dict[road_class]

        try:

            results.append(road_type_to_area_of_prediction(aop_gdf,
                                                            edges,
                                                            road_class,
                                                            road_type,
                                                            pixel_size,
                                                            bounds,
                                                            output_dir)
                                                            )
        except Exception as e:
            aup.log(f"Error processing road class {road_class}: {str(e)}")
            continue

    for results_df in results:
        aop_gdf = aop_gdf.merge(results_df, on='fid', how='left')

    aup.log("Finished parallel processing for proximity to roads")

    for road in road_list:
        if f"{road}_distance_x" not in aop_gdf.columns:
            aup.log(f"Missing column {road}_distance_x")
            aop_gdf[f"{road}_distance_x"] = 0
            aop_gdf[f"{road}_distance_y"] = 0
        elif f"{road}_distance_y" not in aop_gdf.columns:
            aup.log(f"Missing column {road}_distance_y")
            aop_gdf[f"{road}_distance_y"] = 0
            aop_gdf["{road}_distance_x"] = 0


    if 'path_distance_y' in aop_gdf.columns:
        aop_gdf = aop_gdf.drop(columns=['motorway_distance_y', 'primary_distance_y',
               'secondary_distance_y', 'tertiary_distance_y', 'residential_distance_y',
               'other_distance_y'])

        aop_gdf = aop_gdf.rename(columns={'residential_distance_x':'residential_distance',
                                'primary_distance_x':'primary_distance',
                                'tertiary_distance_x':'tertiary_distance',
                                'secondary_distance_x':'secondary_distance',
                                'motorway_distance_x':'motorway_distance',
                                            'other_distance_x':'other_distance'
                                })

    aop_gdf.to_file('../../data/processed/prediccion_uso_suelo/monterrey/area_of_prediction_roads.gpkg')

    del edges
    del results

    aup.log("Finished processing proximity to roads")

    #############################################
    # PROXIMITY DATA

    aup.log("Processing proximity to amenities")

    gdf = aop_gdf.copy()
    del aop_gdf

    buffer = gdf_mun.to_crs("EPSG:6372").buffer(100)
    buffer = gpd.GeoDataFrame(geometry = buffer)
    buffer = buffer.to_crs("EPSG:4326")

    table = 'proximity_v2_23_point'
    schema = 'prox_analysis'

    prox_nodes = aup.gdf_from_polygon(buffer, schema, table)

    aup.log(f"Downloaded {prox_nodes.shape[0]} proximity data")

    cols = ['osmid','denue_primaria','denue_primaria_15min',
            'denue_abarrotes','denue_abarrotes_15min','denue_peluqueria',
            'denue_peluqueria_15min','denue_lavanderia','denue_lavanderia_15min',
            'clues_primer_nivel','clues_primer_nivel_15min','geometry']
    prox_nodes = prox_nodes[cols].copy()

    prox_nodes = prox_nodes.to_crs("EPSG:6372")
    gdf = gdf.to_crs("EPSG:6372")

    if 'fid' not in gdf.columns:
        gdf = gdf.reset_index().rename(columns={'index':'fid'})

    gdf_cnt = gdf[['fid','geometry']].copy()
    gdf_cnt['geometry'] = gdf_cnt.centroid

    aup.log("Interpolate proximity data to area of prediction")

    for col in cols:

        if (col != 'osmid') and (col != 'geometry'):

            division_value = 10000

            gdf_int = gpd.GeoDataFrame()

            for i in range(round(len(gdf_cnt)/division_value)):

                gdf_tmp = gdf_cnt.iloc[i*division_value:
                (i+1)*division_value].copy()

                int_vals = aup.interpolate_at_points(prox_nodes.centroid.x,
                                                    prox_nodes.centroid.y,
                                                    prox_nodes[col],
                                                    gdf_tmp.geometry.x,
                                                    gdf_tmp.geometry.y,
                                                power=2,
                                                search_radius=300)
                gdf_tmp[col] = int_vals

                gdf_int = pd.concat([gdf_int,gdf_tmp])

            gdf_cnt = gdf_cnt.merge(gdf_int[['fid',col]], on='fid')
            aup.log('Finished processing',col)

    gdf = gdf.merge(gdf_cnt.drop(columns=['geometry']),
                    on='fid')

    gdf.to_file('../../data/processed/prediccion_uso_suelo/monterrey/area_of_prediction_prox.gpkg')

    del prox_nodes
    del gdf_cnt

    aup.log("Finished processing proximity data to area of prediction")

    #################################
    # FIRST PERCENTAGE PREDICTION

    aup.log("Starting first percentage prediction")

    if 'fid' not in gdf.columns:
        gdf = gdf.reset_index().rename(columns={'index':'fid'})

    X = gdf[['bld_area_m2', 'block_area_m2', 'pred_area_m2', 'pred_area_pct',
            'bld_pred_area_pct','area_m2_tot',
            'pobtot', 'agropecuario', 'industria', 'servicios',
            'alojamiento', 'comercio', 'cultural_recreativo', 'educacion', 'salud',
            'gobierno', 'otros',  'habitacional',
            'uso_tot',
            'pct_agropecuario', 'pct_industria', 'pct_servicios', 'pct_alojamiento',
            'pct_comercio', 'pct_cultural_recreativo', 'pct_educacion', 'pct_salud',
            'pct_gobierno', 'pct_otros',
            'temperature_mean_diff', 'ndvi_mean',
            'ndmi_diff',
            'motorway_distance', 'primary_distance',
            'secondary_distance', 'tertiary_distance', 'residential_distance',
            'other_distance',
            'denue_primaria', 'denue_primaria_15min',
            'denue_abarrotes', 'denue_abarrotes_15min', 'denue_peluqueria',
            'denue_peluqueria_15min', 'denue_lavanderia', 'denue_lavanderia_15min',
            'clues_primer_nivel', 'clues_primer_nivel_15min'
        ]].to_numpy()

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model_name = 'landuse_nn_ModelOne'
    model_one = pickle.load(open(f"../../data/processed/prediccion_uso_suelo/complete_model/{model_name}.pkl",'rb'))

    aup.log(f"Starting prediction with {model_name}")

    y_hat = model_one.predict(X_scaled)

    aup.log(f"Finished prediction with {model_name}")

    category_list = ['agropecuario','alojamiento_temporal',
                        'area_libre','area_natural','baldio','comercio',
                    'equipamiento','espacio_verde','habitacional',
                    'industria','infraestructura','mixto','otros',
                        'servicio','sin_dato']

    prefix = 'pct_pred_'

    category_list = [prefix+cl for cl in category_list]

    gdf[category_list] = y_hat

    aup.log("Assigned predictions percentages to area of prediction")

    model_name = 'landuse_nn_ModelSmote'
    model_smote = pickle.load(open(f"../../data/processed/prediccion_uso_suelo/complete_model/{model_name}.pkl",'rb'))

    aup.log(f"Starting prediction with {model_name}")

    y_hat = model_smote.predict(X_scaled)

    aup.log(f"Finished prediction with {model_name}")

    category_list = ['agropecuario','alojamiento_temporal',
                        'area_libre','area_natural','baldio','comercio',
                    'equipamiento','espacio_verde','habitacional',
                    'industria','infraestructura','mixto','otros',
                        'servicio','sin_dato']

    prefix = 'pct_smote_'

    category_list = [prefix+cl for cl in category_list]

    gdf[category_list] = y_hat

    aup.log("Assigned predictions percentages to area of prediction")

    gdf.to_file('../../data/processed/prediccion_uso_suelo/monterrey/area_of_prediction_primera_pred.gpkg')

    X = gdf[['bld_area_m2', 'block_area_m2', 'pred_area_m2', 'pred_area_pct',
            'bld_pred_area_pct','area_m2_tot',
            'pobtot', 'agropecuario', 'industria', 'servicios',
            'alojamiento', 'comercio', 'cultural_recreativo', 'educacion', 'salud',
            'gobierno', 'otros',  'habitacional',
            'uso_tot',
            'pct_agropecuario', 'pct_industria', 'pct_servicios', 'pct_alojamiento',
            'pct_comercio', 'pct_cultural_recreativo', 'pct_educacion', 'pct_salud',
            'pct_gobierno', 'pct_otros',
            'temperature_mean_diff', 'ndvi_mean',
            'ndmi_diff',
            'motorway_distance', 'primary_distance',
            'secondary_distance', 'tertiary_distance', 'residential_distance',
            'other_distance',
            'denue_primaria', 'denue_primaria_15min',
            'denue_abarrotes', 'denue_abarrotes_15min', 'denue_peluqueria',
            'denue_peluqueria_15min', 'denue_lavanderia', 'denue_lavanderia_15min',
            'clues_primer_nivel', 'clues_primer_nivel_15min',
        'pct_pred_agropecuario', 'pct_pred_alojamiento_temporal',
            'pct_pred_area_libre', 'pct_pred_area_natural', 'pct_pred_baldio',
            'pct_pred_comercio', 'pct_pred_equipamiento', 'pct_pred_espacio_verde',
            'pct_pred_habitacional', 'pct_pred_industria',
            'pct_pred_infraestructura', 'pct_pred_mixto', 'pct_pred_otros',
            'pct_pred_servicio', 'pct_pred_sin_dato',
        'pct_smote_agropecuario',
        'pct_smote_alojamiento_temporal', 'pct_smote_area_libre',
        'pct_smote_area_natural', 'pct_smote_baldio', 'pct_smote_comercio',
        'pct_smote_equipamiento', 'pct_smote_espacio_verde',
        'pct_smote_habitacional', 'pct_smote_industria',
        'pct_smote_infraestructura', 'pct_smote_mixto', 'pct_smote_otros',
        'pct_smote_servicio', 'pct_smote_sin_dato',
        ]].to_numpy()

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model_name = 'landuse_nn_PredictionModel'
    model_land_use = pickle.load(open(f"../../data/processed/prediccion_uso_suelo/complete_model/{model_name}.pkl",'rb'))

    aup.log(f"Starting prediction with {model_name}")

    y_hat = model_land_use.predict(X_scaled)

    aup.log(f"Finished prediction with {model_name}")

    gdf['pred'] = y_hat.argmax(axis=1)

    gdf[['fid','pred','geometry']].to_file(f'../../data/processed/prediccion_uso_suelo/monterrey/{city_name}_area_of_prediction_landuse.gpkg')



if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('Starting landuse prediction model')
    aup.log('Loading data...')

    schema = 'metropolis'
    table = 'metro_gdf_2020'
    metropolitan_area_name = 'Monterrey'
    query = f"SELECT * FROM {schema}.{table} WHERE city = \'{metropolitan_area_name}\'"

    gdf = aup.gdf_from_query(query)
    gdf = gdf.to_crs('EPSG:4326')

    for nombre in gdf.NOMGEO.unique():
        aup.log(f'Starting prediction for {nombre}')

        gdf_mun = gdf[gdf.NOMGEO == nombre].copy()


        main(gdf_mun, nombre, metropolitan_area_name)
