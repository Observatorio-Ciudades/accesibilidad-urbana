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


def main(gdf_mun):
    ################
    # DENUE TO BLOCK AND SPATIAL KDE

    # Download DENUE data
    schema = "denue"
    table = "denue_2022"

    denue_gdf = aup.gdf_from_polygon(gdf_mun, schema, table)

    denue_gdf = denue_gdf[['cve_ent','cve_mun','cve_loc',
                           'ageb','manzana',
                           'codigo_act','per_ocu','geometry']].copy()

    # Download Census data
    schema = "sociodemografico"
    table = "censo_inegi_20_mza"

    poly_geom = gdf_mun.dissolve().geometry.iloc[0]
    poly_wkt = poly_geom.wkt  # Este sí es un string

    # Consulta que devuelve WKT en lugar de geometría nativa
    query_censo = f"""
    SELECT
    "cvegeo_mza",
    "pobtot","geometry" FROM {schema}.{table}
    WHERE ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')
    """

    block_gdf = aup.gdf_from_query(query_censo)

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

    # Calcular la distancia de cada punto al centroide de su manzana
    denue_gdf['distancia'] = denue_gdf['geometry'].distance(denue_gdf['cnt_geometry'])

    # Calcular d_mean por manzana
    denue_to_cnt = denue_gdf.groupby(
        ['cvegeo_mza'])['distancia'].mean().reset_index(name='d_mean')

    # Add average distance to each block centroid
    denue_gdf = denue_gdf.merge(denue_to_cnt, on='cvegeo_mza')

    # Execute in parallel
    output_dir = '../../data/processed/prediccion_uso_suelo/monterrey/kde_output/'

    results = Parallel(n_jobs=16, verbose=1)(
        delayed(process_block_activities)(idx, manzana, denue_gdf, output_dir)
        for idx, manzana in tqdm(block_gdf.iterrows(), desc="Preparing tasks")
    )

    del results
    del denue_gdf
    del denue_to_cnt
    del block_gdf

    #########################
    # CREATE AREA OF PREDICTION

    # Read data
    bld_gdf = pd.read_csv('../../data/processed/prediccion_uso_suelo/843_buildings.csv')
    bld_gdf['geometry'] = bld_gdf['geometry'].apply(wkt.loads)
    bld_gdf = gpd.GeoDataFrame(bld_gdf, crs='epsg:4326')
    bld_gdf = bld_gdf.to_crs("EPSG:6372")

    # clip buildings to municipality
    gdf_mun = gdf_mun.to_crs("EPSG:6372")
    bld_clip = gpd.clip(bld_gdf, gdf_mun)
    bld_gdf = bld_gdf.loc[bld_gdf.full_plus_code.isin(list(bld_clip.full_plus_code.unique()))].copy()

    # Download block data
    schema = 'marco'
    table = 'mza_2020'
    query = f"SELECT * FROM {schema}.{table} WHERE ST_Intersects(geometry, 'SRID=4326;{poly_wkt}')"
    block_gdf = aup.gdf_from_query(query, geometry_col='geometry')

    block_gdf = block_gdf.to_crs("EPSG:6372")
    block_gdf = block_gdf[['CVEGEO','geometry']].copy()

    # Building data to blocks
    bld_block = gpd.overlay(bld_gdf, block_gdf, how='intersection')

    # Get unique CVEGEOs to process
    unique_cvegeos = bld_block.CVEGEO.unique()

    # Execute in parallel - FIXED: Remove extra brackets and pass correct parameters
    results = Parallel(n_jobs=20, verbose=1)(
        delayed(building_tesselation)(cvegeo, block_gdf, bld_block)
        for cvegeo in tqdm(unique_cvegeos, desc="Processing blocks")
    )

    # Filter out None results and concatenate all DataFrames
    valid_results = [result[0] for result in results if result is not None]
    tess_gdf = pd.concat(valid_results, ignore_index=True)

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

    tess_gdf.to_file('../../data/processed/prediccion_uso_suelo/monterrey/area_of_prediction_uso_suelo.gpkg')

    # del tess_gdf
    # del block_gdf
    del bld_clip
    del results
    del valid_results

    ########################
    # SPATIAL KDE TO AREA OF PREDICTION

    kde_dir = '../../data/processed/prediccion_uso_suelo/monterrey/kde_output/'
    aop_gdf = tess_gdf.copy()
    del tess_gdf

    if 'fid' not in list(aop_gdf.columns):
        aop_gdf = aop_gdf.reset_index().rename(columns={'index':'fid'})

    # Execute in single process

    aop_kde = gpd.GeoDataFrame()

    cvegeo_list = list(aop_gdf.CVEGEO.unique())

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

    aop_data = aop_data.merge(aop_kde[['fid','cultural_recreativo',
                                      'servicios','comercio','salud',
                                      'educacion','gobierno','industria']])

    aop_area = aop_data[['CVEGEO','area_m2']].groupby('CVEGEO').sum().reset_index().rename(columns={'area_m2':'area_m2_tot'})
    aop_data = aop_data.merge(aop_area, on='CVEGEO')
    aop_data['pobtot_relative'] = aop_data['pobtot'] * (aop_data['area_m2']/aop_data['area_m2_tot'])

    aop_data = aop_data.rename(columns={'pobtot_relative':'habitacional',
                                       'educación':'educacion'})

    uso_list = ['habitacional','cultural_recreativo','servicios',
               'comercio','salud','educacion','gobierno',
               'industria']

    aop_data['uso_tot'] = aop_data[uso_list].sum(axis=1)

    for us in uso_list:
        aop_data['pct_'+us] = aop_data[us]/aop_data['uso_tot']

    aop_data = aop_data.fillna(0)

    aop_data.to_file('../../data/processed/prediccion_uso_suelo/monterrey/tess_kde.gpkg')

    del aop_kde
    del block_gdf

    ##########################################
    # ENVIRONMENTAL DATA

    gdf = aop_data.copy()
    del aop_data

    schema = 'raster_analysis'
    table = 'ndvi_analysis_hex'
    city = 'Guadalajara'
    res = 11

    query = f'SELECT hex_id,ndvi_mean FROM {schema}.{table} WHERE \"city\" = \'{city}\' and \"res\"={res}'

    ndvi_gdf = aup.df_from_query(query)

    schema = 'raster_analysis'
    table = 'ndmi_analysis_hex'
    city = 'Guadalajara'
    res = 11

    query = f'SELECT hex_id,ndmi_diff FROM {schema}.{table} WHERE \"city\" = \'{city}\' and \"res\"={res}'

    ndmi_gdf = aup.df_from_query(query)

    schema = 'raster_analysis'
    table = 'temperature_analysis_hex'
    city = 'Guadalajara'
    res = 11

    query = f'SELECT hex_id,temperature_mean,geometry FROM {schema}.{table} WHERE \"city\" = \'{city}\' and \"res\"={res}'

    temp_gdf = aup.gdf_from_query(query, geometry_col='geometry')

    # calculate the variation from the mean
    temp_gdf = temp_gdf[~temp_gdf.temperature_mean.isin([float('inf')])].copy()
    temp_gdf['temperature_mean_diff'] = temp_gdf.temperature_mean.mean() - temp_gdf.temperature_mean
    temp_gdf = temp_gdf.drop(columns=['temperature_mean'])

    env_gdf = temp_gdf.copy()
    env_gdf = env_gdf.merge(ndvi_gdf, on='hex_id')
    env_gdf = env_gdf.merge(ndmi_gdf, on='hex_id')

    del ndvi_gdf
    del ndmi_gdf
    del temp_gdf

    env_gdf = env_gdf.to_crs("EPSG:6372")

    gdf_int = gdf.overlay(env_gdf, how='intersection')
    gdf_int = gdf_int[['full_plus_code','temperature_mean_diff',
            'ndvi_mean','ndmi_diff']].copy()

    gdf_int = gdf_int.groupby('full_plus_code').mean().reset_index()
    gdf = gdf.merge(gdf_int, on='full_plus_code')

    gdf.to_file('../../data/processed/prediccion_uso_suelo/monterrey/tess_kde_env.gpkg')

    del gdf_int
    del env_gdf

    #########################################
    # DISTANCE TO ROADS
    aop_gdf = gdf.copy()
    del gdf

    schema = "osmnx"
    table = "edges_osmnx_23_line"

    edges = aup.gdf_from_polygon(gdf_mun, schema, table)

    edges['highway'] = edges.highway.apply(lambda row: check_for_lists(row))

    edges.loc[edges.highway.map(lambda x:
                                isinstance(x, list)),'highway'] = edges.loc[
        edges.highway.map(lambda x: isinstance(x, list))].apply(
            lambda row: row['highway'][0], axis=1)

    edges.loc[edges['highway'].str.contains(
        "_link"),'highway'] = edges[edges['highway'].str.contains(
        "_link")].highway.apply(lambda x: x.replace('_link',''))

    edges = edges.to_crs("EPSG:4326")
    aop_gdf = aop_gdf.to_crs("EPSG:4326")

    pixel_size = 0.00023 # 0.00023° -> 25m

    output_dir = '../../data/processed/prediccion_uso_suelo/monterrey/prox_vialidades/'

    if 'fid' not in list(aop_gdf.columns):
        aop_gdf = aop_gdf.reset_index().rename(columns={'index':'fid'})

    # define bounds according to area of prediction
    bounds = []
    for c in aop_gdf.bounds:
        if 'min' in c:
            bounds.append(aop_gdf.bounds[c].min().item()-0.05)
        else:
            bounds.append(aop_gdf.bounds[c].max().item()+0.05)
    bounds = tuple(bounds)

    road_types = edges.highway.unique()
    results = []

    for road_type in tqdm(road_types, total=len(road_types), desc="Processing blocks"):

        results.append(road_type_to_area_of_prediction(aop_gdf, edges,
                                        road_type, pixel_size,
                                        bounds, output_dir)
                        )

    for results_df in results:
        aop_gdf = aop_gdf.merge(results_df, on='fid', how='left')

    if 'path_distance_y' in aop_gdf.columns:
        aop_gdf = aop_gdf.drop(columns=['primary_distance_y', 'service_distance_y', 'track_distance_y',
               'path_distance_y', 'tertiary_distance_y', 'living_street_distance_y',
               'footway_distance_y', 'cycleway_distance_y', 'secondary_distance_y',
               'steps_distance_y', 'pedestrian_distance_y', 'trunk_distance_y',
               'unclassified_distance_y', 'motorway_distance_y', 'corridor_distance_y',
               'bridleway_distance_y'])

        aop_gdf = aop_gdf.rename(columns={'residential_distance_x':'residential_distance',
                               'primary_distance_x':'primary_distance',
                               'service_distance_x':'service_distance',
                               'track_distance_x':'track_distance',
                               'path_distance_x':'path_distance',
                               'tertiary_distance_x':'tertiary_distance',
                               'living_street_distance_x':'living_street_distance',
                               'footway_distance_x':'footway_distance',
                               'cycleway_distance_x':'cycleway_distance',
                               'secondary_distance_x':'secondary_distance',
                               'steps_distance_x':'steps_distance',
                               'pedestrian_distance_x':'pedestrian_distance',
                               'trunk_distance_x':'trunk_distance',
                               'unclassified_distance_x':'unclassified_distance',
                               'motorway_distance_x':'motorway_distance',
                               'corridor_distance_x':'corridor_distance',
                               'bridleway_distance_x':'bridleway_distance',
                               })

        aop_gdf.to_file('../../data/processed/prediccion_uso_suelo/monterrey/area_of_prediction_roads.gpkg')

        del edges
        del results

        #############################################
        # PROXIMITY DATA
        gdf = aop_gdf.copy()
        del aop_gdf

        buffer = gdf_mun.to_crs("EPSG:6372").buffer(100)
        buffer = gpd.GeoDataFrame(geometry = buffer)
        buffer = buffer.to_crs("EPSG:4326")

        table = 'proximity_v2_23_point'
        schema = 'prox_analysis'

        prox_nodes = aup.gdf_from_polygon(buffer, schema, table)

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

        #################################
        # FIRST PERCENTAGE PREDICTION
        if 'fid' not in gdf.columns:
            gdf = gdf.reset_index().rename(columns={'index':'fid'})

        X = gdf[['area_m2','bld_area_m2','block_area_m2',
            'pred_area_m2', 'pred_area_pct', 'bld_pred_area_pct',
            'uso_tot','pobtot','pct_habitacional', 'pct_cultural_recreativo',
            'pct_servicios', 'pct_comercio', 'pct_salud', 'pct_educacion',
            'pct_gobierno', 'pct_industria',
            'temperature_mean_diff','ndvi_mean','ndmi_diff',
            'tertiary_distance','residential_distance', 'secondary_distance',
            'footway_distance','path_distance', 'service_distance',
            'living_street_distance','primary_distance', 'trunk_distance',
            'pedestrian_distance','cycleway_distance', 'unclassified_distance',
            'motorway_distance','steps_distance',
            'denue_primaria', 'denue_primaria_15min',
            'denue_abarrotes', 'denue_abarrotes_15min', 'denue_peluqueria',
            'denue_peluqueria_15min', 'denue_lavanderia', 'denue_lavanderia_15min',
            'clues_primer_nivel', 'clues_primer_nivel_15min'
            ]].to_numpy()

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        model_name = 'landuse_nn_ModelOne'
        model_one = pickle.load(open(f"../../data/processed/prediccion_uso_suelo/complete_model/{model_name}.pkl",'rb'))

        y_hat = model_one.predict(X_scaled)

        category_list = ['agropecuario','alojamiento_temporal',
                         'area_libre','area_natural','baldio','comercio',
                        'equipamiento','espacio_verde','habitacional',
                        'industria','infraestructura','mixto','otros',
                         'servicio','sin_dato']

        prefix = 'pct_'

        category_list = [prefix+cl for cl in category_list]

        gdf[category_list] = y_hat

        model_name = 'landuse_nn_ModelSmote'
        model_smote = pickle.load(open(f"../../data/processed/prediccion_uso_suelo/complete_model/{model_name}.pkl",'rb'))

        y_hat = model_smote.predict(X_scaled)

        category_list = ['agropecuario','alojamiento_temporal',
                         'area_libre','area_natural','baldio','comercio',
                        'equipamiento','espacio_verde','habitacional',
                        'industria','infraestructura','mixto','otros',
                         'servicio','sin_dato']

        prefix = 'pct_smote_'

        category_list = [prefix+cl for cl in category_list]

        gdf[category_list] = y_hat

        gdf.to_file('../../data/processed/prediccion_uso_suelo/complete_model/area_of_prediction_primera_pred.gpkg')

        X = gdf[['area_m2','bld_area_m2','block_area_m2',
            'pred_area_m2', 'pred_area_pct', 'bld_pred_area_pct',
            'uso_tot','pobtot','pct_habitacional', 'pct_cultural_recreativo',
            'pct_servicios', 'pct_comercio', 'pct_salud', 'pct_educacion',
            'pct_gobierno', 'pct_industria',
            'temperature_mean_diff','ndvi_mean','ndmi_diff',
            'tertiary_distance','residential_distance', 'secondary_distance',
            'footway_distance','path_distance', 'service_distance',
            'living_street_distance','primary_distance', 'trunk_distance',
            'pedestrian_distance','cycleway_distance', 'unclassified_distance',
            'motorway_distance','steps_distance',
            'denue_primaria', 'denue_primaria_15min',
            'denue_abarrotes', 'denue_abarrotes_15min', 'denue_peluqueria',
            'denue_peluqueria_15min', 'denue_lavanderia', 'denue_lavanderia_15min',
            'clues_primer_nivel', 'clues_primer_nivel_15min',
            'pct_alojamiento_temporal', 'pct_baldio', 'pct_equipamiento',
           'pct_espacio_verde', 'pct_infraestructura', 'pct_mixto', 'pct_servicio',
           'pct_sin_dato', 'pct_smote_alojamiento_temporal', 'pct_smote_baldio',
           'pct_smote_comercio', 'pct_smote_equipamiento',
           'pct_smote_espacio_verde', 'pct_smote_habitacional',
           'pct_smote_industria', 'pct_smote_infraestructura', 'pct_smote_mixto',
           'pct_smote_servicio', 'pct_smote_sin_dato'
            ]].to_numpy()

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        model_name = 'landuse_prediction_model'
        model_land_use = pickle.load(open(f"../../data/processed/prediccion_uso_suelo/monterrey/{model_name}.pkl",'rb'))

        y_hat = model_land_use.predict(X_scaled)

        gdf['pred'] = y_hat.argmax(axis=1)

        gdf[['fid','uso_suelo','pred','geometry']].to_file('../../data/processed/prediccion_uso_suelo/monterrey/tess_pred_smote.gpkg')



if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('Starting landuse prediction model')
    aup.log('Loading data...')

    schema = 'metropolis'
    table = 'metro_gdf_2020'
    city = 'Monterrey'
    query = f"SELECT * FROM {schema}.{table} WHERE city = \'{city}\'"

    gdf = aup.gdf_from_query(query)
    gdf = gdf.to_crs('EPSG:4326')

    for nombre in gdf.NOMGEO.unique():
        aup.log(f'Starting prediction for {nombre}')

        gdf_mun = gdf[gdf.NOMGEO == nombre].copy()


        main(gdf_mun)
