import os
import sys

import pandas as pd
import geopandas as gpd
import numpy as np

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


def main(city, cve_geolist, save=False,):
    res = 9 # analysis resolution
    c = city    
    year = 2020
    #Folder names from database
    block_schema = 'censo'
    block_cnt_folder = f'censo_mza_centroid_{year}'
    block_census_schema = 'censo_mza'
    block_census_folder = f'censo_mza_{year}'
    mpos_schema = 'marco'
    mpos_folder = f'mpos_{year}'
    aup.log("aNTES DE ITERAR")

    # Creates empty GeoDataFrame to store block locations
    block_centroid = gpd.GeoDataFrame()
    block_pop = pd.DataFrame()
    mun_gdf = gpd.GeoDataFrame()
    # Iterates over municipality codes for each metropolitan area or capital
    for i in  cvegeo_list :
        # Extracts specific municipality code
        # Downloads municipality polygon according to code
        cve_geo_values = ', '.join([f"'{value}'" for value in cve_geolist])
        query = f"SELECT * FROM {mpos_schema}.{mpos_folder} WHERE \"CVEGEO\" IN ({cve_geo_values})" 
        block_centroid = block_centroid.append(aup.gdf_from_query(query, geometry_col='geometry'))
        # Downloads municipality polygon according to code
        mun_gdf = mun_gdf.append(aup.gdf_from_query(query, geometry_col='geometry'))
        aup.log("LOGRADO")

    block_centroid = block_centroid.to_crs("EPSG:4326")
    block_centroid.head(2)
    gdf_tmp = mun_gdf.copy()
    gdf_tmp = gdf_tmp.to_crs("EPSG:6372")
    # gdf_tmp.set_geometry(0, False, True)
    gdf_tmp = gdf_tmp.buffer(1).reset_index()
    # gdf_tmp = gdf_tmp.buffer(1).reset_index().rename(columns={0:'geometry'})
    gdf_tmp = gdf_tmp.to_crs("EPSG:4326")
    poly_wkt = gdf_tmp.dissolve().geometry.to_wkt()[0]
    hex_schema = 'censo'
    hex_folder = f'hex_censo_mza_{year}_res{res}'

    hex_census_20 = gpd.GeoDataFrame()

    query = f"SELECT * FROM {hex_schema}.{hex_folder} WHERE (ST_Intersects(geometry, \'SRID=4326; {poly_wkt}\'))"
    hex_census_20 = hex_census_20.append(aup.gdf_from_query(query, geometry_col='geometry'))

    hex_census_20.head(2)

    year = 2010

    #Folder names from database
    block_schema = 'censo'
    block_cnt_folder = f'censo_mza_centroid_{year}'

    # Creates empty GeoDataFrame to store block locations
    block_centroid = gpd.GeoDataFrame()
    block_pop = pd.DataFrame()
    mun_gdf = gpd.GeoDataFrame()
    # Iterates over municipality codes for each metropolitan area or capital
    for i in cvegeo_list:
        # Extracts specific municipality code
        # Downloads municipality polygon according to code
        cve_geo_values = ', '.join([f"'{value}'" for value in cve_geolist])
        query = f"SELECT * FROM {mpos_schema}.{mpos_folder} WHERE \"CVEGEO\" IN ({cve_geo_values})" 
        block_centroid = block_centroid.append(aup.gdf_from_query(query, geometry_col='geometry'))

        block_centroid = block_centroid.to_crs("EPSG:4326")
        block_centroid.head(2)

    ### Download hex_census data

    hex_schema = 'censo'
    hex_folder = f'hex_censo_mza_{year}_res{res}'

    hex_census_10 = gpd.GeoDataFrame()

    query = f"SELECT * FROM {hex_schema}.{hex_folder} WHERE (ST_Intersects(geometry, \'SRID=4326; {poly_wkt}\'))"
    hex_census_10 = hex_census_10.append(aup.gdf_from_query(query, geometry_col='geometry'))

    aup.log(hex_census_10.shape)
    hex_census_10.head(2)


    ## Data comparison

    hex_census_10 = hex_census_10.add_prefix('10_')
    hex_census_10.drop(columns=['10_geometry'], inplace=True)
    hex_census_10.rename(columns={f'10_hex_id_{res}':f'hex_id_{res}'}, inplace=True)

    hex_mrg = hex_census_20.merge(hex_census_10, on=f'hex_id_{res}', how='left')
    aup.log(hex_mrg.shape)
    hex_mrg.head(2)
    hex_mrg_10_20 = hex_mrg.copy()
    hex_mrg_10_20.replace(np.nan, 0, inplace=True)

    ### Change calculation
    aup.log("Cálculos")

    hex_mrg_10_20['T_Pob_10_20'] = hex_mrg_10_20['pobtot'] - hex_mrg_10_20['10_pobtot']
    hex_mrg_10_20['T_Viv_10_20'] = hex_mrg_10_20['vivtot'] - hex_mrg_10_20['10_vivtot']
    hex_mrg_10_20['R_TViv_10_20'] = (hex_mrg_10_20['vivtot'] - hex_mrg_10_20['10_vivtot']) / hex_mrg_10_20['10_vivtot']
    hex_mrg_10_20['T_VivDes_10_20'] = hex_mrg_10_20['vivpar_des'] - hex_mrg_10_20['10_vivpar_des']
    hex_mrg_10_20['R_TVivDes_10_20'] = (hex_mrg_10_20['vivpar_des'] - hex_mrg_10_20['10_vivpar_des']) / hex_mrg_10_20['10_vivpar_des']
    hex_mrg_10_20['Z_RTVivDes_10_20'] = (hex_mrg_10_20['R_TVivDes_10_20']-hex_mrg_10_20['R_TVivDes_10_20'].mean()) / hex_mrg_10_20['R_TVivDes_10_20'].std()
    hex_mrg_10_20['Z_RTViv_10_20'] = (hex_mrg_10_20['R_TViv_10_20']-hex_mrg_10_20['R_TViv_10_20'].mean()) / hex_mrg_10_20['R_TViv_10_20'].std()
    hex_mrg_10_20['R_VivHab_20'] = hex_mrg_10_20['tvivparhab'] / hex_mrg_10_20['vivtot']
    hex_mrg_10_20['R_VivDes_20'] = hex_mrg_10_20['vivpar_des'] / hex_mrg_10_20['vivtot']
    hex_mrg_10_20['Z_RVivHab_20'] = (hex_mrg_10_20['R_VivHab_20'] - hex_mrg_10_20['R_VivHab_20'].mean()) / hex_mrg_10_20['R_VivHab_20'].std()
    hex_mrg_10_20['R_VivHab_10'] = hex_mrg_10_20['10_tvivparhab'] / hex_mrg_10_20['10_vivtot']
    hex_mrg_10_20['Z_RVivHab_10'] = (hex_mrg_10_20['R_VivHab_10'] - hex_mrg_10_20['R_VivHab_10'].mean()) / hex_mrg_10_20['R_VivHab_10'].std()
    hex_mrg_10_20['Chng_RVivHab_10'] = hex_mrg_10_20['R_VivHab_20'] - hex_mrg_10_20['R_VivHab_10']
    
    aup.log("Guarda")
    if save: 
         hex_mrg_10_20.to_file(f'../data/processed/pop_chng/{c}_Census_10_20_BlockAnalysis_res{res}.geojson', driver='GeoJSON')


if __name__ == "__main__":
    aup.log('--'*20)
    aup.log('Starting script')
    #main("Tijuana")

    gdf_mun = aup.gdf_from_db('metro_gdf', 'metropolis')

    # prevent cities being analyzed to times in case of a crash
    processed_city_list = []
    try:
        processed_city_list = aup.gdf_from_db('cd_script18_hexres8', 'prox_analysis')
        #quitar cd cuidadoras_hexres8 cambiar el nombre a script 18 
        processed_city_list = list(processed_city_list.city.unique())
    except:
        pass

    for city in gdf_mun.city.unique():
        aup.log(f'\n Starting city {city}')
        if city not in processed_city_list:
            cvegeo_list = list(gdf_mun.loc[gdf_mun.city==city]["CVEGEO"].unique())

            main(city, cvegeo_list)