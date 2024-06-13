import geopandas as gpd
import pandas as pd
import numpy as np

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup

def main(source_list, hex_gdf, nodes, nodes_save_table, local_save=False, save=False):
    
    aup.log("--"*40)
    aup.log(f"--- STARTING MAIN FUNCTION.")
    
    ############################################################### PART 1 ###############################################################
    #################################################### FIND NODES PROXIMITY TO POIS ####################################################
    
    # 1.1 --------------- NODES PROXIMITY TO POIS
    # ------------------- This step loads each source of interest, calculates nodes proximity and saves to database
    k = len(source_list)
    i = 1

    # Create nodes analysis gdf
    nodes_analysis = nodes.reset_index().copy()
    del nodes
    nodes_analysis = nodes_analysis[['osmid','geometry']]
    
    for source in source_list:

        aup.log(f"--- Starting nodes proximity to pois for source {i}/{k}: {source}. ")
        # Read pois from source
        query = f"SELECT * FROM {save_schema}.{nodes_save_table} WHERE \"source\" = \'{source}\'"
        nodes_source = aup.gdf_from_query(query, geometry_col='geometry')

        aup.log(f"--- Loaded {len(nodes_source)} nodes from source {source}.")

        # Translate source to column name
        nodes_source.rename(columns={'source_time':f'{source}_time'}, inplace=True) 
        nodes_source.rename(columns={'source_15min':f'{source}_count_15min'}, inplace=True) 

        # Filter nodes gdf
        nodes_source = nodes_source[['osmid', f'{source}_time', f'{source}_count_15min']]

        # Merge to nodes analysis
        nodes_analysis = nodes_analysis.merge(nodes_source, on='osmid', how='left')
        
        aup.log(f"--- Appended {len(nodes_source)} nodes to nodes analysis.")
        del nodes_source
    
    nodes_analysis['city'] = 'Santiago'

    # Assign values to hex_gdf
    for r in hex_gdf.res.unqiue():
        hex_tmp = hex_gdf[hex_gdf.res == r].copy()

        hex_tmp = aup.group_by_hex_mean(nodes_analysis, hex_tmp, r, source_list)
        hex_tmp = hex_tmp.drop(columns=['res','geometry'])

        # Merge to hex_gdf
        hex_gdf = hex_gdf.merge(hex_tmp, on='hex_id', how='left')

        aup.log(f"--- Merged {len(hex_tmp)} hexagons to hex_gdf.")

        del hex_tmp
    
    hex_gdf['city'] = 'Santiago'
        
    # 1.1f) Save output
    aup.log(f"--- Saving nodes proximity to {source}.")
    if save:
        aup.gdf_to_db_slow(nodes_analysis, nodes_save_table, save_schema, if_exists='append')
        aup.log(f"--- Saved nodes proximity in database.")
        aup.gdf_to_db_slow(hex_gdf, nodes_save_table, save_schema, if_exists='append')
        aup.log(f"--- Saved hexagons proximity in database.")

    if local_save:
        nodes_analysis.to_file(nodes_local_save_dir, driver='GPKG')
        aup.log(f"--- Saved nodes proximity to {source} locally.")
        
    if save_space:
        del nodes_analysis

    i+=1

    ############################################################### PART 2 ###############################################################
    ######################################################### AMENITIES ANALYSIS #########################################################
    ############################################################## (LATER?) ##############################################################

if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('--- STARTING SCRIPT 23.')

    # ------------------------------ BASE DATA REQUIRED ------------------------------
    

    # List of pois to be examined.
    # This list should contain the source_name that will be assigned to each processed poi.
    # That source_name will be stored in a 'source' column at first and be turned into a column name after all pois are processed.
    # That source_name must also be the name of the file stored in gral_dir (.gpkg)
    # source_list = ['vacunatorio_pub']
    # create source_dict to store index and source_name
    # civic_office, social_security
    source_list = ['carniceria','hogar','local_mini_market',
                   'supermercado','clinica_priv','clinica_pub',
                   'hospital_priv','hospital_pub','farmacia',
                   'consult_ado_priv','consult_ado_pub',
                   'club_deportivo','eq_deportivo_pub','eq_deportivo_priv',
                   'tax_collection','feria','ep_plaza_big',
                   'banco','museos_priv','museos_pub','sitios_historicos',
                   'cines','restaurantes_bar_cafe','librerias','edu_basica_priv',
                   'edu_basica_pub','edu_media_priv','edu_media_pub',
                   'jardin_inf_priv','jardin_inf_pub','edu_especial_priv',
                   'edu_especial_pub','bibliotecas']

    # Pois proximity methodology - Count pois at a given time proximity?
    count_pois = (True,15)

    # walking_speed (float): Decimal number containing walking speed (in km/hr) to be used if prox_measure="length",
	#						 or if prox_measure="time_min" but needing to fill time_min NaNs.
    walking_speed = [4.5]
    # WARNING: Make sure to change nodes_save_table to name {santiago_nodesproximity_n_n_kmh}, where n_n is walking_speed.
    # e.g. 3.5km/hr --> 'santiago_nodesproximity_3_5_kmh'

    # Area of interest (Run 'AM_Santiago', it represents Santiago's metropolitan area. We can clip data as soon as we know inputs extent.)
    city = 'AM_Santiago'

    # Save space in disk by deleting data that won't be used again?
    save_space = True

    ##### WARNING ##### WARNING ##### WARNING #####
    save = False # save output to database?
    local_save = False # save output to local? (Make sure directory exists)
    nodes_local_save_dir = f"../data/processed/santiago/test_script23_nodes.gpkg"
    ##### WARNING ##### WARNING ##### WARNING #####

    # ------------------------------ SCRIPT CONFIGURATION - DATABASE SCHEMAS AND TABLES ------------------------------

    # Area of interest (aoi)
    aoi_schema = 'projects_research'
    aoi_table = 'santiago_aoi'
    # OSMnx Network
    network_schema = 'projects_research'
    edges_table = 'santiago_edges'
    nodes_table = 'santiago_nodes'
    projected_crs = 'EPSG:32719'
    # Save output to db
    save_schema = 'projects_research'

    # 0.0 --------------- BASE DATA FOR POIS-NODES ANALYSIS
    # ------------------- This step downloads the area of interest and network used to measure distance.

    # Area of interest (aoi)
    aup.log("--- Downloading area of interest.")
    query = f"SELECT * FROM {aoi_schema}.{aoi_table} WHERE \"city\" LIKE \'{city}\'"
    aoi = aup.gdf_from_query(query, geometry_col='geometry')
    aoi = aoi.set_crs("EPSG:4326")

    # Create hexgrid
    hex_gdf = gpd.GeoDataFrame()

    for r in range(8,10):
        hex_tmp = aup.create_hexgrid(aoi, r)
        hex_tmp.rename(columns={f'hex_id_{r}':'hex_id'}, inplace=True)
        hex_tmp['res'] = r

        hex_gdf = pd.concat([hex_gdf, hex_tmp], 
                ignore_index = True, axis = 0)
        
        del hex_tmp

    # OSMnx Network
    aup.log("--- Downloading network.")
    _, nodes, _ = aup.graph_from_hippo(aoi, network_schema, edges_table, nodes_table, projected_crs)

    # add length data to edges
    # edges['length'] = edges.to_crs(projected_crs).length

    for walk_speed in walking_speed:
        str_walk_speed = str(walk_speed).replace('.','_')
        nodes_save_table = f'santiago_nodesproximity_{str_walk_speed}_kmh'
        source_speed_list = source_list.copy()
    
        # general pois local dir
        gral_dir = f'../data/processed/00_pois_formated/'

        aup.log(f"--- Running script for speed: {walk_speed}.")
        # ------------------------------ SCRIPT START ------------------------------
            
        # If passed source check, proceed to main function
        aup.log(f"--- Running Script for verified sources.")
        main(source_speed_list, hex_gdf, nodes, nodes_save_table, save_schema, local_save, save)