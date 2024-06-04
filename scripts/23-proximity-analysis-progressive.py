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

def main(source_list, local_save=False, save=False):
    aup.log("--"*40)
    aup.log(f"--- STARTING MAIN FUNCTION.")
    
    ############################################################### PART 1 ###############################################################
    #################################################### FIND NODES PROXIMITY TO POIS ####################################################
    
    # 1.1 --------------- BASE DATA FOR POIS-NODES ANALYSIS
    # ------------------- This step downloads the area of interest and network used to measure distance.

    # Area of interest (aoi)
    aup.log("--- Downloading area of interest.")
    query = f"SELECT * FROM {aoi_schema}.{aoi_table} WHERE \"city\" LIKE \'{city}\'"
    aoi = aup.gdf_from_query(query, geometry_col='geometry')
    aoi = aoi.set_crs("EPSG:4326")

    # OSMnx Network
    aup.log("--- Downloading network.")
    G, nodes, edges = aup.graph_from_hippo(aoi, network_schema, edges_table, nodes_table, projected_crs)
    
    # 1.1 --------------- NODES PROXIMITY TO POIS
    # ------------------- This step loads each source of interest, calculates nodes proximity and saves to database
    k = len(source_list)
    i = 1

    for source in source_list:

        aup.log(f"--- Starting nodes proximity to pois for source {i}/{k}: {source}. ")

        # 1.1a) Read pois from pois dir
        aup.log(f"--- Reading pois dir.")
        # Directory where pois to be examined are located
        pois_dir = gral_dir + f'{source}.gpkg'
        # Load all pois from directory
        pois = gpd.read_file(pois_dir)
        # Set code column
        pois['code'] = source
        # Format
        pois = pois[['code','geometry']]
        pois = pois.set_crs("EPSG:4326")

        # 1.1b) Clip pois to aoi
        source_pois = gpd.sjoin(pois, aoi)
        source_pois = source_pois[['code','geometry']]
        aup.log(f"--- Keeping {len(source_pois)} pois inside aoi from original {len(pois)} pois.")

        if save_space:
            del pois

        # 1.1c) Calculate nodes proximity (Function pois_time())
        aup.log(f"--- Calculating nodes proximity.")
        # Calculate time data from nodes to source
        source_nodes_time = aup.pois_time(G, nodes, edges, source_pois, source, prox_measure='length', count_pois=count_pois, projected_crs=projected_crs)
        source_nodes_time.rename(columns={'time_'+source:source},inplace=True)
        nodes_analysis = source_nodes_time.copy()

        if save_space:
            del source_nodes_time

        # 1.1d) Nodes_analysis format
        # if count_pois, include generated col
        if count_pois[0]:
            column_order = ['osmid'] + [source, f'{source}_{count_pois[1]}min'] + ['x','y','geometry']
        else:
            column_order = ['osmid'] + [source] + ['x','y','geometry']
        nodes_analysis = nodes_analysis[column_order]

        # 1.1e) Tidy data format (Allows loop-upload)
        aup.log(f"--- Reordering datased as tidy data format.")
        # Add source column to be able to extract source proximity data. Fill with current source.
        nodes_analysis['source'] = source
        # Rename source-specific column names as name that apply to all sources (source_time, source_15min)
        nodes_analysis.rename(columns={source:'source_time'},inplace=True)
        if count_pois[0]:
            nodes_analysis.rename(columns={f'{source}_{count_pois[1]}min':'source_15min'},inplace=True)
        # Set column order
        if count_pois[0]:
            nodes_analysis = nodes_analysis[['osmid','source','source_time','source_15min','x','y','geometry']]
        else:
            nodes_analysis = nodes_analysis[['osmid','source','source_time','x','y','geometry']]
        # Add city data
        nodes_analysis['city'] = city

        # 1.1f) Save output
        aup.log(f"--- Saving nodes proximity to {source}.")
        if save:
            aup.gdf_to_db_slow(nodes_analysis, nodes_save_table, save_schema, if_exists='append')
            aup.log(f"--- Saved nodes proximity to {source} in database.")

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
    # general pois local dir
    gral_dir = '../data/external/temporal_fromjupyter/santiago/pois/'

    # List of pois to be examined.
    # This list should contain the source_name that will be assigned to each processed poi.
    # That source_name will be stored in a 'source' column at first and be turned into a column name after all pois are processed.
    # That source_name must also be the name of the file stored in gral_dir (.gpkg)
    source_list = ['vacunatorio_pub']

    # Pois proximity methodology - Count pois at a given time proximity?
    count_pois = (True,15)

    # Area of interest (Run 'AM_Santiago', it represents Santiago's metropolitan area. We can clip data as soon as we know inputs extent.)
    city = 'AM_Santiago'

    # Save space in disk by deleting data that won't be used again?
    save_space = True

    ##### WARNING ##### WARNING ##### WARNING #####
    save = False # save output to database?
    local_save = True # save output to local? (Make sure directory exists)
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
    nodes_save_table = 'santiago_nodesproximity'

    # ------------------------------ SCRIPT START ------------------------------

    if save:
        # Saved sources check (prevents us from uploading same source twice/errors on source list)
        aup.log(f"--- Verifying sources by comparing to data already uploaded.")

        # Load sources already processed
        query = f"SELECT DISTINCT source FROM {save_schema}.{nodes_save_table}"
        saved_data = aup.df_from_query(query)
        saved_sources = list(saved_data.source.unique())
        
        # Verify current source list
        for source in source_list:
            if source in saved_sources:
                aup.log(f"--- ERROR: Source {source} already processed and in database.")
                aup.log(f"--- Remove source from dataset or change source_name before continuing.")
                intended_crash
        
    # If passed source check, proceed to main function
    aup.log(f"--- Running Script for verified sources.")
    main(source_list, local_save, save)