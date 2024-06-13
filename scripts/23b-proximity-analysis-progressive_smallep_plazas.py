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

def main(source_list, aoi, nodes, edges, G, walking_speed, local_save=False, save=False):
    aup.log("--"*40)
    aup.log(f"--- STARTING MAIN FUNCTION.")
    
    ############################################################### PART 1 ###############################################################
    #################################################### FIND NODES PROXIMITY TO POIS ####################################################
    
    # 1.1 --------------- NODES PROXIMITY TO POIS
    # ------------------- This step loads each source of interest, calculates nodes proximity and saves to database
    k = len(source_list)
    i = 1

    for source in source_list:
        aup.log('--'*40)
        aup.log(f"--- STARTING nodes proximity to pois using speed {walking_speed}km/hr for source {i}/{k}: {source}.")

        # 1.1a) Read pois from pois dir
        aup.log(f"--- Reading pois dir.")
        # Directory where pois to be examined are located
        pois_dir = gral_dir + f'{source}.gpkg'
        # Load all pois from directory
        pois = gpd.read_file(pois_dir)
        # Format
        pois = pois[['area_ha','ID','geometry']]
        pois = pois.set_crs("EPSG:4326")

        # 1.1b) Clip pois to aoi
        source_pois = gpd.clip(pois, aoi)
        source_pois = source_pois[['area_ha','ID','geometry']]
        aup.log(f"--- Keeping {len(source_pois)} pois inside aoi from original {len(pois)} pois.")

        if save_space:
            del pois

        # 1.1c) Calculate nodes proximity (Use both function pois_time() AND function id_pois_time(), then merge results)

        # pois_time() [for public spaces below 2000m2]
        # For very small public spaces, the proximity analysis can consider any poi derived from the geometry of interest (goi, polygon) because it is small.
        # Because we just care about one poi only (any), filter and drop duplicate IDs, keeping the first occurrence.
        very_small_source_pois = source_pois.loc[source_pois['area_ha']<0.2].copy().drop_duplicates(subset='ID')
        # Calculate time data from nodes to source for very_small_source_pois (Has 1 pois for each goi)
        aup.log(f"--- Calculating nodes proximity with function pois_time().")
        source_nodes_time_1 = aup.pois_time(G, nodes, edges, very_small_source_pois, source,'length',
                                            walking_speed, count_pois, projected_crs)
        if save_space:
            del very_small_source_pois

        if local_save:
            source_nodes_time_1_dir = f"../data/processed/santiago/source_nodes_time_1.gpkg"
            source_nodes_time_1.to_file(source_nodes_time_1_dir, driver='GPKG')
            aup.log(f"--- Saved source_nodes_time 1 locally.")
            
        # id_pois_time() [for public spaces above 2000m2]
        # For larger public spaces, having several accesses becomes relevant, and goi IDs necessary.
        small_source_pois = source_pois.loc[source_pois['area_ha']>=0.2].copy()
        # Calculate time data from nodes to source for small_source_pois (Has n pois for each goi, needs goi_id)
        aup.log(f"--- Calculating nodes proximity with function id_pois_time().")
        source_nodes_time_2 = aup.id_pois_time(G, nodes, edges, small_source_pois, source,'length',
                                               walking_speed, goi_id='ID', count_pois=count_pois, projected_crs=projected_crs)
        if save_space:
            del source_pois
            del small_source_pois

        if local_save:
            source_nodes_time_2_dir = f"../data/processed/santiago/source_nodes_time_2.gpkg"
            source_nodes_time_2.to_file(source_nodes_time_2_dir, driver='GPKG')
            aup.log(f"--- Saved source_nodes_time 2 locally.")

        # Merge source_nodes_time_1 results with source_nodes_time_2 results
        source_nodes_time_all = source_nodes_time_1.merge(source_nodes_time_2[['osmid', 'time_'+source, f'{source}_{count_pois[1]}min']],on='osmid')
        
        # Find min time between both source_nodes_time
        time_cols = [f'time_{source}_x', f'time_{source}_y']
        source_nodes_time_all[f'time_{source}'] = source_nodes_time_all[time_cols].min(axis=1)
        source_nodes_time_all.drop(columns=time_cols,inplace=True)

        # Find sum of counted pois at {count_pois[1]} distance (minutes) for both source_nodes_time
        count_cols = [f'{source}_{count_pois[1]}min_x',f'{source}_{count_pois[1]}min_y']
        source_nodes_time_all[f'{source}_{count_pois[1]}min'] = source_nodes_time_all[count_cols].sum(axis=1)
        source_nodes_time_all.drop(columns=count_cols,inplace=True)
        
        # 1.1d) Nodes_analysis format
        source_nodes_time_all.rename(columns={'time_'+source:source},inplace=True)
        nodes_analysis = source_nodes_time_all.copy()

        if save_space:
            del source_nodes_time_2

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
            nodes_analysis.rename(columns={f'{source}_{count_pois[1]}min':f'source_{count_pois[1]}min'},inplace=True)
        # Set column order
        if count_pois[0]:
            nodes_analysis = nodes_analysis[['osmid','source','source_time',f'source_{count_pois[1]}min','x','y','geometry']]
        else:
            nodes_analysis = nodes_analysis[['osmid','source','source_time','x','y','geometry']]
        # Add city data
        nodes_analysis['city'] = city

        # 1.1f) Save output
        aup.log(f"--- Saving nodes proximity to {source}.")
        if save:
            nodes_analysis['source_15min'] = nodes_analysis['source_15min'].astype(int)
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
    aup.log('--- STARTING SCRIPT 23b [With UNIQUE IDs specifically for small public spaces and squares].')

    # ------------------------------ BASE DATA REQUIRED ------------------------------
    # general pois local dir
    gral_dir = '../data/external/temporal_fromjupyter/santiago/pois/'

    # List of pois to be examined.
    # This list should contain the source_name that will be assigned to each processed poi.
    # That source_name will be stored in a 'source' column at first and be turned into a column name after all pois are processed.
    # That source_name must also be the name of the file stored in gral_dir (.gpkg)
    source_list = ['ep_plaza_small']

    # Pois proximity methodology - Count pois at a given time proximity?
    count_pois = (True,15)

    # walking_speed (float): Decimal number containing walking speed (in km/hr) to be used if prox_measure="length",
	#						 or if prox_measure="time_min" but needing to fill time_min NaNs.
    walking_speed_list = [4.5] #[3.5,4.5,5,12,24,20,40]

    # Area of interest (Run 'AM_Santiago', it represents Santiago's metropolitan area. We can clip data as soon as we know inputs extent.)
    city = 'AM_Santiago'

    # goi_id (str): Text containing name of column with unique ID for the geometry of interest from which pois where created.
    goi_id = 'ID'

    # Save space in disk by deleting data that won't be used again?
    save_space = True

    ##### WARNING ##### WARNING ##### WARNING #####
    save = True # save output to database?
    local_save = True # save output to local? (Make sure directory exists) #RECOMMENDED IN CASE SCRIPT FAILS
    nodes_local_save_dir = f"../data/processed/santiago/santiago_nodesproximity_ep_plaza_small.gpkg"
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

    # ------------------------------ SCRIPT START ------------------------------

    # 0.0 --------------- BASE DATA FOR POIS-NODES ANALYSIS
    # ------------------- This step downloads the area of interest and network used to measure distance.

    # Area of interest (aoi)
    aup.log("--- Downloading area of interest.")
    query = f"SELECT * FROM {aoi_schema}.{aoi_table} WHERE \"city\" LIKE \'{city}\'"
    aoi = aup.gdf_from_query(query, geometry_col='geometry')
    aoi = aoi.set_crs("EPSG:4326")

    # OSMnx Network
    aup.log("--- Downloading network.")
    G, nodes, edges = aup.graph_from_hippo(aoi, network_schema, edges_table, nodes_table, projected_crs)

    for walking_speed in walking_speed_list:
        aup.log('--'*45)
        aup.log(f"--- Running Script [Modified] for ep_plaza_small for speed = {walking_speed}km/hr.")
        str_walk_speed = str(walking_speed).replace('.','_')
        nodes_save_table = f'santiago_nodesproximity_{str_walk_speed}_kmh'

        if save:
            # Saved sources check (prevents us from uploading same source twice/errors on source list)
            aup.log(f"--- Verifying sources by comparing to data already uploaded.")
            try:
                # Load sources already processed
                query = f"SELECT DISTINCT source FROM {save_schema}.{nodes_save_table}"
                saved_data = aup.df_from_query(query)
                saved_sources = list(saved_data.source.unique())
            except:
                saved_sources = []
                aup.log(f"--- No data found for {nodes_save_table}.")
            
            # Verify current source list
            for source in source_list:
                if source in saved_sources:
                    aup.log(f"--- ERROR: Source {source} already processed and in database.")
                    aup.log(f"--- Remove source from dataset or change source_name before continuing.")
                    break
                    # intended_crash
            
        # If passed source check, proceed to main function
        aup.log(f"--- Running Script for verified sources.")
        main(source_list, aoi, nodes, edges, G, walking_speed, local_save, save)