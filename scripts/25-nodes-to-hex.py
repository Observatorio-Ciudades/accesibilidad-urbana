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

def calculate_hqsl(hex_gdf):
    #parameters_dict = {social_functions:{themes:[sources]}}
    parameters_dict = {'supplies':{'groceries':['carniceria','hogar','local_mini_market','bakeries','ferias','supermercado']},
                   'caring':{'health':['clinica_priv','clinica_pub','hospital_priv','hospital_pub','consult_ado_priv','consult_ado_pub','residencia_adumayor','farmacia'],
                            'exercise':['club_deportivo','eq_deportivo_pub','eq_deportivo_priv']},
                   'living':{'errands_paperwork':['civic_office','tax_collection','social_security','banco'],
                             'fresh_air':['ep_plaza_small','ep_plaza_big']},
                   'enjoying':{'culture':['museos_priv','museos_pub','bibliotecas','sitios_historicos'],
                               'entertainment':['cines','librerias','restaurantes_bar_cafe']},
                   'learning':{'education':['edu_basica_priv','edu_basica_pub','edu_media_priv','edu_media_pub','jardin_inf_pub','jardin_inf_priv','edu_especial_priv','edu_especial_pub']},
                   'working':{'sustainable_mobility':['ciclovias','paradas_tp','paradas_tp_tren','paradas_tp_metro']}}
    
    # scale count source values
    # proxhexs_countprocess = hex_gdf.copy()
    # scalar_count_column_list = []

    for social_function in parameters_dict.keys():
        aup.log(f"--- {social_function}")
        for theme in parameters_dict[social_function]:
            aup.log(f"------ {theme}")
            for source in parameters_dict[social_function][theme]:

                # Set col name of interest and find min and max values
                count_colname = f"{source}_count_15min"
                min_val = hex_gdf[count_colname].min()
                max_val = hex_gdf[count_colname].max()
                # Calculate MinMax Scalar
                hex_gdf[f"{source}_scaledcount"] = hex_gdf[count_colname].apply(lambda x: ((x - min_val) /(max_val - min_val)))
                aup.log(f"------ Scaled {source} count.")
                ''' # Drop original count col
                hex_gdf.drop(columns=[count_colname],inplace=True)
                # Add
                scalar_count_column_list.append(f"{source}_scaledcount")'''

    # Keep columns of interest only
    # proxhexs_countprocess = proxhexs_countprocess[['hex_id','geometry']+scalar_count_column_list+['res','city']]
    aup.log(f"--- Scaled count columns added to hex_gdf.")
    aup.log(f"--- Starting social function analysis.")
    sum_count_column_list = []

    for social_function in parameters_dict.keys():
        # Set social function sources list
        sf_sources_list = []
        
        for theme in parameters_dict[social_function]:
            # Set theme_sources_list and feed sf_sources_list
            theme_sources_list = []
            for source in parameters_dict[social_function][theme]:
                theme_sources_list.append(f"{source}_scaledcount")
                sf_sources_list.append(f"{source}_scaledcount")

            
            # Find sum of count anlysis for theme
            hex_gdf[f"{theme}_count"] = hex_gdf[theme_sources_list].sum(axis=1)
            aup.log(f"------ Summed {theme} count with a mean value of " + str(round(hex_gdf[f"{theme}_count"].mean(),4)))
            sum_count_column_list.append(f"{theme}_count")
            
        # Find sum of count anlysis for social function
        hex_gdf[f"{social_function}_count"] = hex_gdf[sf_sources_list].sum(axis=1)
        aup.log(f"--- Summed {social_function} count with a mean value of {round(hex_gdf[f'{social_function}_count'].mean(),4)}.")
        sum_count_column_list.append(f"{social_function}_count")

    social_fn_cols = []
    for k in parameters_dict.keys():
        social_fn_cols.append(k+'_count')
    
    hex_gdf['hqsl'] = hex_gdf[social_fn_cols[0]] + hex_gdf[social_fn_cols[1]] + hex_gdf[social_fn_cols[2]] + hex_gdf[social_fn_cols[3]] + hex_gdf[social_fn_cols[4]] + hex_gdf[social_fn_cols[5]]
    aup.log(f"--- Calculated HQSL with a mean value of {round(hex_gdf['hqsl'].mean(),4)}.")
        

    return hex_gdf


def main(source_list, hex_gdf, nodes, nodes_save_table, save_schema, str_walk_speed, local_save_dir=None, local_save=False, save=False):
    
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

    source_cols = []

    ###

    try:

        # Donwload previously processed nodes proximity
        aup.log(f"--- Downloading nodes proximity from database.")
        nodes_analysis = aup.gdf_from_db(f'santiago_nodesproximity_format_{str_walk_speed}_kmh', save_schema)
        aup.log(f"--- Loaded {len(nodes_analysis)} nodes proximity from database.")
    
    except:
        aup.log(f"--- No nodes proximity found in database. Starting from scratch.")
    
    # Merge to nodes analysis
    # nodes_analysis = nodes_analysis.merge(nodex_prox, on='osmid', how='left')
    # aup.log(f"--- Merged nodes proximity to nodes analysis.")

    # del nodex_prox
    
    ###
    
    for source in source_list:

        if f'{source}_time' in nodes_analysis.columns:
            aup.log(f"--- Source {source} already processed. Skipping.")
            source_cols.append(f'{source}_time')
            source_cols.append(f'{source}_count_15min')
            i += 1
            continue

        aup.log(f"--- Starting nodes proximity to pois for source {i}/{k}: {source}. ")
        # Read pois from source
        query = f"SELECT * FROM {save_schema}.{nodes_save_table} WHERE \"source\" = \'{source}\'"
        nodes_source = aup.gdf_from_query(query, geometry_col='geometry')

        aup.log(f"--- Loaded {len(nodes_source)} nodes from source {source}.")

        # Translate source to column name
        nodes_source.rename(columns={'source_time':f'{source}_time'}, inplace=True) 
        nodes_source.rename(columns={'source_15min':f'{source}_count_15min'}, inplace=True)
        source_cols.append(f'{source}_time')
        source_cols.append(f'{source}_count_15min')

        # Filter nodes gdf
        nodes_source = nodes_source[['osmid', f'{source}_time', f'{source}_count_15min']]

        # Merge to nodes analysis
        nodes_analysis = nodes_analysis.merge(nodes_source, on='osmid', how='left')
        
        aup.log(f"--- Appended {len(nodes_source)} nodes to nodes analysis.")
        del nodes_source

        i += 1

    
    nodes_processed_table = f'santiago_nodesproximity_format_{str_walk_speed}_kmh'

    # Save nodes proximity
    if save:
        aup.gdf_to_db_slow(nodes_analysis, nodes_processed_table, save_schema, if_exists='replace')
        aup.log(f"--- Saved nodes proximity in database.")

    # Assign values to hex_gdf
    hex_bins = gpd.GeoDataFrame()

    for r in hex_gdf.res.unique():

        aup.log(f"--- Calculating mean proximity for hexagons at resolution {r}.")

        hex_tmp = hex_gdf[hex_gdf.res == r].copy()

        hex_tmp = aup.group_by_hex_mean(nodes_analysis, hex_tmp, r, source_cols, 'hex_id')

        aup.log(f"--- Calculated mean proximity for {len(hex_tmp)} hexagons at resolution {r}.")

        hex_tmp = hex_tmp.drop(columns=['res_x','res_y'])
        hex_tmp['res'] = r

        # Merge to hex_gdf
        # hex_gdf = hex_gdf.merge(hex_tmp, on='hex_id', how='left')

        hex_bins = pd.concat([hex_bins, hex_tmp], 
                ignore_index = True, axis = 0)

        aup.log(f"--- Merged {len(hex_tmp)} hexagons to hex_gdf.")

        del hex_tmp

    hex_bins = hex_bins.set_geometry('geometry')
    hex_bins = hex_bins.set_crs("EPSG:4326")
    
    hex_bins['city'] = 'Santiago'
    nodes_analysis['city'] = 'Santiago'

    # hex_bins = calculate_hqsl(hex_bins)

        
    # 1.1f) Save output
    aup.log(f"--- Saving nodes and hex proximity.")

    hex_processed_table = f'santiago_hexproximity_{str_walk_speed}_kmh'

    if local_save:
        nodes_analysis.to_file(local_save_dir + nodes_processed_table, driver='GPKG')
        aup.log(f"--- Saved nodes proximity locally.")

        hex_bins.to_file(local_save_dir + hex_processed_table, driver='GPKG')
        aup.log(f"--- Saved hexagons proximity locally.")

    if save:
        aup.gdf_to_db_slow(hex_bins, hex_processed_table, save_schema, if_exists='replace')
        aup.log(f"--- Saved hexagons proximity in database.")
  
    if save_space:
        del nodes_analysis


    ############################################################### PART 2 ###############################################################
    ######################################################### AMENITIES ANALYSIS #########################################################
    ############################################################## (LATER?) ##############################################################

if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('--- STARTING SCRIPT 25.')

    # ------------------------------ BASE DATA REQUIRED ------------------------------
    

    # List of pois to be examined.
    # This list should contain the source_name that will be assigned to each processed poi.
    # That source_name will be stored in a 'source' column at first and be turned into a column name after all pois are processed.
    # That source_name must also be the name of the file stored in gral_dir (.gpkg)
    # source_list = ['vacunatorio_pub']
    # create source_dict to store index and source_name
    # civic_office, social_security
    source_list = ['supermercado','clinica_priv','clinica_pub',
                   'hospital_priv','hospital_pub',
                   'consult_ado_priv','consult_ado_pub',
                   'club_deportivo','eq_deportivo_pub','eq_deportivo_priv',
                   'tax_collection','civic_office','social_security',
                   'museos_priv','museos_pub','sitios_historicos',
                   'cines','edu_basica_priv',
                   'edu_basica_pub','edu_media_priv','edu_media_pub',
                   'jardin_inf_priv','jardin_inf_pub','edu_especial_priv',
                   'edu_especial_pub','bibliotecas','agua_alcantarillado',
                   'residencia_adumayor','paradas_tp','paradas_tp_tren',
                   'paradas_tp_metro', 'banco','carniceria','farmacia',
                   'hogar', 'librerias','local_mini_market','bakeries',
                   'restaurantes_bar_cafe', 'universidad', 'edu_tecnica',
                   'edu_adultos_priv','edu_adultos_pub','centro_edu_amb',
                   'centro_recyc', 'labs_priv', 'salud_mental', 'bomberos',
                   'correos', 'police', 'vacunatorio_pub', 'vacunatorio_priv','ferias',
                   'ep_plaza_small','ep_plaza_big','ciclovias','eleam']
    # source_list = ['ferias','ep_plaza_small','ep_plaza_big','ciclovias']

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
    save = True # save output to database?
    local_save = False # save output to local? (Make sure directory exists)
    local_save_dir = f"../data/processed/santiago/"
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

    for r in range(8,11):
        hex_tmp = aup.create_hexgrid(aoi, r)
        hex_tmp.rename(columns={f'hex_id_{r}':'hex_id'}, inplace=True)
        hex_tmp['res'] = r

        aup.log(f"--- Created {len(hex_tmp)} hexagons at resolution {r}.")

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
        main(source_speed_list, hex_gdf, nodes, nodes_save_table, save_schema, str_walk_speed, local_save_dir, local_save, save)