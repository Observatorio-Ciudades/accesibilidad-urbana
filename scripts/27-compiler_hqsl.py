import geopandas as gpd
import pandas as pd
import numpy as np

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from shapely.geometry import Point
import osmnx as ox


import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


def create_filtered_navigable_network(public_space_quality_dir, projected_crs, filtering_column, filtering_value):

    # 1.0 --------------- LOAD DATA
    # ------------------- This step loads the public space quality index gdf for the current project
    # Load data
    pub_space_qty = gpd.read_file(public_space_quality_dir)
    # Set CRS
    pub_space_qty = pub_space_qty.set_crs(projected_crs)
    # Filter for data of relevance
    gdf = pub_space_qty[[filtering_column,'geometry']].copy()

    # 2.0 --------------- EXTRACT VERTICES
    # ------------------- This step extracts points from each linestring and stores them in gdf_points.
    # Explode multi-part geometries into single parts
    gdf_exploded = gdf.explode(index_parts=False)
    # Reset index
    gdf_exploded.reset_index(inplace=True)
    gdf_exploded.drop(columns=['index'],inplace=True)
    #Initialize an empty list to store the points and its values
    points = []
    attributes = []
    #Iterate through each LineString and extract its vertices
    for idx, row in gdf_exploded.iterrows():
        line = row.geometry
        for coord in line.coords:
            points.append(Point(coord))
            attributes.append(row[filtering_column])
    # Create a new GeoDataFrame from the points
    gdf_points = gpd.GeoDataFrame(attributes,geometry=points)
    # Rename data
    gdf_points.rename(columns={0:filtering_column},inplace=True)

    # 3.0 --------------- CREATE NODES AND EDGES COMPATIBLE WITH OSMnx AND FILTER THEM.
    # ------------------- This step uses the lines and points available to create nodes and edges, then filters by filtering value.
    # Create nodes and edges
    nodes = gdf_points.copy()
    edges = gdf_exploded.copy()
    nodes = nodes.set_crs("EPSG:4326")
    edges = edges.set_crs("EPSG:4326")
    nodes, edges = aup.create_network(nodes, edges, projected_crs)
    # Filter them
    edges_filt = edges.loc[edges[filtering_column] >= filtering_value]

    # 4.0 --------------- CREATE NAVIGABLE NETWORK
    # ------------------- This step creates G from the previous nodes and edges_filt.
    # Format nodes and edges
    nodes_gdf = nodes.copy()
    nodes_gdf.set_index('osmid',inplace=True)
    edges_gdf = edges_filt.copy()
    edges_gdf.set_index(['u','v','key'],inplace=True)
    # Set x and y columns
    nodes_gdf['x'] = nodes_gdf['geometry'].x
    nodes_gdf['y'] = nodes_gdf['geometry'].y
    # Create network G
    G = ox.graph_from_gdfs(nodes_gdf, edges_gdf)

    return G, nodes_gdf, edges_gdf


def main(source_list, aoi, nodes, edges, G, walking_speed, local_save, save):
    
    # 1.1 --------------- NODES PROXIMITY TO POIS
    # ------------------- This step loads each source of interest, calculates nodes proximity and saves to database
    k = len(source_list)-len(special_sources)
    i = 1

    for source in source_list:

        # ----------
        # UNIQUE ID CONSIDERATION
        # Check if current source has a unique ID that needs to be considered in the process
        if source in unique_id_sources:
            unique_id = True
        # Check if current source needs special consideration (Another function applies)
        elif source in special_sources:
            continue
        else:
            unique_id=False
        # ----------

        aup.log("--"*40)
        aup.log(f"--- Starting nodes proximity to pois using speed {walking_speed}km/hr for source {i}/{k}: {source}. ")

        # 1.1a) Read pois from pois dir
        aup.log(f"--- Reading pois dir.")
        # Directory where pois to be examined are located
        pois_dir = gral_dir + f'{source}.gpkg'
        # Load all pois from directory
        pois = gpd.read_file(pois_dir)

        # ----------
        # UNIQUE ID CONSIDERATION
        if unique_id:
            # Keep already existing unique ID and geometry
            pois = pois[['ID','geometry']]
        else:
            # Set ID col as source name and keep geometry
            pois['ID'] = source
            pois = pois[['ID','geometry']]
        # ----------

        # Format
        if pois.crs:
            pois = pois.to_crs("EPSG:4326")
        else:
            pois = pois.set_crs("EPSG:4326")

        # 1.1b) Clip pois to aoi
        source_pois = gpd.sjoin(pois, aoi)
        source_pois = source_pois[['ID','geometry']]
        aup.log(f"--- Keeping {len(source_pois)} pois inside aoi from original {len(pois)} pois.")

        if save_space:
            del pois

        # 1.1c) Calculate nodes proximity (Function pois_time())
        aup.log(f"--- Calculating nodes proximity.")

        # ----------
        # UNIQUE ID CONSIDERATION
        if unique_id:
            # Function id_pois_time() consideres the unique ID belonging to each geometry of interest (goi).
            source_nodes_time = aup.id_pois_time(G, nodes, edges, source_pois, source, 'length', walking_speed, 
                                                 goi_id='ID', count_pois=count_pois, projected_crs=projected_crs)
        else:
            # Function pois_time() calculates proximity data from nodes to source (all) without considering any unique ID.
            source_nodes_time = aup.pois_time(G, nodes, edges, source_pois, source,'length',walking_speed, 
                                              count_pois, projected_crs)
        # ----------

        # 1.1d) Nodes_analysis format
        source_nodes_time.rename(columns={'time_'+source:source},inplace=True)
        nodes_analysis = source_nodes_time.copy()

        if save_space:
            del source_pois
            del source_nodes_time

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
        nodes_analysis[f'source_{count_pois[1]}min'] = nodes_analysis[f'source_{count_pois[1]}min'].astype(int)
        
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

if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('--- STARTING SCRIPT 27.')

    ########################################################## DATA FOR PART 1 ###########################################################
    ############################################## NAVIGABLE NETWORK AND PROXIMITY DATA ##################################################

    # ------------------------------ BASE DATA REQUIRED ------------------------------
    
    # --------------- LIST OF POIS TO BE EXAMINED
    # This list should contain the source_name that will be assigned to each processed poi.
    # That source_name will be stored in a 'source' column at first and be turned into a column name after all pois are processed.
    # That source_name must also be the name of the file stored in gral_dir (.gpkg)
    # e.g if source_list = ['vacunatorio_pub'], vacanatorio_pub.gpkg must exist.

    source_list = ['carniceria','hogar','bakeries','supermercado','banco', #supplying-wellbeing
                   #supplying-sociability
                   'ferias','local_mini_market','correos', 
                   #supplying-environmental impact
                   'centro_recyc',

                   #caring-wellbeing
                   'hospital_priv','hospital_pub','clinica_priv','clinica_pub','farmacia','vacunatorio_priv','vacunatorio_pub','consult_ado_priv','consult_ado_pub','salud_mental','labs_priv','residencia_adumayor',
                   #caring-sociability
                   'eq_deportivo_priv','eq_deportivo_pub','club_deportivo',
                   #caring-environmental impact [areal data: 'noise','temp']

                   #living-wellbeing
                   'civic_office','tax_collection','social_security','police','bomberos',
                   #living-sociability [areal data: 'houses','social_viv','hotel']
                   #living-environmental impact [areal_data: 'inter']
                   
                   #enjoying-wellbeing [areal data: 'ndvi']
                   'museos_priv','museos_pub','cines','sitios_historicos',
                   #enjoying-sociability
                   'restaurantes_bar_cafe','librerias','ep_plaza_small',
                   #enjoying-environmental impact
                   'ep_plaza_big',

                   #learning-wellbeing
                   'edu_basica_pub','edu_media_pub','jardin_inf_pub','universidad', 'edu_tecnica',
                   #learning-sociability
                   'edu_adultos_pub','edu_especial_pub','bibliotecas',
                   #learning-environmental impact
                   'centro_edu_amb',

                   #working-wellbeing
                   'paradas_tp_ruta','paradas_tp_metro','paradas_tp_tren',
                   #working-sociability [areal data: 'oficinas']
                   #working-environmental impact
                   'ciclovias','estaciones_bicicletas']
    
    # --------------- UNIQUE ID POIS (Special proximity cases)
    # From source_list, sources that have an unique ID and require special processing (id_pois_time function)
    # Unique ID for each of them is 'ID'.
    unique_id_sources = ['ferias','ep_plaza_big','ciclovias']
    # From source_list, sources that require normal and special processing (pois_time + id_pois_time functions)
    # Unique ID for each of them is 'ID'.
    special_sources = ['ep_plaza_small']

    # --------------- POIS LOCAL DIR
    # general pois local dir
    gral_dir = '../data/external/temporal_fromjupyter/santiago/pois/'

    # --------------- AREA OF INTEREST
    # Area of interest (aoi)
    aoi_schema = 'projects_research'
    aoi_table = 'santiago_aoi'
    # 'AM_Santiago' represents Santiago's metropolitan area, 'alamedabuffer_4500m' also available
    city = 'alamedabuffer_4500m'

    # --------------- PROYECTION
    projected_crs = 'EPSG:32719'

    # --------------- METHODOLOGY
    # Pois proximity methodology - Count pois at a given time proximity?
    count_pois = (True,15)

    # walking_speed (float): Decimal number containing walking speed (in km/hr) to be used if prox_measure="length",
	#						 or if prox_measure="time_min" but needing to fill time_min NaNs.
    walking_speed_list = [4.5] #[3.5,4.5,5,12,24,20,40]
    
    # --------------- INPUT NETWORK
    # If using previously downloaded OSMnx network available in database, set following to true
    osmnx_network = True
    # If true, set schemas and tables
    network_schema = 'projects_research'
    edges_table = 'santiago_edges'
    nodes_table = 'santiago_nodes'
    # Else, set external network data (Allows for filtering network according to a given column value)
    public_space_quality_dir = "../data/external/temporal_todocker/santiago/compiler_process/calidad_ep/redvial2019_buffer_3750m_c_utilidad_2.shp"
    projected_crs = "EPSG:32719"
    filtering_column = 'pje_ep'
    filtering_value = 0.5 # Will keep equal or more than this value

    # --------------- SAVING SPACE IN DISK
    # Save space in disk by deleting data that won't be used again?
    save_space = True

    # --------------- SAVING DATA
    ##### WARNING ##### WARNING ##### WARNING #####
    save = True # save output to database?
    project_code = 'p00' #Code to be added to saved files
    save_schema = 'projects_research'
    local_save = False # save output to local? (Make sure nodes_local_save_dir directory exists)
    ##### WARNING ##### WARNING ##### WARNING #####

    ########################################################## SCRIPT START ###########################################################

    # 0.0 --------------- BASE DATA FOR POIS-NODES ANALYSIS
    # ------------------- This step downloads the area of interest and network used to measure distance.

    # Area of interest (aoi)
    aup.log("--- Downloading area of interest.")
    query = f"SELECT * FROM {aoi_schema}.{aoi_table} WHERE \"city\" LIKE \'{city}\'"
    aoi = aup.gdf_from_query(query, geometry_col='geometry')
    aoi = aoi.set_crs("EPSG:4326")

    # Network
    if osmnx_network:
        aup.log("--- Downloading OSMnx network.")
        G, nodes, edges = aup.graph_from_hippo(aoi, network_schema, edges_table, nodes_table, projected_crs)
    else:
        aup.log("--- Converting local data to OSMnx format network.")
        G, nodes, edges = create_filtered_navigable_network(public_space_quality_dir, projected_crs, filtering_column, filtering_value)

    # Nodes proximity
    for walking_speed in walking_speed_list:
        aup.log('--'*45)
        aup.log(f"--- Running Script [Modified] for ep_plaza_small for speed = {walking_speed}km/hr.")
        str_walk_speed = str(walking_speed).replace('.','_')

        # Set saving dir and database table names
        nodes_local_save_dir = f"../data/processed/santiago/santiago_{project_code}_nodesproximity_{str_walk_speed}.gpkg"
        nodes_save_table = f'santiago_{project_code}_nodesproximity_{str_walk_speed}_kmh'

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
            source_speed_list = [source_check for source_check in source_speed_list
                                  if source_check not in saved_sources]
            aup.log(f"--- {len(source_speed_list)} sources to be processed.")
            
        # If passed source check, proceed to main function
        aup.log(f"--- Running Script for verified sources.")
        main(source_list, aoi, G, nodes, edges, walking_speed, local_save, save)