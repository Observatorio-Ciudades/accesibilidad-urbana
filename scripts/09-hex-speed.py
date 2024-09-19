import os
import sys

import numpy as np
import pandas as pd
import geopandas as gpd
import math

import matplotlib.pyplot as plt

import networkx as nx

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup

# Added this function on 2024/03/13 - Keeps only parts of Network which are have a path (are connected) to a central point of city.
def filter_city_osmnx_network(G, nodes, edges, mun_gdf, ageb_schema, ageb_table):

    # a) --------------- Create urban city shape (in order to find city center)
    aup.log(f"-- Filter osmnx network - Downloading city shape through ageb gdf.")
    ageb_gdf = gpd.GeoDataFrame()
    # Load city states (CVE_ENT)
    cve_ent_list = list(mun_gdf.CVE_ENT.unique())
    for cve_ent in cve_ent_list:
        #Load muns in each city state
        cve_mun_list = list(mun_gdf.loc[mun_gdf.CVE_ENT == cve_ent].CVE_MUN.unique())
        # To avoid error that happens when there's only one MUN (Creates tupple with one value only)
        # Error example: [SQL: SELECT * FROM censo_mza.censo_mza_2020 WHERE ("CVE_ENT" = '02') AND "CVE_MUN" IN ('001',) ]
        if len(cve_mun_list) >= 2:
            cve_mun_tpl = str(tuple(cve_mun_list))
        else:
            cve_mun_list.append(cve_mun_list[0])
            cve_mun_tpl = str(tuple(cve_mun_list))
        # Load AGEBs and concat
        query = f"SELECT * FROM {ageb_schema}.{ageb_table} WHERE (\"cve_ent\" = \'{cve_ent}\') AND \"cve_mun\" IN {cve_mun_tpl} "
        ageb_gdf = pd.concat([ageb_gdf,aup.gdf_from_query(query, geometry_col='geometry')])
    
    # b) --------------- Closest OSMnx node to city centroid
    aup.log(f"-- Filter osmnx network - Calculating nearest node to city shape centroid.")
    # City centroid
    aoi = ageb_gdf.dissolve()
    aoi = aoi.to_crs("EPSG:6372")
    aoi_centroid = gpd.GeoDataFrame(geometry=aoi.centroid)
    aoi_centroid = aoi_centroid.to_crs("EPSG:4326")
    # Nearest osmnx node
    nearest = aup.find_nearest(G, nodes, aoi_centroid, return_distance=False)
    
    # c) --------------- Find paths to city center
    aup.log(f"-- Filter osmnx network - Converting G to indirected to ignore street directions when finding paths.")
    # Get the unique osmid of the target node
    target_osmid = nearest.osmid.unique()[0]
    # Convert the graph to an undirected graph (To ignore nodes which are not reachable due to direction of streets)
    G_undirected = G.to_undirected()
    # Initialize a list to store nodes that have a path to the target node
    osmids_with_path = []

    # ---
    # LOG CODE - Will create progress logs when progress reaches these percentages:
    progress_logs = [5,10,15,20,25,30,35,40,45,50,
                    55,60,65,70,75,80,85,90,95,100]
    i = 1
    # ---

    # Iterate over all nodes in the graph searching for path to target
    for node in G_undirected.nodes():
        # Check if there is a path from the current node to the target node
        if nx.has_path(G_undirected, node, target_osmid):
            # If a path exists, append the node to the list
            osmids_with_path.append(node)

        # ---
        # LOG CODE - Measures current progress, prints if passed a checkpoint of progress_logs list.
        current_progress = (i / len(G_undirected.nodes()))*100
        for checkpoint in progress_logs:
            if current_progress >= checkpoint:
                aup.log(f'-- Filter osmnx network - Finding osmids with a path to city shape centroid. {checkpoint}% done.')
                progress_logs.remove(checkpoint)
                break
        i = i+1
        # ---

    # d) --------------- Filter edges using osmids_with_path
    aup.log(f"-- Filter osmnx network - Filtering edges using osmids with path.")
    edges_gdf = edges.reset_index()
    filtered_edges = edges_gdf.loc[(edges_gdf['u'].isin(osmids_with_path)) | (edges_gdf['v'].isin(osmids_with_path))].copy()
    filtered_edges = filtered_edges.set_index(["u", "v", "key"])

    aup.log(f"-- Filter osmnx network - Deleted {edges.shape[0] - filtered_edges.shape[0]} unconnected edges.")
    return filtered_edges


def main(mun_gdf, save=False, local_save=False):

    # Creates query to download OSMNX nodes and edges from the DB
    # by metropolitan area using the municipality geometry
    G,nodes,edges = aup.graph_from_hippo(mun_gdf, schema, edges_table, nodes_table)
    aup.log(f"--- Downloaded {len(edges)} edges from database for {city}.")

    # Deletes edges unconnected to main network (network that connects to city center)
    filtered_edges = filter_city_osmnx_network(G, nodes, edges, mun_gdf, ageb_schema='marco', ageb_table='ageb_2020')

    # Calculate walking speed for edges
    filtered_edges = aup.walk_speed(filtered_edges)
    aup.log(f"--- Average speed for {city} is {filtered_edges.walkspeed.mean()}")

    # Calculate time for edges
    filtered_edges['time_min'] = filtered_edges['length']/(1000*filtered_edges['walkspeed']/60)
    aup.log(f"--- Calculated time_min.")

    '''
    # Downloads hexgrid for city
    hex_gdf = gpd.GeoDataFrame()
    for CVEGEO in list(mun_gdf['CVEGEO'].unique()):
        # Downloads municipality polygon according to code
        query = f"SELECT * FROM processed.{hex_folder} WHERE \"CVEGEO\" LIKE \'{CVEGEO}\'"
        hex_gdf = hex_gdf.append(aup.gdf_from_query(query, geometry_col='geometry'))
        aup.log(f"Downloaded {CVEGEO} GeoDataFrame at: {city}")

    hex_gdf = hex_gdf.set_crs("EPSG:4326")
    
    #intersects edges with hex bins
    res_intersection = edges.overlay(hex_gdf, how='intersection')

    #calculates new length of cuted edges
    res_intersection = res_intersection.to_crs("EPSG:6372")

    res_intersection['length'] = res_intersection.geometry.length

    #calculate weighted walking speed
    dict_hex = {}

    for h in list(res_intersection.hex_id_8.unique()):
        sum_len = res_intersection.loc[res_intersection.hex_id_8 == h]['length'].sum()
        wWalkSpeed = []
        
        for idx, row in res_intersection.loc[res_intersection.hex_id_8 == h].iterrows():
            wWalkSpeed.append((row['length']*row['walkspeed'])/sum_len)
            
        dict_hex[h] = [sum(wWalkSpeed)]

    #walking speed by hex to dataframe
    df_walkspeed = pd.DataFrame.from_dict(dict_hex, orient='index', columns=['walkspeed']).reset_index()
    df_walkspeed.rename(columns={'index':'hex_id_8'}, inplace=True)

    #append walking speed to GeoDataFrame
    gdf_mrg = hex_gdf.merge(df_walkspeed, on='hex_id_8')

    gdf_mrg = gdf_mrg[['hex_id_8','CVEGEO','walkspeed','geometry']]'''

    # Tests
    if local_save:
        filtered_edges.reset_index(inplace=True)
        filtered_edges.to_file(f"../data/processed/proximity_v2/test_mty_filtered_edges.gpkg", driver='GPKG')
        aup.log(f"--- Saved edges speed gdf locally.")

    # Save
    if save:
        filtered_edges.reset_index(inplace=True)
        aup.gdf_to_db_slow(filtered_edges, edges_save_table, schema=schema, if_exists="append")
        aup.log(f"--- Uploaded {len(filtered_edges)} edges.")


if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('\n Starting script 09.')

    # ------------------------------ SCRIPT CONFIGURATION - DATABASE SCHEMAS AND TABLES ------------------------------
    # City data
    metro_schema = 'projects_research' # proxanalysis mexico: 'metropolis'
    metro_table = 'femsainfancias_missingcities_metrogdf2020' # proxanalysis mexico: 'metro_gdf_2015' or 'metro_gdf_2020'
    # Network data
    schema = 'projects_research' # proxanalysis mexico: 'osmnx'
    nodes_table = 'femsainfancias_missingcities_nodeselevation' # proxanalysis mexico: 'nodes_elevation' or 'nodes_elevation_23_point'
    edges_table = 'femsainfancias_missingcities_edgeselevation' #  proxanalysis mexico: 'edges_elevation' or 'edges_elevation_23_line'
    # Output to db
    edges_save_table = 'femsainfancias_missingcities_edgesspeed' #  proxanalysis mexico: 'edges_speed' or 'edges_speed_23_line'

    # ------------------------------ SCRIPT START ------------------------------
    # Load all available cities
    aup.log("--- Reading available cities.")
    query = f"SELECT city FROM {metro_schema}.{metro_table}"
    metro_df = aup.df_from_query(query)
    city_list = list(metro_df.city.unique())
    k = len(city_list)
    aup.log(f'--- Loaded city list with {k} cities.')

    # In metro_gdf_2020 CDMX was separated from the rest of ZMVM. 
    # For current edges speed analysis, it's better if they are joined together.
    # Remove CDMX, when ZMVM runs include CDMX.
    if metro_table == 'metro_gdf_2020':
        city_list.remove('CDMX') 

    # In case of a crash, must write processed_city_list manually because column city is not saved to output.
    # Currently, all cities:
    processed_city_list = ['Aguascalientes','Monterrey','Ensenada', 'Mexicali', 'Tijuana', 'La Paz', 'Los Cabos', 'Campeche', 'Laguna', 'Monclova', 'Piedras Negras', 
                           'Saltillo', 'Colima', 'Tapachula', 'Tuxtla', 'Chihuahua', 'Delicias', 'Juarez', 'Durango', 'Celaya', 'Guanajuato', 'Leon', 
                           'Irapuato', 'Acapulco', 'Chilpancingo', 'Pachuca', 'Tulancingo', 'Guadalajara', 'Vallarta', 'Piedad', 'Toluca', 'Morelia', 'Zamora', 
                           'Uruapan', 'Cuautla', 'Cuernavaca', 'Tepic', 'Oaxaca', 'Puebla', 'San Martin', 'Tehuacan', 'Queretaro', 'Cancun', 'Chetumal',
                           'Playa', 'SLP', 'Culiacan', 'Los Mochis', 'Mazatlan', 'Guaymas', 'Ciudad Obregon', 'Hermosillo', 'Nogales', 'Villahermosa', 'Victoria',
                           'Matamoros', 'Nuevo Laredo', 'Reynosa', 'Tampico', 'Tlaxcala', 'Coatzacoalcos', 'Cordoba', 'Minatitlan', 'Orizaba', 'Poza Rica', 'Veracruz', 
                           'Xalapa', 'Merida', 'Zacatecas','ZMVM']
    
    # LOG - Print progress of script so far
    missing_cities_list = []
    for city in city_list:
        if city not in processed_city_list:
            missing_cities_list.append(city)
    i = len(processed_city_list)
    aup.log(f'--- Already processed ({i}/{k}) cities.')
    aup.log(f'--- Missing procesing for cities: {missing_cities_list}')

    # Create mun_gdf for each city and run main function
    for city in missing_cities_list:
        i = i + 1
        aup.log("--"*40)
        aup.log(f"--- Running script for city {i}/{k}:{city}.")

        # Creates empty GeoDataFrame to store specified municipality polygons and hex grid
        mun_gdf = gpd.GeoDataFrame()

        if (metro_table =='metro_gdf_2020') and (city == 'ZMVM'):
            # Loads ZMVM
            city = 'CDMX'
            query = f"SELECT * FROM {metro_schema}.{metro_table} WHERE \"city\" LIKE \'{city}\'"
            gdf_1 = aup.gdf_from_query(query, geometry_col='geometry')
            # Loads CDMX
            city = 'ZMVM'
            query = f"SELECT * FROM {metro_schema}.{metro_table} WHERE \"city\" LIKE \'{city}\'"
            gdf_2 = aup.gdf_from_query(query, geometry_col='geometry')
            # Concatenates both
            mun_gdf = pd.concat([gdf_1,gdf_2])
        else:
            # Loads current city
            query = f"SELECT * FROM {metro_schema}.{metro_table} WHERE \"city\" LIKE \'{city}\'"
            mun_gdf = aup.gdf_from_query(query, geometry_col='geometry')

        #Define projections for municipalities and hexgrids
        mun_gdf = mun_gdf.set_crs("EPSG:4326")
        aup.log(f"--- Loaded municipalities (mun_gdf).")

        # Run main function
        main(mun_gdf, save=True, local_save=False)


