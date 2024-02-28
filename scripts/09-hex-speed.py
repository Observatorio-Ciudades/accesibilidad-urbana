import os
import sys

import numpy as np
import pandas as pd
import geopandas as gpd
import math

import matplotlib.pyplot as plt

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


def main(mun_gdf, save=False):

    # Creates query to download OSMNX nodes and edges from the DB
    # by metropolitan area or capital using the municipality geometry
    _,_,edges = aup.graph_from_hippo(mun_gdf, schema, edges_table, nodes_table)
    aup.log(f"--- Downloaded {len(edges)} edges from database for {city}.")

    # Calculate walking speed for edges
    edges = aup.walk_speed(edges)
    # Calculate time for edges
    edges['time_min'] = edges['length']/(1000*edges['walkspeed']/60)
    aup.log(f"Average speed for {city} is {edges.walkspeed.mean()}")

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

    # Save
    if save:
        edges.reset_index(inplace=True)
        aup.gdf_to_db_slow(edges, "edges_"+edges_table_sufix, schema=schema, if_exists="append")


if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('\n Starting script.')

    # --------------- PARAMETERS
    # City data
    metro_schema = 'metropolis'
    metro_table = 'metro_gdf_2020' # metro_gdf_2015 or metro_gdf_2020
    # Network data
    schema = 'osmnx'
    nodes_table = 'nodes_elevation_23_point' # nodes_elevation or nodes_elevation_23_point
    edges_table = 'edges_elevation_23_line' # edges_elevation or edges_elevation_23_line
    # Output table sufix
    edges_table_sufix = 'speed_23_line' # speed or speed_23_line

    # --------------- SCRIPT
    # Load all available cities
    aup.log("--- Reading available cities.")
    query = f"SELECT city FROM {metro_schema}.{metro_table}"
    metro_df = aup.df_from_query(query)
    city_list = list(metro_df.city.unique())
    k = len(city_list)
    aup.log(f'--- Loaded city list with {k} cities.')

    # In metro_gdf_2020 CDMX was separated from the rest of ZMVM. 
    # For current edges speed analysis, it's better if they are joined together.
    if metro_table == 'metro_gdf_2020':
        city_list.remove('CDMX') 

    # In case of a crash, must write processed_city_list manually 
    # because column city is not saved to output.
    processed_city_list = ['Aguascalientes', 'Ensenada', 'Mexicali', 'Tijuana', 'La Paz', 'Los Cabos', 'Campeche', 'Laguna', 'Monclova', 'Piedras Negras', 
                           'Saltillo', 'Colima', 'Tapachula', 'Tuxtla', 'Chihuahua', 'Delicias', 'Juarez', 'Durango', 'Celaya', 'Guanajuato', 'Leon', 
                           'Irapuato', 'Acapulco', 'Chilpancingo', 'Pachuca', 'Tulancingo', 'Guadalajara', 'Vallarta', 'Piedad', 'Toluca', 'Morelia', 'Zamora', 
                           'Uruapan', 'Cuautla', 'Cuernavaca', 'Tepic', 'Monterrey', 'Oaxaca', 'Puebla', 'San Martin', 'Tehuacan', 'Queretaro', 'Cancun', 'Chetumal', 
                           'Playa', 'SLP', 'Culiacan', 'Los Mochis', 'Mazatlan', 'Guaymas', 'Ciudad Obregon', 'Hermosillo', 'Nogales', 'Villahermosa', 'Victoria', 
                           'Matamoros', 'Nuevo Laredo', 'Reynosa', 'Tampico', 'Tlaxcala', 'Coatzacoalcos', 'Cordoba', 'Minatitlan', 'Orizaba', 'Poza Rica', 'Veracruz', 
                           'Xalapa', 'Merida', 'Zacatecas']
    
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
        aup.log("--"*40)
        i = i + 1
        aup.log(f"--- Loading municipalities for city {i}/{k}:{city}.")

        # Creates empty GeoDataFrame to store specified municipality polygons and hex grid
        mun_gdf = gpd.GeoDataFrame()

        if (metro_table =='metro_gdf_2020') and (city == 'ZMVM'):
            # Loads ZMVM
            city = 'ZMVM'
            query = f"SELECT * FROM {metro_schema}.{metro_table} WHERE \"city\" LIKE \'{city}\'"
            gdf_1 = aup.gdf_from_query(query, geometry_col='geometry')
            # Loads CDMX
            city = 'CDMX'
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

        # Run main function
        aup.log(f"--- Starting Script 09 main function for {city}.")
        main(mun_gdf, save=True)


