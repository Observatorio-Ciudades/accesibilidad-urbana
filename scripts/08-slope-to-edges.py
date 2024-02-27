import os
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Point, LineString

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


def main(mun_gdf, save=False):

        # Creates query to download OSMNX nodes and edges from the DB
        # by metropolitan area or capital using the municipality geometry
        G, nodes, edges = aup.graph_from_hippo(mun_gdf, schema, edges_table, nodes_table)
        aup.log(f"--- Downloaded {len(nodes)} nodes and {len(edges)} edges from database for {city}.")

        mde_path = [] # list to append mde path strings
        #Gathers state codes for MDE
        for e in mun_gdf['CVE_ENT'].unique():
            tmp_path = grl_path + (f'CEM_V3_20170619_R15_E{e}_TIF/')

            #search for files in tmp_path for .tif
            for f in os.listdir(tmp_path):
                if f.endswith('.tif'):
                    mde_path.append(tmp_path+f)

        #elevations to nodes
        G_elev_mde = ox.elevation.add_node_elevations_raster(G, mde_path)
        #slope to edges
        G_elev_mde = ox.elevation.add_edge_grades(G_elev_mde, add_absolute=True, precision=3)
        nodes_elev_mde, edges_elev_mde = ox.graph_to_gdfs(G_elev_mde, nodes=True, edges=True)
        mean_elev = round(nodes_elev_mde.elevation.mean(),2)
        mean_slope = round(edges_elev_mde.grade_abs.mean(),2)
        aup.log(f"--- Assigned a mean elevation of {mean_elev} to nodes")
        aup.log(f"    and mean slope of {mean_slope} to edges.")
        # reset index for upload
        nodes_elev_mde.reset_index(inplace=True)
        edges_elev_mde.reset_index(inplace=True)

        #set street_count as float
        nodes_elev_mde["street_count"] = nodes_elev_mde["street_count"].astype(float)

        # Temp - nodes_23_point and edges_23_point has column 'city', yet to be deleted from DB.
        # If used, delete column 'city'.
        if nodes_table == 'nodes_23_point':
            nodes_elev_mde = nodes_elev_mde.drop(columns=['city'])
        if edges_table == 'edges_23_line':
            edges_elev_mde = edges_elev_mde.drop(columns=['city'])

        #upload
        if save:
            aup.gdf_to_db_slow(edges_elev_mde, "edges_"+folder_sufix+"_line", 
            schema=schema, if_exists="append")
            aup.log(f"--- Uploaded edges into DB.")
            #Due to memory constraints the nodes are uploaded in groups of 10,000
            c_nodes = len(nodes_elev_mde)/10000
            for p in range(int(c_nodes)+1):
                nodes_upload = nodes_elev_mde.iloc[int(10000*p):int(10000*(p+1))].copy()
                aup.gdf_to_db_slow(nodes_upload, "nodes_"+folder_sufix+"_point", 
                schema=schema, if_exists="append")
                aup.log(f"--- Uploaded {10000*(p+1)} nodes into DB out of {len(nodes_elev_mde)}.")
            aup.log("--- Finished uploading nodes.")

if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('\n Starting script.')

    # --------------- PARAMETERS

    # City data
    metro_schema = 'metropolis'
    metro_table = 'metro_gdf_2020' # metro_gdf_2015 or metro_gdf_2020
    # Network data
    schema = 'osmnx'
    nodes_table = 'nodes_23_point' # nodes or nodes_23_point
    edges_table = 'edges_23_line' # edges or edges_23_line
    # Location of unzipped MDE data
    grl_path = '../data/external/MDE/'
    # Folder sufix
    folder_sufix = 'elevation_23' # elevation or elevation_23

    # --------------- SCRIPT
    # Load all cities
    aup.log("--- Reading available cities.")
    query = f"SELECT city FROM {metro_schema}.{metro_table}"
    metro_df = aup.df_from_query(query)
    city_list = list(metro_df.city.unique())
    k = len(city_list)
    aup.log(f'--- Loaded city list with {k} cities.')

    #In metro_gdf_2020 CDMX was separated from the rest of ZMVM. 
    # For current elevation analysis, it's better if they are together.
    if metro_table == 'metro_gdf_2020':
        city_list.remove('CDMX') 

    # In case of a crash, must write processed_city_list manually
    processed_city_list = ['Aguascalientes','Ensenada','Mexicali','Tijuana','La Paz','Los Cabos',
                           'Campeche','Laguna','Monclova','Piedras Negras','Saltillo','Colima',
                           'Tapachula','Tuxtla','Chihuahua','Delicias','Juarez','ZMVM','Durango',
                           'Celaya','Guanajuato','Leon','Irapuato','Acapulco','Chilpancingo',
                           'Pachuca','Tulancingo','Guadalajara','Vallarta','Piedad','Toluca',
                           'Morelia','Zamora','Uruapan','Cuautla','Cuernavaca','Tepic','Monterrey',
                           'Oaxaca','Puebla','San Martin','Tehuacan','Queretaro','Cancun','Chetumal',
                           'Playa','SLP','Culiacan']
    
    # LOG - Print progress of script so far
    missing_cities_list = []
    for city in city_list:
        if city not in processed_city_list:
            missing_cities_list.append(city)
    i = len(processed_city_list)
    aup.log(f'--- Already processed ({i}/{k}) cities.')
    aup.log(f'--- Missing procesing for cities: {missing_cities_list}')

    # Iterate over municipality DataFrame columns to access each municipality code
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
        aup.log(f"--- Starting main function for {city}.")
        main(mun_gdf, save=True)