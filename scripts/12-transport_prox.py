import os
import sys

import pandas as pd
import geopandas as gpd
import osmnx as ox

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup
#########This script integrates denue to nodes and distance amenities to do in a single run the distance/time check.
######### In this case to public transport stops
def main(year, schema, save=False):

    year = 2020

    df = pd.read_json("../Metropolis_CVE.json")


    aup.log("Read metropolitan areas and capitals json")

    #Folder names from database
    resolution = 8

    # Iterate over municipality DataFrame columns to access each municipality code
    for c in df.columns.unique():
        aup.log(f"\n Starting municipality filters for {c}")
        # Creates empty GeoDataFrame to store specified municipality polygons and hex grid
        mun_gdf = gpd.GeoDataFrame()
        hex_bins = gpd.GeoDataFrame()
        # Iterates over city names for each metropolitan area or capital
        query = f"SELECT * FROM metropolis.metro_list WHERE \"city\" LIKE \'{c}\'"
        mun_gdf = aup.gdf_from_query(query, geometry_col='geometry')
        query = f"SELECT * FROM metropolis.hexgrid_{resolution}_city WHERE \"metropolis\" LIKE \'{c}\'"
        hex_bins = aup.gdf_from_query(query, geometry_col='geometry')

        #Define projections
        mun_gdf = mun_gdf.set_crs("EPSG:4326")
        hex_bins = hex_bins.set_crs("EPSG:4326")

        #########PONER AQUI EL FILE EN LOCAL"""""
        transport = gpd.read_file('')
        
        G, nodes, edges = aup.graph_from_hippo(mun_gdf, 'osmnx')
        #Defines projection for downloaded data
        transport = transport.set_crs("EPSG:4326")
        nodes = nodes.set_crs("EPSG:4326")
        edges = edges.set_crs("EPSG:4326")

        aup.log(f"Created NetworkX for {c}")


        ############### PONER AQUI COLUMNAS RELEVANTES DE PARADAS DE TRANSPORTE###############
        points = transport[['', 'geometry']]
        nearest = aup.find_nearest(G, nodes, points, return_distance= True)
        nearest = nearest.set_crs("EPSG:4326")

        # Creates query to download OSMNX nodes and edges from the DB
        # by metropolitan area or capital using the municipality geometry
        _, nodes, edges = aup.graph_from_hippo(mun_gdf, 'osmnx', edges_folder='edges_speed')
        nodes_analysis = nodes.reset_index().copy()
        edges['time_min'].fillna(edges['time_min'].mean(),inplace=True)
        aup.log(f"Downloaded {len(nodes)} nodes and {len(edges)} from database for {c}")
        aup.log(f"")

        ###############NOMBRAR TIPO DE PARADA#################
        a = str('paradas_transporte')
        #############################################
        c_denue = len(nearest)/250
        for k in range(int(c_denue)+1):
            aup.log(f"Starting range k = {k} of {int(c_denue)}")
            denue_process = nearest.iloc[int(250*k):int(250*(1+k))].copy()
            nodes_distance_prep = aup.calculate_distance_nearest_poi(denue_process, nodes_analysis, 
            edges, a, 'osmid', wght='time_min')
            #A middle gdf is created whose columns will be the name of the amenity and the group number it belongs to
            df_int = pd.DataFrame()
            df_int['time_'+str(k)+a] = nodes_distance_prep['dist_'+a]
            #The middle gdf is merged into the previously created temporary gdf to store the data
            df_temp = df_temp.merge(df_int, left_index=True, right_index=True)
        aup.log(f"finished")
        #Once all groups of 100 are run, we drop the non-distance values from the temporary gdf
        df_temp.drop(['x', 'y', 'street_count','geometry'], inplace = True, axis=1)
        #We apply the min function to find the minimum value. This value is sent to a new df_min
        df_min = pd.DataFrame()
        df_min['time_'+a] = df_temp.min(axis=1)
        #We merge df_min which contains the shortest distance to the POI with nodes_distance which will store
        #all final data
        nodes_distance = nodes_distance.merge(df_min, left_index=True, right_index=True)



        #Data to hex_bins
        #In this process the data in the nodes will be summarized into the hexbin they fall.
        nodes_distance.reset_index(inplace=True)
        nodes_distance = nodes_distance.set_crs("EPSG:4326")
        hex_bins = hex_bins.set_crs("EPSG:4326")
        col_name = f'time_{a}'
        hex_dist = aup.group_by_hex_mean(nodes_distance, hex_bins, resolution, col_name)
        hex_bins = hex_bins.merge(hex_dist[['hex_id_'+str(resolution),col_name]], 
        on='hex_id_'+str(resolution))
        aup.log(f"Added distance data to {a} to {len(hex_bins)} hex bins")

        #If it is the first round nodes_amenities is created equal to nodes_distance
        #If it is the second or more, the new nodes_distance is merged.
        #This way we obtain the final gdf of interest that will contain the minimum disstance
        #to each type of amenity
        if i == 0:
            nodes_amenities = nodes_distance[['osmid','x','y','geometry','time_'+a]]
        else:
            nodes_amenities = nodes_amenities.merge(
                nodes_distance[['osmid','time_'+a]], on='osmid')
        aup.log('Added nodes distance to nodes_amenities')
        i += 1
        #We define the projections and upload
        hex_bins = hex_bins.set_crs("EPSG:4326")
        nodes_amenities = nodes_amenities.set_crs("EPSG:4326")

        #Give more efficient format to table
        hex_format = gpd.GeoDataFrame()
        node_format = gpd.GeoDataFrame()

        hex_format = hex_bins[['hex_id_8', 'geometry']]
        hex_format['time'] = hex_bins['time_'+a]
        hex_format['amenity'] = str(a)
        hex_format['metropolis'] = str(c)
        node_format = nodes_amenities[['osmid', 'x', 'y', 'geometry']]
        node_format['time'] = nodes_amenities['time_'+a]
        node_format['amenity'] = str(a)
        node_format['metropolis'] = str(c)


if __name__ == "__main__":

    SCHEMA = 'censo'
    years = [2010, 2020]

    aup.log('--'*20)
    aup.log('\n Starting script')

    for year in years:
        main(year, SCHEMA, save=True)