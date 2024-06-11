import geopandas as gpd
import pandas as pd
import osmnx as ox

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup

from multiprocessing import Pool
from tqdm import tqdm

import warnings
warnings.filterwarnings("ignore")


def chunker_list(seq, size):
    return (seq[i::size] for i in range(size))

# Define a function to be executed by each process
def create_isochrone(input_tuple):
        G, nodes, edges, point_of_interest, trip_time, prox_measure, projected_crs = input_tuple
        return aup.proximity_isochrone(G, nodes, edges, point_of_interest, trip_time, prox_measure, projected_crs)

def main(trip_time, prox_measure, projected_crs, nodes_list, nodes, edges, G, db_save=False, local_save=False):
    aup.log("--"*40)
    aup.log(f"--- STARTING MAIN FUNCTION.")


    # 1.2 --------------- CREATE ISOCHRONES FOR ALL NODES
    # ------------------- This creates pois from all nodes and creates isochrones for each one of them.
    
    # Create pois from nodes
    aup.log("--- Considering all nodes as pois.")
    # pois = nodes.reset_index().copy()[['osmid','geometry']]
    # osmid_list = list(pois.osmid.unique())
    '''# Test mode creates the first 100 isochrones only and saves to local only.
    if test:
        osmid_list = osmid_list[0:100]'''

    k = len(nodes_list)

    # Integrate multiprocessor here
    isochrones_gdf = gpd.GeoDataFrame()

    # Create a multiprocessing input list
    # input_list = [(G, nodes, edges, point_of_interest, trip_time, prox_measure, projected_crs) for point_of_interest in pois.itertuples()]

    # Create a multiprocessing pool
    # pool = multiprocessing.Pool()

    # Map the create_isochrone function to the input list using the multiprocessing pool
    # results = pool.map(create_isochrone, input_list)

    # Close the multiprocessing pool
    # pool.close()
    # pool.join()

    # Iterate over the results and assign osmid and geometry to the isochrones_gdf
    # for i, result in enumerate(results):
    #     isochrones_gdf.loc[i, 'osmid'] = pois.loc[i, 'osmid']
    #     isochrones_gdf.loc[i, 'geometry'] = result

    # Single process version
    
    # Iterate over each node and create an isochrones
    aup.log("--- Iterating over {k} nodes.")
    isochrones_gdf = gpd.GeoDataFrame()

    # change crs
    nodes = nodes.to_crs(projected_crs)
    edges = edges.to_crs(projected_crs)

    # i = 0
    for i in tqdm(range(k), position=0, leave=True):

        # aup.log(f"Creating isochrone for node_poi {i}/{k}.")

        # Current node being analysed
        # point_of_interest = pois.loc[pois.osmid == osmid].copy()
        osmid = nodes_list[i]
        
        # Calculate isochrone geometry
        # hull_geom = aup.proximity_isochrone_from_osmid(G, nodes, edges, osmid, trip_time, prox_measure, projected_crs)
        

        center_node_gdf = nodes.loc[nodes.index==osmid].copy()
        buffer = center_node_gdf.buffer(5000)

        nodes_buff = gpd.clip(nodes, buffer)
        edges_buff = gpd.clip(edges, buffer)
        edges_buff['length'] = edges_buff.length

        nodes_buff = nodes_buff.to_crs("EPSG:4326")
        edges_buff = edges_buff.to_crs("EPSG:4326")
        

        G_buff = ox.graph_from_gdfs(nodes_buff, edges_buff)
        # 1000m in 15 minutes at 4km/hr
        iso_geom = aup.calculate_isochrone(G_buff, osmid, trip_time, prox_measure, True)

        # Assign osmid and geometry to created isochrone and save to gdf
        isochrones_gdf.loc[i,'osmid'] = osmid
        isochrones_gdf.loc[i,'geometry'] = iso_geom

        # i += 1
    
    isochrones_gdf = isochrones_gdf.set_geometry('geometry')
    isochrones_gdf = isochrones_gdf.set_crs("EPSG:4326")

    # 1.3 --------------- SAVING
    if local_save:
        isochrones_gdf.to_file(isochrones_local_save_dir, driver='GPKG')
        aup.log(f"--- Saved {city}'s nodes isochrones locally.")

    if db_save:
        aup.gdf_to_db_slow(isochrones_gdf, save_table, save_schema, if_exists='append')
        aup.log(f"--- Saved {city}'s nodes isochrones in database.")


if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('--- STARTING SCRIPT 24.')
    
    # ------------------------------ BASE DATA REQUIRED ------------------------------

    # Area of interest (Run 'AM_Santiago', it represents Santiago's metropolitan area. We can clip data as soon as we know inputs extent.)
    city = 'AM_Santiago'

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
    save_table = 'santiago_nodesisochrones'

    # ---------------------------- SCRIPT CONFIGURATION - ANALYSIS AND OUTPUT OPTIONS ----------------------------

    # ANALYSIS AND OUTPUT -  Size of isochrones (minutes)
    trip_time = 1000

    # ANALYSIS AND OUTPUT - Network distance method used in function pois_time. (If length, assumes pedestrian speed of 4km/hr.)
    prox_measure = 'length' # Must pass 'length' or 'time_min'

    # ANALYSIS AND OUTPUT - Depending on area of interest, projected crs to be used when needed.
    projected_crs = "EPSG:32719"

    # ---------------------------- SCRIPT CONFIGURATION - SAVING ----------------------------

    ##### WARNING ##### WARNING ##### WARNING #####
    db_save = True # save output to database?
    local_save = False # save output to local? (Make sure directory exists)
    isochrones_local_save_dir = f"../data/processed/santiago/test_script24_nodesisochrones.gpkg"
    ##### WARNING ##### WARNING ##### WARNING #####

    # Test mode overrides db_save (Does not save to database), 
    # creates isochrones for the first 100 nodes only,
    # and saves to local only (Make sure directory exists).
    test = False

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
    aup.log("--- Network downloaded.")

    # ---------------------------- SCRIPT START ----------------------------
    # if test:
    #     main(trip_time, prox_measure, projected_crs, db_save=False, local_save=True)
    # else:
    # split nodes for multiprocessing
    all_nodes = list(set(list(nodes.index.unique())))
    split_nodes = chunker_list(all_nodes, 10)

    aup.log('Starting multiprocessing.')
    # list for multiprocessing
    
    input_list = [(trip_time, prox_measure, projected_crs, input_nodes, nodes, edges, G, db_save, local_save) for input_nodes in split_nodes]

    pool = Pool(processes=190)
    pool.starmap(main, input_list)
    pool.close()