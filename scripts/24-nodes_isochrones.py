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

def main(trip_time, prox_measure, projected_crs, db_save=False, local_save=False):
    aup.log("--"*40)
    aup.log(f"--- STARTING MAIN FUNCTION.")
    
    
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


    # 1.2 --------------- CREATE ISOCHRONES FOR ALL NODES
    # ------------------- This creates pois from all nodes and creates isochrones for each one of them.
    
    # Create pois from nodes
    aup.log("--- Considering all nodes as pois.")
    pois = nodes.reset_index().copy()[['osmid','geometry']]
    osmid_list = list(pois.osmid.unique())
    # Test mode creates the first 100 isochrones only and saves to local only.
    if test:
        osmid_list = osmid_list[0:100]

    k = len(osmid_list)

    # Iterate over each node and create an isochrones
    aup.log("--- Iterating over all pois.")
    isochrones_gdf = gpd.GeoDataFrame()
    i = 0
    for osmid in osmid_list:

        aup.log(f"Creating isochrone for node_poi {i}/{k}.")

        # Current node being analysed
        point_of_interest = pois.loc[pois.osmid == osmid].copy()
        
        # Calculate isochrone geometry
        hull_geom = aup.proximity_isochrone(G, nodes, edges, point_of_interest, trip_time, prox_measure, projected_crs)

        # Assign osmid and geometry to created isochrone and save to gdf
        isochrones_gdf.loc[i,'osmid'] = point_of_interest.osmid.unique()[0]
        isochrones_gdf.loc[i,'geometry'] = hull_geom

        i += 1

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
    trip_time = 15

    # ANALYSIS AND OUTPUT - Network distance method used in function pois_time. (If length, assumes pedestrian speed of 4km/hr.)
    prox_measure = 'length' # Must pass 'length' or 'time_min'

    # ANALYSIS AND OUTPUT - Depending on area of interest, projected crs to be used when needed.
    projected_crs = "EPSG:32719"

    # ---------------------------- SCRIPT CONFIGURATION - SAVING ----------------------------

    ##### WARNING ##### WARNING ##### WARNING #####
    db_save = False # save output to database?
    local_save = True # save output to local? (Make sure directory exists)
    isochrones_local_save_dir = f"../data/processed/santiago/test_script24_nodesisochrones.gpkg"
    ##### WARNING ##### WARNING ##### WARNING #####

    # Test mode overrides db_save (Does not save to database), 
    # creates isochrones for the first 100 isochrones only,
    # and saves to local only (Make sure directory exists).
    test = True

    # ---------------------------- SCRIPT START ----------------------------
    if test:
        main(trip_time, prox_measure, projected_crs, db_save=False, local_save=True)
    else:
        main(trip_time, prox_measure, projected_crs, db_save, local_save)