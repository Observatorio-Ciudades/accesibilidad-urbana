import os
import sys

import pandas as pd
import geopandas as gpd
import osmnx as ox

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup

def main(schema, year, save=False):
    df = pd.read_json("/home/jovyan/work/scripts/Metropolis_CVE.json")
    aup.log("Read metropolitan areas and capitals json")

    #Folder names from database
    denue_folder = 'denue_' + year

    # Iterate over cities and download municipalities gdf
    for c in df.columns.unique():
        aup.log(f"\n Starting municipality filters for {c}")
        # Creates empty GeoDataFrame to store specified municipality polygons
        mun_gdf = gpd.GeoDataFrame()
        # Iterates over municipality codes for each metropolitan area or capital
        # Downloads municipality polygon according to code
        query = f"SELECT * FROM metropolis.metro_list WHERE \"city\" LIKE \'{c}\'"
        mun_gdf = aup.gdf_from_query(query, geometry_col='geometry')
        aup.log(f"Downloaded municipality GeoDataFrames at: {c}")    
        #Define projections
        mun_gdf = mun_gdf.set_crs("EPSG:4326")
    
        #Creates polygon for query
        poly_wkt = mun_gdf.dissolve().geometry.to_wkt()[0]
        # Creates query to download nodes from the metropolitan area or capital
        aup.log("Created wkt based on dissolved polygon")
        # Creates query to download DENUE from the metropolitan area or capital
        query = f"SELECT * FROM denue.{denue_folder} WHERE ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')"
        denue = aup.gdf_from_query(query, geometry_col='geometry')
        aup.log(f"Downloaded {len(denue)} denue from database for {c}")

        #Downloads street network from base
        G, nodes, edges = aup.graph_from_hippo(mun_gdf, 'osmnx')
        #Defines projection for downloaded data
        denue = denue.set_crs("EPSG:4326")
        nodes = nodes.set_crs("EPSG:4326")
        edges = edges.set_crs("EPSG:4326")


        aup.log(f"Created NetworkX for {c}")
        ### Filters relevant columns from DENUE.
        ### If you are interested in keeping other columns, change/add here
        points = denue[['id', 'codigo_act', 'geometry']]
        ### Calculate nearest node for each DENUE point
        nearest = aup.find_nearest(G, nodes, points, return_distance= True)
        nearest = nearest.set_crs("EPSG:4326")
        ###SAVE
        if save:
            aup.gdf_to_db_slow(nearest, "denue_node_"+year, schema=schema, if_exists="append")




if __name__ == "__main__":
    aup.log('Starting script')
    year = '2020'
    schema = 'denue_nodes'
    main(schema, year, save = True)