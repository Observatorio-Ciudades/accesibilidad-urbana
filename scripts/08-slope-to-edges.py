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


def main(schema, folder_sufix, year, grl_path, save=False):

    # Read json with municipality codes by capital or metropolitan area
    df = pd.read_json("Metropolis_CVE.json")
    aup.log("Read metropolitan areas and capitals json")

    # Iterate over municipality DataFrame columns to access each municipality code
    for c in df.columns.unique():
        aup.log(f"\n Starting municipality filters for {c}")
        # Creates empty GeoDataFrame to store specified municipality polygons and hex grid
        mun_gdf = gpd.GeoDataFrame()
        # Iterates over municipality codes for each metropolitan area or capital
        query = f"SELECT * FROM metropolis.metro_list WHERE \"city\" LIKE \'{c}\'"
        mun_gdf = aup.gdf_from_query(query, geometry_col='geometry')

        #Define projections for municipalities and hexgrids
        mun_gdf = mun_gdf.set_crs("EPSG:4326")

        # Creates query to download OSMNX nodes and edges from the DB
        # by metropolitan area or capital using the municipality geometry
        G, nodes, edges = aup.graph_from_hippo(mun_gdf, 'osmnx')
        aup.log(f"Downloaded {len(nodes)} nodes and {len(edges)} edges from database for {c}")

        mde_path = [] # list to append mde path strings
        #Gathers state codes for MDE
        for i in range(len(df.loc["edo", c])):
            e = df.loc["edo", c][i]
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
        aup.log(f"Assigned a mean elevation of {mean_elev} to nodes "+
        f"\nand mean slope of {mean_slope} to edges")
        # reset index for upload
        nodes_elev_mde.reset_index(inplace=True)
        edges_elev_mde.reset_index(inplace=True)

        #set street_count as float
        nodes_elev_mde["street_count"] = nodes_elev_mde["street_count"].astype(float)


        #upload
        if save:
            aup.gdf_to_db_slow(edges_elev_mde, "edges_"+folder_sufix, 
            schema=schema, if_exists="append")
            #Due to memory constraints the nodes are uploaded in groups of 10,000
            c_nodes = len(nodes_elev_mde)/10000
            for p in range(int(c_nodes)+1):
                nodes_upload = nodes_elev_mde.iloc[int(10000*p):int(10000*(p+1))].copy()
                aup.gdf_to_db_slow(nodes_upload, "nodes_"+folder_sufix, 
                schema=schema, if_exists="append")
                aup.log(f"uploaded {10000*(p+1)} nodes into DB out of {len(nodes_elev_mde)}")
            aup.log("Finished uploading nodes ")



if __name__ == "__main__":
    aup.log('--'*20)
    aup.log('\n Starting script')
    schema = 'osmnx'
    folder_sufix = 'elevation' #sufix for folder name
    year = '2020'
    grl_path = '../data/external/MDE/'
    main(schema, folder_sufix, year, grl_path, save=True)