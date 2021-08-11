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
    df = pd.read_json("/home/jovyan/work/scripts/metropolis_test.json")
    aup.log("Read metropolitan areas and capitals json")

    #Folder names from database
    mpos_folder = 'mpos_'+ year
    denue_folder = 'denue_' + year

    # Iterate over municipality DataFrame columns to access each municipality code
    for c in df.columns.unique():
        aup.log(f"\n Starting municipality filters for {c}")
        # Creates empty GeoDataFrame to store specified municipality polygons
        mun_gdf = gpd.GeoDataFrame()
        # Iterates over municipality codes for each metropolitan area or capital
        for i in range(len(df.loc["mpos", c])):
            # Extracts specific municipality code
            m = df.loc["mpos", c][i]
            # Downloads municipality polygon according to code
            query = f"SELECT * FROM marco.{mpos_folder} WHERE \"CVEGEO\" LIKE \'{m}\'"
            mun_gdf = mun_gdf.append(aup.gdf_from_query(query, geometry_col='geometry'))
            aup.log(f"Downloaded {m} GeoDataFrame at: {c}")    
        #Define projections
        mun_gdf = mun_gdf.set_crs("EPSG:4326")
        
        poly = mun_gdf.geometry
        # Extracts coordinates from polygon as DataFrame
        coord_val = poly.bounds
        # Gets coordinates for bounding box
        n = coord_val.maxy.max()
        s = coord_val.miny.min()
        e = coord_val.maxx.max()
        w = coord_val.minx.min()
    # Downloads OSMnx graph from bounding box
        G = ox.graph_from_bbox(n, s, e, w, network_type="all")
    
        #Creates polygon for query
        poly_wkt = mun_gdf.dissolve().geometry.to_wkt()[0]
        # Creates query to download nodes from the metropolitan area or capital
        aup.log("Created wkt based on dissolved polygon")
        ###################
        #query = f"SELECT * FROM osmnx_new.nodes WHERE ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')"
        #nodes = aup.gdf_from_query(query, geometry_col='geometry')
        #aup.log(f"Downloaded {len(nodes)} nodes from database for {c}")
        ## Creates query to download edges from the metropolitan area or capital
        #query = f"SELECT * FROM osmnx_new.edges WHERE ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')"
        #edges = aup.gdf_from_query(query, geometry_col='geometry')
        #aup.log(f"Downloaded {len(edges)} edges from database for {c}")
        #####################

        # Creates query to download DENUE from the metropolitan area or capital
        query = f"SELECT * FROM denue.{denue_folder} WHERE ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')"
        denue = aup.gdf_from_query(query, geometry_col='geometry')
        aup.log(f"Downloaded {len(denue)} denue from database for {c}")
        #Defines projection for downloaded data
        denue = denue.set_crs("EPSG:4326")
        #########
        #nodes = nodes.set_crs("EPSG:4326")
        #edges = edges.set_crs("EPSG:4326")
        #######
        #Creates NetworkX graph from nodes and edges
        #G = ox.graph_from_gdfs(nodes, edges, graph_attrs=None)

        aup.log(f"Created NetworkX for {c}")

        nearest = aup.find_nearest(G, denue, "nearest_node")
        nearest = nearest.set_crs("EPSG:4326")

        if save:
            aup.gdf_to_db_slow(nearest, "denue_node_"+year, schema=schema, if_exists="append")




if __name__ == "__main__":
    aup.log('Starting script')
    year = '2020'
    schema = 'population'
    main(schema, year, save = True)