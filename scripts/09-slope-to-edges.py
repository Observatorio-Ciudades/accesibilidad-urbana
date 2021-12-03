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
    
    #Folder names from database
    mpos_folder = 'mpos_'+year
    
    # Iterate over municipality DataFrame columns to access each municipality code
    for c in df.columns.unique():
        aup.log(f"\n Starting municipality filters for {c}")
        # Creates empty GeoDataFrame to store specified municipality polygons and hex grid
        mun_gdf = gpd.GeoDataFrame()
        # Iterates over municipality codes for each metropolitan area or capital
        for i in range(len(df.loc["mpos", c])):
            # Extracts specific municipality code
            m = df.loc["mpos", c][i]
            # Downloads municipality polygon according to code
            query = f"SELECT * FROM marco.{mpos_folder} WHERE \"CVEGEO\" LIKE \'{m}\'"
            mun_gdf = mun_gdf.append(aup.gdf_from_query(query, geometry_col='geometry'))
            aup.log(f"Downloaded {m} GeoDataFrame at: {c}")

        #Define projections for municipalities and hexgrids
        mun_gdf = mun_gdf.set_crs("EPSG:4326")

        # Creates query to download OSMNX nodes and edges from the DB
        # by metropolitan area or capital using the municipality geometry
        _, nodes, edges = aup.graph_from_hippo(mun_gdf, 'osmnx')
        aup.log(f"Downloaded {len(nodes)} nodes and {len(edges)} edges from database for {c}")

        #Completes nodes GeoDataFrame using edges data
        nodes_tmp = nodes.reset_index().copy()

        edges_tmp = edges.reset_index().copy()

        #finds difference between node osmid in edges and nodes
        #at start of line
        from_osmid = list(set(edges_tmp['u'].to_list()).difference(
            set(nodes_tmp.osmid.to_list())))

        nodes_dict = nodes_tmp.to_dict()

        #gathers geometry for missing nodes and adds it to dictionary
        for i in from_osmid:
            row = edges_tmp.loc[(edges_tmp.u==i)].iloc[0]
            coords = [(coords) for coords in list(row['geometry'].coords)]
            first_coord, last_coord = [ coords[i] for i in (0, -1) ]
            
            nodes_dict['osmid'][len(nodes_dict['osmid'])] = i
            nodes_dict['x'][len(nodes_dict['x'])] = first_coord[0]
            nodes_dict['y'][len(nodes_dict['y'])] = first_coord[1]
            nodes_dict['street_count'][len(nodes_dict['street_count'])] = np.nan
            nodes_dict['geometry'][len(nodes_dict['geometry'])] = Point(first_coord)
                
        #finds difference between node osmid in edges and nodes
        #at end of line
        to_osmid = list(set(edges_tmp['v'].to_list()).difference(
            set(list(nodes_dict['osmid'].values()))))

        #gathers geometry for missing nodes and adds it to dictionary
        for i in to_osmid:
            row = edges_tmp.loc[(edges_tmp.u==i)].iloc[0]
            coords = [(coords) for coords in list(row['geometry'].coords)]
            first_coord, last_coord = [ coords[i] for i in (0, -1) ]
            
            nodes_dict['osmid'][len(nodes_dict['osmid'])] = i
            nodes_dict['x'][len(nodes_dict['x'])] = last_coord[0]
            nodes_dict['y'][len(nodes_dict['y'])] = last_coord[1]
            nodes_dict['street_count'][len(nodes_dict['street_count'])] = np.nan
            nodes_dict['geometry'][len(nodes_dict['geometry'])] = Point(last_coord)
            
        #create GeoDataFrame with missing nodes
        nodes_tmp = pd.DataFrame.from_dict(nodes_dict)
        nodes_tmp = gpd.GeoDataFrame(nodes_tmp, crs="EPSG:4326", geometry='geometry')
        G = ox.graph_from_gdfs(nodes_tmp.set_index('osmid'), edges_tmp.set_index(['u','v','key']))

        missing_nodes = len(from_osmid) + len(to_osmid)
        aup.log(f"Filled {missing_nodes} missing nodes")

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
    aup.log('Starting script')
    schema = 'osmnx'
    folder_sufix = 'elevation' #sufix for folder name
    year = '2020'
    grl_path = '../data/external/MDE/'
    main(schema, folder_sufix, year, grl_path, save=False)