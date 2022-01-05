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


def main(schema, folder_sufix, save=False):

    df = pd.read_json("../scripts/Metropolis_CVE.json")
    aup.log("Read metropolitan areas and capitals json")

    mpos_folder = 'mpos_2020'
    hex_folder = 'hex_bins_index_2020'

    for c in df.columns.unique():
        
        mun_gdf = gpd.GeoDataFrame()
        hex_gdf = gpd.GeoDataFrame()

        for i in range(len(df.loc["mpos", c])):
            # Extracts specific municipality code
            m = df.loc["mpos", c][i]
            # Downloads municipality polygon according to code
            query = f"SELECT * FROM marco.{mpos_folder} WHERE \"CVEGEO\" LIKE \'{m}\'"
            mun_gdf = mun_gdf.append(aup.gdf_from_query(query, geometry_col='geometry'))
            query = f"SELECT * FROM processed.{hex_folder} WHERE \"CVEGEO\" LIKE \'{m}\'"
            hex_gdf = hex_gdf.append(aup.gdf_from_query(query, geometry_col='geometry'))
            aup.log(f"Downloaded {m} GeoDataFrame at: {c}")

        hex_gdf = hex_gdf.set_crs("EPSG:4326")

        _,_,edges = aup.graph_from_hippo(mun_gdf, 'osmnx', 
                                     edges_folder='edges_elevation', nodes_folder='nodes_elevation')

        #calculate walking speed for edges
        edges = aup.walk_speed(edges)

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

        gdf_mrg = gdf_mrg[['hex_id_8','CVEGEO','walkspeed','geometry']]

        aup.log(f"Average speed for {c} is {gdf_mrg.walkspeed.mean()}")

        if save:

            aup.gdf_to_db_slow(gdf_mrg, "hex_"+folder_sufix, 
                    schema=schema, if_exists="append")



if __name__ == "__main__":

    schema = 'speed'
    folder_sufix = 'speed'

    main(schema, folder_sufix, save=True)


