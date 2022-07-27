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
    ##### This script creates a network from INEGIs 2011 streets and nodes data
    for i in range(1,33):

        c = format(i, '02d')

        aup.log(f"Starting download for e{c}")

        # Downloads edges and nodes from DB
        query = f"SELECT * FROM vialidades11.edges11 WHERE \"UNIDAD\" LIKE \'e{c}\'"
        edges = aup.gdf_from_query(query, geometry_col='geometry')
        query = f"SELECT * FROM vialidades11.nodes11 WHERE \"UNIDAD\" LIKE \'e{c}\'"
        nodes = aup.gdf_from_query(query, geometry_col='geometry')

        aup.log(f"Downloaded {len(nodes)} nodes and {len(edges)} edges from database for e{c}")
        ###Creates network
        nodes, edges = aup.create_network(nodes, edges)

        aup.log("Finished creating network")
        ###Upload
        if save:
            aup.gdf_to_db_slow(edges, "edges"+folder_sufix, 
                    schema=schema, if_exists="append")

            c_nodes = len(nodes) / 10000
            aup.log(f"There are a total of {round(c_nodes,2)} nodes divisions")
            for cont in range(int(c_nodes)+1):
                nodes_upload = nodes.iloc[int(10000*cont):int(10000*(cont+1))].copy()
                aup.gdf_to_db_slow(nodes_upload, "nodes"+folder_sufix, schema=schema, if_exists="append")
                aup.log(f"Uploaded {cont} out of {round(c_nodes,2)}")




if __name__ == "__main__":

    schema = 'networks'
    folder_sufix = '_2011'

    main(schema, folder_sufix, save=True)