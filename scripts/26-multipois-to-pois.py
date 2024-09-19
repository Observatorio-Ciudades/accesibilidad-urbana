
import geopandas as gpd
import pandas as pd
import numpy as np

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
import sys
module_path = os.path.abspath(os.path.join('../'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


# source_list = ['ferias','ep_plaza_small',
#               'ep_plaza_big','ciclovia']
source_list = ['ciclovias']
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

walking_speed = [3.5]

for walk_speed in walking_speed:
    str_walk_speed = str(walk_speed).replace('.','_')
    nodes_save_table_new = f'santiago_nodesproximity_{str_walk_speed}_kmh'
    nodes_save_table_old = f'santiago_nodesproximity_{str_walk_speed}_kmh_old'
    source_speed_list = source_list.copy()


for source in source_list:

    # aup.log(f"--- Starting nodes proximity to pois for source {i}/{k}: {source}. ")
    aup.log(f"--- Starting download for nodes proximity for source {source}. ")
    # Read pois from source
    query = f"SELECT * FROM {save_schema}.{nodes_save_table_old} WHERE \"source\" = \'{source}\'"
    nodes_source = aup.gdf_from_query(query, geometry_col='geometry')
    nodes_source['source_15min'] = nodes_source['source_15min'].astype(int)
    aup.log(f"--- Downloaded {len(nodes_source)} nodes from source {source}. ")
    

    # upload data
    aup.log(f"--- Starting upload for nodes proximity for source {source}. ")
    aup.gdf_to_db_slow(nodes_source, nodes_save_table_new, save_schema, if_exists='append')
    aup.log(f"--- Uploaded {len(nodes_source)} nodes from source {source}. ")