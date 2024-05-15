import os
import sys

import pandas as pd
import geopandas as gpd
import osmnx as ox
import numpy as np

from shapely import wkt

import matplotlib.pyplot as plt
import seaborn as sns

from pandas.api.types import CategoricalDtype

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

module_path = os.path.abspath(os.path.join('../'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup

cell = '85d'
#next_cells = []
#done_cells = ['80d','813','86d','815','83f','841','85b','86f','865','86b','8f5'(2522s),
# '867','869','85f','859','843']
#failed_cells = []

chunksize = 5000000

directory = f"../data/external/buildings_footprints/{cell}_buildings.csv"
aup.log(f'Started reading file for cell {cell}')

cont = 1

for buildings_tmp in pd.read_csv(directory, chunksize=chunksize):
	if cont <= 4:
		cont += 1
		continue
	# process each chunk here
	aup.log(f'Finished reading file for iteration {cont}')

	buildings_tmp['geometry'] = buildings_tmp['geometry'].apply(wkt.loads)
	buildings_gdf = gpd.GeoDataFrame(buildings_tmp, crs='epsg:4326')
	del buildings_tmp
	buildings_gdf['cell'] = cell

	aup.log('Finished assigning geometry')
	limit_len = 500000
	if len(buildings_gdf)>limit_len:
		c_upload = len(buildings_gdf)/limit_len
		for k in range(int(c_upload)+1):
			aup.log(f"Starting range k = {k} of {int(c_upload)}")
			gdf_inter_upload = buildings_gdf.iloc[int(limit_len*k):int(limit_len*(1+k))].copy()
			aup.gdf_to_db_slow(gdf_inter_upload,'buildings_google_v3','google_buildings',if_exists="append")
	else:
		aup.log('Starting upload of all data')
		aup.gdf_to_db_slow(buildings_gdf,'buildings_google_v3','google_buildings',if_exists="append")

	cont += 1

	del buildings_gdf
	del gdf_inter_upload
