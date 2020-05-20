import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
	sys.path.append(module_path)
import src
import json
import geopandas as gpd
import osmnx as ox
import networkx as nx
import igraph as ig
import pandas as pd
from config import *

ox.config(data_folder='../data', cache_folder='../data/raw/cache',
		  use_cache=True, log_console=True)

if __name__ == "__main__":
	logging.info('Starting the script distance.py')	
	gdf = src.load_farmacias_denue() #Load the Denue
	areas = src.load_study_areas() #Load the metropolitan areas
	for city, data in  areas.items():
		logging.info(f'Starting with {city}')
		area = src.load_polygon(city) #load the gdf with the polygon to use
		polygon = area['geometry'][0]
		gdf_f = gpd.clip(gdf,area)
		G = src.download_graph(polygon,city,network_type='all_private',save=True) #load the graph
		try:
			gdf_f = gpd.read_file('../data/processed/farmacias_{}.geojson'.format(city))
			logging.info(f'{city} data already in system')
		except:
			logging.info(f'{city} data not in system, starting to look for nearest nodes')
			gdf_f = src.find_nearest(G,gdf_f) #find nearest nodes to seeds
			gdf_f.to_file(filename='../data/processed/farmacias_{}.geojson'.format(city), driver='GeoJSON') #save the geoDataFrame with the closest nodes
			logging.info(f'{city} farmacias data saved')
		try:
			nodes = gpd.read_file('../data/processed/nodes_{}.geojson'.format(city))
		except:
			nodes = src.calculate_distance_nearest_poi(gdf,G) #run the calculations and return the final dataframe
			nodes.to_file(filename='../data/processed/nodes_{}.geojson'.format(city), driver='GeoJSON') #save the nodes
		logging.info(f'{city} done.')	