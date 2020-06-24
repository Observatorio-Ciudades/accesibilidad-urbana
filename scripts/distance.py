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
	areas = src.load_study_areas() #Load the metropolitan areas
	for city, data in  areas.items():
		try:
			logging.info(f'Starting with {city}')
			area = src.load_polygon(city) #load the gdf with the polygon to use
			polygon = area['geometry'][0]
			G = src.download_graph(polygon,city,network_type='all_private',save=True) #load the graph
			for amenity_name in amenities:
				try:
					gdf_f = gpd.read_file('../data/processed/{}_{}.geojson'.format(amenity_name,city))
					logging.info(f'{city} {amenity_name} data already in system')
				except:
					gdf = src.load_denue(amenity_name) #Load the Denue
					gdf_f = gpd.clip(gdf,area)
					logging.info(f'{city} {amenity_name} data not in system, starting to look for nearest nodes')
					gdf_f = src.find_nearest(G,gdf_f,amenity_name) #find nearest nodes to seeds
					gdf_f.to_file(filename='../data/processed/{}_{}.geojson'.format(amenity_name, city), driver='GeoJSON') #save the geoDataFrame with the closest nodes
					logging.info(f'{city} {amenity_name} nearest nodes saved.')
				logging.info(f'{city} {amenity_name}, calculating distances to nearest POI')
				nodes = src.calculate_distance_nearest_poi(gdf_f,G,amenity_name, city) #run the calculations and return the final dataframe
				nodes.to_file(filename='../data/processed/nodes_{}.geojson'.format(city), driver='GeoJSON') #save the nodes
				logging.info(f'{city} {amenity_name} done.')
			logging.info(f'{city} done.')	
		except Exception as e:
			logging.error(f'Error: {e}')	