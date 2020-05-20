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

def load_denue():
	"""
	Load the DENUE into a geoDataFrame

	Returns:
		geopandas.geoDataFrame -- geoDataFrame with the DENUE
	"""
	gdf = gpd.read_file('../data/external/DENUE/denue_00_46321-46531_shp/conjunto_de_datos/denue_inegi_46321-46531_.shp')
	gdf = gdf[(gdf['codigo_act']=="464111")|(gdf['codigo_act']=="464112")]
	logging.info('DENUE loaded')
	return gdf

def load_graph(polygon, city):
	"""
	Download the graph from a given polygon

	Arguments:
		polygon {shapely.polygon} -- Shapely polygon to use as boundary
		city {str} -- Name of the city/metropolitan area to download
	"""
	G = src.download_graph(polygon,city,network_type='all_private',save=True)
	logging.info(f'Graph loaded {city}')
	return G

def run_calculations(gdf_f, node_mapping, g, weights):
	logging.info('looking for seeds')	
	seeds = src.get_seeds(gdf_f, node_mapping)
	logging.info('calculating voronoi assignment')
	voronoi_assignment = src.voronoi_cpu(g, weights, seeds)
	logging.info('Calculating distances')	
	distances = src.get_distances(g,seeds,weights,voronoi_assignment)
	df = pd.DataFrame(node_mapping ,index=[0]).T
	df['dist'] = distances
	logging.info('DataFrame created')	
	nodes, edges = ox.graph_to_gdfs(G)
	nodes = pd.merge(nodes,df,left_index=True,right_index=True)
	return nodes

def load_areas():
	with open('areas.json', 'r') as f:
		distros_dict = json.load(f)
	return distros_dict

def load_polygon(city):
	return gpd.read_file(f"../data/raw/{city}_area.geojson")

if __name__ == "__main__":
	logging.info('Starting the script')	
	gdf = load_denue() #Load the Denue
	areas = load_areas() #Load the metropolitan areas
	for city, data in  areas.items():
		logging.info(f'Starting with {city}')
		area = load_polygon(city) #load the gdf with the polygon to use
		polygon = area['geometry'][0]
		gdf_f = gpd.clip(gdf,area)
		G = load_graph(polygon, city) #load the graph
		gdf_f = src.find_nearest(G,gdf_f) #find nearest nodes to seeds
		gdf_f.to_file(filename='../data/processed/farmacias_{}.geojson'.format(city), driver='GeoJSON') #save the geoDataFrame with the closest nodes
		g, weights, node_mapping = src.to_igraph(G) #convert to igraph to run the calculations
		nodes = run_calculations(gdf_f, node_mapping, g, weights) #run the calculations and return the final dataframe
		nodes.to_file(f"../data/processed/{city}_nodes_distance.geojson", driver='GeoJSON') #save the nodes
		logging.info(f'{city} done.')	