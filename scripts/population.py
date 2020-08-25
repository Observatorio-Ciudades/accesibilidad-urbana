from config import *
import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
	sys.path.append(module_path)
	import aup
import osmnx as ox
import pandas as pd 
import geopandas as gpd
#

def main(city, gdf_pop, columns):
	gdf_area = aup.load_polygon(city)
	G = aup.download_graph(gdf_area,city,network_type='all_private',save=False)
	nodes = ox.graph_to_gdfs(G,edges=False)
	gdf_pop_area = gpd.clip(gdf_pop, gdf_area)
	gdf_pop_area = ox.project_gdf(gdf_pop_area, to_crs=nodes.crs)
	nodes = aup.population_to_nodes(nodes,gdf_pop_area)
	hex_bins = gpd.read_file(f'../data/processed/{city}_hex_bins.geojson')
	hex_temp = hex_bins[[f'hex_id_{resolution}', 'geometry']]
	nodes.drop('index_right', axis=1, inplace=True)
	hex_temp = ox.utils_geo.projection.project_gdf(hex_temp, to_crs=nodes.crs)
	hex_temp = gpd.sjoin(nodes, hex_temp)
	hex_temp = hex_temp.groupby(f'hex_id_{resolution}').sum()
	hex_temp = hex_temp[columns]
	hex_bins = pd.merge(hex_bins, hex_temp, right_index=True,left_on=f'hex_id_{resolution}', how='left').fillna(0)
	hex_bins.to_file(f"../data/processed/{city}_hex_bins.geojson", driver='GeoJSON')
if __name__ == "__main__":
	areas = aup.load_study_areas()  # Load the metropolitan areas
	gdf_pop, columns = aup.load_population()
	for city in areas:
		main(city,gdf_pop,columns)
