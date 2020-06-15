import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
import geopandas as gpd
import matplotlib.pyplot as plt
import src
from config import *
import osmnx as ox
import pandas as pd

ox.config(data_folder='../data', cache_folder='../data/raw/cache',
		  use_cache=True, log_console=True)


if __name__ == "__main__":
	logging.info('Starting the script hexplots.py')
	areas = src.load_study_areas() #Load the metropolitan areas
	for city, data in  areas.items():
		logging.info(f'Starting with {city}')
		try:
			area = src.load_polygon(city) #load the gdf with the polygon to use
			logging.info('area loded')
			hex_bins = src.create_hexgrid(area,resolution)
			logging.info('hex created')
			nodes = gpd.read_file(filename='../data/processed/nodes_{}.geojson'.format(city))
			logging.info('nodes loaded')
			hex_bins.to_crs(crs=nodes.crs,inplace=True)
			for amenity_name in amenities:
				hex_new = src.group_by_hex_mean(nodes,hex_bins,resolution, amenity_name)
				logging.info('nodes grouped by')
				try:
					gdf = gpd.read_file(f"../data/processed/{city}_hex_bins.geojson")
					logging.info(f'Previous hex bins file found for {city}')
					hex_new.drop('geometry',axis=1)
					gdf = pd.merge(gdf,hex_new,right_on=f'hex_id_{resolution}',left_on=f'hex_id_{resolution}')
					gdf.to_file(f"../data/processed/{city}_hex_bins.geojson", driver='GeoJSON')
					logging.info(f'Hex bins file updated for {city}')
				except:
					logging.info(f'No previous hex bins file for {city}')
					hex_new.to_file(f"../data/processed/{city}_hex_bins.geojson", driver='GeoJSON')
					logging.info(f'Hex bins from {city} saved')
				polygon = area['geometry'][0]
				G = src.download_graph(polygon,city,network_type='all_private',save=True)
				logging.info('Graph loaded')
				edges = ox.graph_to_gdfs(G, nodes=False)
				edges['highway'] = edges.highway.apply(lambda x: x[0] if type(x)== list else x)
				logging.info('Starting the plotting')
				fig, ax = plt.subplots(1,1,figsize=(10,10))
				src.hex_plot(ax, hex_new, area, edges, column=f'dist_{amenity_name}',title=f'{city}\ndistancia promedio a {amenity_name}', save_png=True, show=False, name=f'{city}_dist_{amenity_name}')
				logging.info(f'Plotting {amenity_name} finished for {city}')
			logging.info(f'Plotting done, script finished for {city}')
		except Exception as e:
			logging.info(f'Error: {e}')