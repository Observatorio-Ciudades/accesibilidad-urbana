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

ox.config(data_folder='../data', cache_folder='../data/raw/cache',
		  use_cache=True, log_console=True)



if __name__ == "__main__":
	logging.info('Starting the script hexplots.py')
	areas = src.load_study_areas() #Load the metropolitan areas
	for city, data in  areas.items():
		logging.info(f'Starting with {city}')
		area = src.load_polygon(city) #load the gdf with the polygon to use
		logging.info('area loded')
		hex_bins = src.create_hexgrid(area,resolution)
		logging.info('hex created')
		nodes = gpd.read_file(filename='../data/processed/nodes_{}.geojson'.format(city))
		logging.info('nodes loaded')
		hex_bins.to_crs(crs=nodes.crs,inplace=True)
		hex_new = src.group_by_hex_mean(nodes,hex_bins,resolution)
		logging.info('nodes grouped by')
		hex_new.to_file(f"../data/processed/{city}_hex_bins.geojson", driver='GeoJSON')
		logging.info(f'Hex bins from {city} saved')
		polygon = area['geometry'][0]
		G = src.download_graph(polygon,city,network_type='all_private',save=True)
		logging.info('Graph loaded')
		edges = ox.graph_to_gdfs(G, nodes=False)
		edges['highway'] = edges.highway.apply(lambda x: x[0] if type(x)== list else x)
		logging.info('Starting the plotting')
		fig, ax = plt.subplots(1,1,figsize=(10,10))
		src.hex_plot(ax, hex_new, area, edges, column='dist',title=f'{city}\ndistancia promedio a farmacia', save_png=True, show=False, name=f'{city}_dist_farmacias')
		logging.info(f'Plotting done, script finished for {city}')