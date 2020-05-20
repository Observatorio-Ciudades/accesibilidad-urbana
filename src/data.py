################################################################################
# Module: Data downloader
# saves the original network in the folder ../data/raw/{type}_{city}.{format}
# name of the city as key.
# developed by: Luis Natera @natera
# 			  nateraluis@gmail.com
# updated: 08/05/2020
################################################################################
import osmnx as ox
from .utils import *
from shapely.geometry import Polygon

ox.config(data_folder='../data', cache_folder='../data/raw/cache',
          use_cache=True, log_console=True)
		  
def create_polygon(bbox, city, save=True):
	"""Create a polygon from a bounding box and save it to a file
	
	Arguments:
		bbox {list} -- list containing the coordinates of the bounding box [north, south, east, west]
	
	Keyword Arguments:
		save {bool} -- boolean to save or not the polygon to a file as a GeoJSON (default: {True})
	
	Returns:
		polygon -- GoeDataFrame with the geometry of the polygon to be used to download the data
	"""	
	n_w = bbox[3],bbox[0]
	n_e = bbox[2],bbox[0]
	s_w = bbox[3],bbox[1]
	s_e = bbox[2],bbox[1]
	polygon_geom = Polygon((n_w,n_e,s_e,s_w,n_w))
	crs = {'init': 'epsg:4326'}
	polygon = gpd.GeoDataFrame(index=[0], crs=crs, geometry=[polygon_geom])
	if save:
		polygon.to_file(filename='../data/raw/bbox_{}.geojson'.format(city), driver='GeoJSON')
	return polygon

def download_graph(polygon, city, network_type='walk', save=True):
	"""Download a graph from a bounding box, and saves it to disk
	
	Arguments:
		polygon {polygon} -- polygon to use as boundary to download the network
		city {str} -- string with the name of the city
	
	Keyword Arguments:
		network_type {str} -- String with the type of network to download (drive, walk, bike, all_private, all) for more details see OSMnx documentation
		save {bool} -- Save the graph to disk or not (default: {True})
	
	Returns:
		nx.MultiDiGraph
	"""	
	try:
		G = ox.load_graphml('raw/network_{}_{}.graphml'.format(city,network_type))
	except:
		G = ox.graph_from_polygon(polygon,network_type=network_type, simplify=True, retain_all=False, truncate_by_edge=False, name=city)
	if save:
		ox.save_graphml(G, filename='raw/network_{}_{}.graphml'.format(city,network_type))
	return G

def df_to_geodf(df, x, y, crs):
	"""Create a geo data frame from a pandas data frame
	
	Arguments:
		df {pandas.DataFrame} -- pandas data frame with lat, lon or x, y, columns
		x {str} -- Name of the column that contains the x or Longitud values
		y {str} -- Name of the column that contains the y or Latitud values
		crs {dict} -- Coordinate reference system to use
	
	Returns:
		geopandas.GeoDataFrame -- GeoDataFrame with Points as geometry
	"""    
	df['y'] = df[y].astype(float)
	df['x'] = df[x].astype(float)
	geometry = [Point(xy) for xy in zip(df.x, df.y)]
	return gpd.GeoDataFrame(df, crs=crs, geometry=geometry)