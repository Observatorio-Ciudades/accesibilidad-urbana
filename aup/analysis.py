################################################################################
# Module: analysis.py
# Set of utility functions 
# developed by: Luis Natera @natera
# 			  nateraluis@gmail.com
# updated: 25/08/2020
################################################################################

import igraph as ig
import numpy as np
from .utils import *


def voronoi_cpu(g, weights, seeds):
	"""
	Voronoi diagram calculator for undirected graphs
    Optimized for computational efficiency

	Args:
		g (igraph.Graph): graph object with Nodes and Edges
		weights (numpy.array): array of weights for all edges of length len(V)
		seeds (numpy.array): generator points as numpy array of indices from the node array

	Returns:
		[numpy.array]: numpy.array on len(N) where the location (index) of the node refers to the node, the value is the generator (seed) the respective nodes belongs to.
	"""
	return seeds[np.array(g.shortest_paths_dijkstra(seeds, weights=weights)).argmin(axis=0)]

def get_distances(g,seeds,weights,voronoi_assignment):
	"""
	Distance for the shortest path for each node to the closest seed

	Arguments:
		g {[type]} -- [description]
		seeds {[type]} -- [description]
		weights {[type]} -- [description]
		voronoi_assignment {[type]} -- [description]

	Returns:
		[type] -- [description]
	"""
	shortest_paths = np.array(g.shortest_paths_dijkstra(seeds,weights=weights))
	distances = [np.min(shortest_paths[:,i]) for i in range(len(voronoi_assignment))]
	return distances


def calculate_distance_nearest_poi(gdf_f, nodes, edges, amenity_name, column_name, 
wght='length', max_distance=(0,'distance_node')):
	"""
	Calculate the distance to the shortest path to the nearest POI (in gdf_f) for all the nodes in the network G

	Arguments:
		gdf_f {geopandas.GeoDataFrame} -- GeoDataFrame with the Points of Interest the geometry type has to be shapely.Point
		nodes {geopandas.GeoDataFrame} -- GeoDataFrame with nodes for network analysis
		edges {geopandas.GeoDataFrame} -- GeoDataFrame with edges for network analysis
		amenity_name {str} -- string with the name of the amenity that is used as seed (pharmacy, hospital, shop, etc.) 
		column_name {str} -- column name where the nearest distance index is stored
		wght {str} -- weights column in edges. Defaults to length
		max_distance {tuple} -- tuple containing limits for distance to node and column name that contains that value. Defaults to (0, distance_node)

	Returns:
		geopandas.GeoDataFrame -- GeoDataFrame with geometry and distance to the nearest POI
	"""
	nodes = nodes.copy()
	edges = edges.copy()
	if max_distance[0] > 0:
		gdf_f = gdf_f.loc[gdf_f[max_distance[1]]<=max_distance[0]]
	g, weights, node_mapping = to_igraph(nodes,edges,wght=wght) #convert to igraph to run the calculations
	col_dist = f'dist_{amenity_name}'
	seeds = get_seeds(gdf_f, node_mapping, column_name)
	voronoi_assignment = voronoi_cpu(g, weights, seeds)
	distances = get_distances(g,seeds,weights,voronoi_assignment)

	nodes[col_dist] = distances

	nodes.replace([np.inf, -np.inf], np.nan, inplace=True)
	nodes.dropna(inplace=True)

	return nodes

def group_by_hex_mean(nodes, hex_bins, resolution, amenity_name):
	"""
	Group by hexbin the nodes and calculate the mean distance from the hexbin to the closest amenity

	Arguments:
		nodes {geopandas.GeoDataFrame} -- GeoDataFrame with the nodes to group
		hex_bins {geopandas.GeoDataFrame} -- GeoDataFrame with the hexbins
		resolution {int} -- resolution of the hexbins, used when doing the group by and to save the column
		amenity_name {str} -- string with the name of the amenity that is used as seed (pharmacy, hospital, shop, etc.)

	Returns:
		geopandas.GeoDataFrame -- GeoDataFrame with the hex_id{resolution}, geometry and average distance to amenity for each hexbin
	"""
	dist_col = f'dist_{amenity_name}'
	nodes_in_hex = gpd.sjoin(nodes, hex_bins)
	nodes_hex = nodes_in_hex.groupby([f'hex_id_{resolution}']).mean()
	hex_new = pd.merge(hex_bins,nodes_hex,right_index=True,left_on=f'hex_id_{resolution}',how = 'outer')
	hex_new = hex_new.drop(['index_right','osmid'],axis=1)
	hex_new[dist_col].apply(lambda x: x+1 if x==0 else x )
	hex_new.fillna(0, inplace=True)
	return hex_new

def population_to_nodes(nodes, gdf_population, column_start=1, column_end=-1, cve_column='CVEGEO'):
	"""
	Assign the proportion of population to each node inside an AGEB

	Arguments:
		nodes {geopandas.GeoDataFrame} -- GeoDataFrame with the nodes to group
		gdf_population {geopandas.GeoDataFrame} -- GeoDataFrame with the population attributes of each AGEB

	Returns:
		geopandas.GeoDataFrame -- nodes GeoDataFrame with the proportion of population by nodes in the AGEB
	"""
	totals = gpd.sjoin(nodes, gdf_population).groupby(cve_column).count().rename(
		columns={'x': 'nodes_in'})[['nodes_in']].reset_index()  # caluculate the totals
	# get a temporal dataframe with the totals and columns
	temp = pd.merge(gdf_population, totals, on=cve_column)
	for col in temp.columns.tolist()[column_start:column_end]:  # get the average for the values
		temp[col] = temp[col]/temp['nodes_in']
	temp = temp.set_crs("EPSG:4326")
	nodes = gpd.sjoin(nodes, temp)
	nodes.drop(['nodes_in','index_right'], axis=1, inplace=True)  # drop the nodes_in column
	return  nodes # spatial join the nodes with the values

def walk_speed(edges_elevation):

	"""
	Calculates the Walking speed Using Tobler's Hiking Function and the slope in edges

	Arguments:
		edges_elevation {geopandas.GeoDataFrame} -- GeoDataFrame with the street edges with slope data
		

	Returns:
		geopandas.GeoDataFrame -- edges_speed GeoDataFrame with the edges with an added column for speed
	"""
	edges_speed = edges_elevation.copy()
	edges_speed['walkspeed'] = edges_speed.apply(lambda row : (4*np.exp(-3.5*abs((row['grade'])))), axis=1)
	##To adapt to speed at 0 slope = 3.5km/hr use: (4.2*np.exp(-3.5*abs((row['grade']+0.05))))
	#Using this the max speed 4.2 at -0,05 slope
	return edges_speed

def gdf_in_hex(grid, gdf, resolution, contain):

	"""
	Finds the hexagons that have or do not have a point within

	Arguments:
		grid {geopandas.GeoDataFrame} -- GeoDataFrame with the full hex grid of the city
		resolution {int} -- resolution of the hexbins, used when doing the group by and to save the column
		gdf {geopandas.GeoDataFrame} -- GeoDataFrame of figures to be overlaid with hexes
		contain {str} -- True == hexes that have at least a point / False == hexes that DO NOT contain at least a point

		
	Returns:
		geopandas.GeoDataFrame -- gdf_in_hex -- hexes that contain or do not contain a gdf within
	"""
	#PIP (Point in Polygon). Overlays gdf with hexes to find hexes that have the gdf in them and those that do not
	pip = gpd.overlay(grid, gdf, how='intersection', keep_geom_type=False)
	pip = pip.set_index(f'hex_id_{resolution}')
	grid = grid.set_index(f'hex_id_{resolution}')
	#simplify and keep only relevant columns
	pip_idx = pip.drop(['geometry'], axis=1)
	hex_geom = grid[['geometry']]
	#Merge with indicator. Right only means that the hexagon does NOT have any node
	hex_merge = pip_idx.merge(hex_geom, left_index=True, right_index=True, how='outer', indicator=True)
	## False means it will find hexes without points
	if contain == 'False':
		hex_node = hex_merge[hex_merge['_merge']=='right_only']
	## False means it will find hexes with points
	if contain == 'True':
		hex_node = hex_merge[hex_merge['_merge']=='both']
	#(simplify)
	point_hex = gpd.GeoDataFrame(hex_node, geometry = 'geometry')
	point_hex = point_hex[['geometry']]
	point_hex.reset_index(inplace = True)
	grid.reset_index(inplace = True)
	point_hex = point_hex.drop_duplicates(subset=[f'hex_id_{resolution}'])

	return point_hex


def fill_hex(missing_hex, data_hex, resolution, data_column):

	"""
	Fills hexagons with no data with the average data of neighbors (note: hex id must be a column not index)

	Arguments:
		missing_hex {geopandas.GeoDataFrame} -- GeoDataFrame with the hexes that does not have the information due to lack of nodes
		data_hex {geopandas.GeoDataFrame} -- GeoDataFrame of hexes that do contain the information
		resolution {int} -- resolution of the hexbins, used when doing the group by and to save the column
		data_column {str} -- Name of the column with the data to be filled with (ex. distance)

		
	Returns:
		geopandas.GeoDataFrame -- full_hex -- hexgrid filled with relevant data
	"""
	missing_hex[[f'{data_column}']] = np.nan
	urb_hex = gpd.GeoDataFrame()
	urb_hex = data_hex.append(missing_hex)
	urb_hex = urb_hex.set_index(f'hex_id_{resolution}')
	## Start looping
	count = 0
	iter = 1
	urb_hex[f'{data_column}'+ str(count)] = urb_hex[f'{data_column}'].copy()
	while urb_hex[f'{data_column}'+str(count)].isna().sum() > 0:
		missing = urb_hex[urb_hex[f'{data_column}'+str(count)].isna()]
		urb_hex[f'{data_column}'+ str(iter)] = urb_hex[f'{data_column}'+str(count)].copy()
		for idx,row in missing.iterrows():
			###Cell 1
			near = pd.DataFrame(h3.k_ring(idx,1))
			near['hex_id_10'] = h3.k_ring(idx,1)
			near['a'] = np.nan
			near= near.set_index('hex_id_10')
			###Cell 2
			neighbors = near.merge(urb_hex, left_index=True, right_index=True, how='left')
			#Cell 3
			average = neighbors[f'{data_column}'+str(count)].mean()
			urb_hex.at[idx, f'{data_column}'+str(iter)] = average
		count = count + 1
		iter = iter + 1
	full_hex = urb_hex[['geometry']]
	full_hex[f'{data_column}'] = urb_hex[f'{data_column}'+ str(count)].copy()

	return full_hex

