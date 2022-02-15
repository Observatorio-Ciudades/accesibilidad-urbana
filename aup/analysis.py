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

def get_distances(g, seeds, weights, voronoi_assignment):
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
	col_weight = f'dist_{amenity_name}'
	seeds = get_seeds(gdf_f, node_mapping, column_name)
	voronoi_assignment = voronoi_cpu(g, weights, seeds)
	distances = get_distances(g,seeds,weights,voronoi_assignment)

	nodes[col_weight] = distances

	nodes.replace([np.inf, -np.inf], np.nan, inplace=True)
	idx = pd.notnull(nodes[col_weight])
	nodes = nodes[idx].copy()

	return nodes

def group_by_hex_mean(nodes, hex_bins, resolution, col_name):
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
	dist_col = col_name
	nodes = nodes.copy()
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