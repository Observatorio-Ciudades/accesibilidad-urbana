################################################################################
# Module: analysis.py
# Set of utility functions 
# developed by: Luis Natera @natera
# 			  nateraluis@gmail.com
# updated: 08/05/2020
################################################################################

import igraph as ig
import numpy as np
from .utils import *

def voronoi_cpu(g, weights, seeds):
    """
    Voronoi diagram calculator for undirected graphs
    Optimized for computational efficiency

    g - igraph.Graph object (N, V)
    weights - array of weights for all edges of length len(V)(numpy.ndarray)
    generators - generator points as a numpy array of indeces from the node array

    Returns:
    numpy array of length len(N). Location (index) refers to the node,
    the value is the generator point the respective node belongs to.
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

def calculate_distance_nearest_poi(gdf_f, G):
	"""
	Calculate the distance to the shortest path to the nearest POI (in gdf_f) for all the nodes in the network G

	Arguments:
		gdf_f {geopandas.geoDataFrame} -- geoDataFrame with the Points of Interest the geometry type has to be shapely.Point
		G {networkx.MultiDiGraph} -- Graph created with OSMnx

	Returns:
		geopandas.GeoDataFrame -- geoDataFrame with geometry and distance to the nearest POI
	"""
	g, weights, node_mapping = to_igraph(G) #convert to igraph to run the calculations
	seeds = get_seeds(gdf_f, node_mapping)
	voronoi_assignment = voronoi_cpu(g, weights, seeds)
	distances = get_distances(g,seeds,weights,voronoi_assignment)
	df = pd.DataFrame(node_mapping ,index=[0]).T
	df['dist'] = distances
	nodes, edges = ox.graph_to_gdfs(G)
	nodes = pd.merge(nodes,df,left_index=True,right_index=True)
	nodes.drop([i for i in nodes.columns if i not in ['geometry','dist','osmid']],axis=1,inplace=True)
	return nodes

def group_by_hex_mean(nodes, hex_bins, resolution):
	"""
	Group by hexbin the nodes and calculate the mean distance from the hexbin to the closest pharmacy

	Arguments:
		nodes {geopandas.geoDataFrame} -- geoDataFrame with the nodes to group
		hex_bins {geopandas.geoDataFrame} -- geoDataFrame with the hexbins
		resolution {int} -- resolution of the hexbins, used when doing the group by and to save the column

	Returns:
		geopandas.geoDataFrame -- geoDataFrame with the hex_id{resolution}, geometry and average distance to pharmacy for each hexbin
	"""
	nodes_in_hex = gpd.sjoin(nodes, hex_bins)
	nodes_hex = nodes_in_hex.groupby([f'hex_id_{resolution}']).mean()
	hex_new = pd.merge(hex_bins,nodes_hex,right_index=True,left_on=f'hex_id_{resolution}',how = 'outer')
	hex_new = hex_new.drop('index_right',axis=1)
	hex_new.dist.apply(lambda x: x+1 if x==0 else x )
	hex_new.fillna(0, inplace=True)
	return hex_new