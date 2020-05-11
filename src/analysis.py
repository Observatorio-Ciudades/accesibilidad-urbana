################################################################################
# Module: analysis.py
# Set of utility functions 
# developed by: Luis Natera @natera
# 			  nateraluis@gmail.com
# updated: 08/05/2020
################################################################################

import igraph as ig
import numpy as np

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