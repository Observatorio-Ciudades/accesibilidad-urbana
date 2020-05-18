################################################################################
# Module: utils.py
# Set of utility functions 
# developed by: Luis Natera @natera
# 			  nateraluis@gmail.com
# updated: 02/01/20
################################################################################

import pandas as pd
import geopandas as gpd
import osmnx as ox
import os
import igraph as ig
import numpy as np
from shapely.geometry import Point, Polygon
from matplotlib.patches import RegularPolygon


def find_nearest(G, gdf):
	"""
	Find the nearest graph nodes to the points in a GeoDataFrame

	Arguments:
		G {networkx.Graph} -- Graph created with OSMnx that contains geographic information (Lat,Lon, etc.)
		gdf {geopandas.GeoDataFrame} -- GeoDataFrame with the points to locate

	Returns:
		geopandas.GeoDataFrame -- GeoDataFrame original dataframe with a new column call 'nearest' with the node id closser to the point
	"""
	gdf['x'] = gdf['geometry'].apply(lambda p: p.x)
	gdf['y'] = gdf['geometry'].apply(lambda p: p.y)
	gdf['nearest'] = ox.get_nearest_nodes(G,list(gdf['x']),list(gdf['y']))
	return gdf

def to_igraph(G):
	"""
	Convert a graph from networkx to igraph

	Arguments:
		G {networkx.Graph} -- networkx Graph to be converted

	Returns:
		igraph.Graph -- Graph with the same number of nodes and edges as the original one
		np.array  -- With the weight of the graph, if the original graph G is from OSMnx the weights are lengths
		dict -- With the node mapping, index is the node in networkx.Graph, value is the node in igraph.Graph
	"""
	node_mapping = dict(zip(G.nodes(),range(G.number_of_nodes())))
	g = ig.Graph(len(G), [(node_mapping[i[0]],node_mapping[i[1]]) for i in G.edges()])
	weights=np.array([float(e[2]['length']) for e in G.edges(data=True)])
	node_id_array=np.array(list(G.nodes())) #the inverse of the node_mapping (the index is the key)
	assert g.vcount() == G.number_of_nodes()
	return g, weights, node_mapping

def get_seeds(gdf, node_mapping):
	"""
	Generate the seed to be used to calculate shortest paths for the Voronoi's

	Arguments:
		gdf {geopandas.GeoDataFrame} -- GeoDataFrame with 'nearest' column
		node_mapping {dict} -- dictionary containing the node mapping from networkx.Graph to igraph.Graph

	Returns:
		np.array -- numpy.array with the set of seeds
	"""
	# Get the seed to calculate shortest paths
	return np.array(list(set([node_mapping[i] for i in gdf['nearest']])))

def haversine(coord1, coord2):
	"""
	Calculate distance between two coordinates in meters with the Haversine formula

	Arguments:
		coord1 {tuple} -- tuple with coordinates in decimal degrees (e.g. 43.60, -79.49)
		coord2 {tuple} -- tuple with coordinates in decimal degrees (e.g. 43.60, -79.49)

	Returns:
		float -- distance between coord1 and coord2 in meters
	"""
	# Coordinates in decimal degrees (e.g. 43.60, -79.49)
	lon1, lat1 = coord1
	lon2, lat2 = coord2
	R = 6371000  # radius of Earth in meters
	phi_1 = np.radians(lat1)
	phi_2 = np.radians(lat2)    
	delta_phi = np.radians(lat2 - lat1)
	delta_lambda = np.radians(lon2 - lon1)    
	a = np.sin(delta_phi / 2.0) ** 2 + np.cos(phi_1) * np.cos(phi_2) * np.sin(delta_lambda / 2.0) ** 2    
	c = 2 * np.arctan2(np.sqrt(a),np.sqrt(1 - a))    
	meters = R * c  # output distance in meters
	km = meters / 1000.0  # output distance in kilometers    
	return meters

def create_hex_grid(gdf, diameter):
	"""
	Generate a hexagonal grid on top of a boundary

	Arguments:
		gdf {geopandas.GeoDataFrame} -- GeoDataFrame with the boundary to use
		diameter {int} -- diameter of the hexagons, in meters 

	Returns:
		geopandas.GeoDataFrame -- GeoDataFrame with the hexbins
	"""
	xmin,ymin,xmax,ymax = gdf.total_bounds # lat-long of 2 corners
	EW = haversine((xmin,ymin),(xmax,ymin)) #East-West extent
	NS = haversine((xmax,ymin),(xmax,ymax)) # North-South extent 
	w = diameter*np.sin(np.pi/3) # horizontal width of hexagon = w = d* sin(60) 
	n_cols = int(EW/w)+1# Approximate number of hexagons per row = EW/w
	n_rows = int(NS/diameter)+1 # Approximate number of hexagons per column = NS/d
	w = (xmax-xmin)/n_cols # width of hexagon
	d = w/np.sin(np.pi/3) #diameter of hexagon
	array_of_hexes = []
	for rows in range(0,n_rows+20): #Have to add +20 to cover all the area
		hcoord = np.arange(xmin,xmax,w) + (rows%2)*w/2
		vcoord = [ymax- rows*d*0.75]*n_cols
		for x, y in zip(hcoord, vcoord):
			hexes = RegularPolygon((x, y), numVertices=6, radius=d/2, alpha=0.2, edgecolor='k')
			verts = hexes.get_path().vertices
			trans = hexes.get_patch_transform()
			points = trans.transform(verts)
			array_of_hexes.append(Polygon(points))
	hex_grid = gpd.GeoDataFrame({'geometry':array_of_hexes},crs=gdf.crs)
	hex_grid = gpd.overlay(hex_grid,gdf)
	hex_grid = gpd.GeoDataFrame(hex_grid,geometry='geometry')
	return hex_grid