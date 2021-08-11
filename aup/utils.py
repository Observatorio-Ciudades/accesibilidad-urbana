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
from h3 import h3
import shapely
import logging
import datetime as dt
from shapely.geometry import Point, Polygon
from matplotlib.patches import RegularPolygon
import datetime as dt
import logging as lg
import sys

import psycopg2
from sqlalchemy import create_engine

from . import settings


def ts(style="datetime", template=None):
    """Get current timestamp as string.

    Args:
        style (str): format the timestamp with this built-in template.
        Must be one of {'datetime', 'date', 'time'}
        template (str): If not None, format the timestamp with this template instead of
        one of the built-in styles.

    Returns:
        ts (str): The string timestamp
    """
    if template is None:
        if style == "datetime":
            template = "{:%Y-%m-%d %H:%M:%S}"
        elif style == "date":
            template = "{:%Y-%m-%d}"
        elif style == "time":
            template = "{:%H:%M:%S}"
        else:
            raise ValueError(f'unrecognized timestamp style "{style}"')

    ts = template.format(dt.datetime.now())
    return ts


# Logger functions taken from OSMnx
def log(message, level=None, name=None, filename=None):
    """Write a message to the logger.
    This logs to file and/or prints to the console (terminal), depending on
    the current configuration of settings.log_file and settings.log_console.

    Args:
        message (str): The message to log
        level (int): One of the logger.level constants
        name (str): Name of the logger
        filename (str): Name of the log file

    Returns:
        None
    """
    if level is None:
        level = settings.log_level
    if name is None:
        name = settings.log_name
    if filename is None:
        filename = settings.log_filename

    logger = _get_logger(level=level, name=name, filename=filename)
    if level == lg.DEBUG:
        logger.debug(message)
    elif level == lg.INFO:
        logger.info(message)
    elif level == lg.WARNING:
        logger.warning(message)
    elif level == lg.ERROR:
        logger.error(message)


def _get_logger(level=None, name=None, filename=None):
    """Create a logger or return the current one if already instantiated.

    Args:
        level (int): One of the logger.level constants
        name (str): Name of the logger
        filename (str): Name of the log file

   Returns:
        logger : logging.logger
    """
    if level is None:
        level = settings.log_level
    if name is None:
        name = settings.log_name
    if filename is None:
        filename = settings.log_filename

    logger = lg.getLogger(name)

    # if a logger with this name is not already set up
    if not getattr(logger, "handler_set", None):

        # get today's date and construct a log filename
        log_filename = os.path.join(settings.logs_folder, f'{filename}_{ts(style="date")}.log')

        # if the logs folder does not already exist, create it
        if not os.path.exists(settings.logs_folder):
            os.makedirs(settings.logs_folder)

        # create file handler and log formatter and set them up
        handler = lg.FileHandler(log_filename, encoding="utf-8")
        formatter = lg.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.handler_set = True

    return logger


def db_engine():
    """Function to create an engine with Ada

    Returns:
        database engine: sqlalchemy engine
    """
    log("Creating SQL engine")
    return create_engine(
        "postgresql://{user}:{pw}@{url}/{db}".format(
            user=str(settings.user), pw=str(settings.pw), url=str(settings.url), db=str(settings.db)
        )
    )


def connect():
    """Get data base connection

    Returns:
        psycopg2 connect: connection to data base
    """
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(
            database=str(settings.db),
            user=str(settings.user),
            password=str(settings.pw),
            host=str(settings.url),
        )
    except (Exception, psycopg2.DatabaseError) as error:
        log(error)
        sys.exit(1)
    return conn


def get_cursor():
    """Creates a SQL cursor

    Returns:
        psycopg2 cursor: Cursor to interact with the database
    """
    pg_conn = psycopg2.connect(
        database=str(settings.db),
        user=str(settings.user),
        password=str(settings.pw),
        host=str(settings.url),
    )
    cur = pg_conn.cursor()
    return pg_conn, cur


def find_nearest_old(G, gdf, amenity_name):
	"""
	Find the nearest graph nodes to the points in a GeoDataFrame

	Arguments:
		G {networkx.Graph} -- Graph created with OSMnx that contains geographic information (Lat,Lon, etc.)
		gdf {geopandas.GeoDataFrame} -- GeoDataFrame with the points to locate
		amenity_name {str} -- string with the name of the amenity that is used as seed (pharmacy, hospital, shop, etc.)

	Returns:
		geopandas.GeoDataFrame -- GeoDataFrame original dataframe with a new column call 'nearest' with the node id closser to the point
	"""
	gdf['x'] = gdf['geometry'].apply(lambda p: p.x)
	gdf['y'] = gdf['geometry'].apply(lambda p: p.y)
	gdf[f'nearest_{amenity_name}'] = ox.get_nearest_nodes(G,list(gdf['x']),list(gdf['y']))
	return gdf

def find_nearest(G, gdf, return_distance=False):
    """
	Find the nearest graph nodes to the points in a GeoDataFrame

	Arguments:
		G {networkx.Graph} -- Graph created with OSMnx that contains geographic information (Lat,Lon, etc.)
		gdf {geopandas.GeoDataFrame} -- GeoDataFrame with the points to locate
		amenity_name {str} -- string with the name of the amenity that is used as seed (pharmacy, hospital, shop, etc.)

	Returns:
		geopandas.GeoDataFrame -- GeoDataFrame original dataframe with a new column call 'nearest' with the node id closser to the point
	"""
    osmnx_tuple = ox.nearest_nodes(G,list(gdf.geometry.x),list(gdf.geometry.y), return_dist=return_distance)
    
    if return_distance:
        gdf['osmid'] = osmnx_tuple[0]
        gdf['distance_node'] = osmnx_tuple[1]
    else:
        gdf['osmid'] = osmnx_tuple
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
	#node_id_array=np.array(list(G.nodes())) #the inverse of the node_mapping (the index is the key)
	#assert g.vcount() == G.number_of_nodes()
	return g, weights, node_mapping

def get_seeds_old(gdf, node_mapping, amenity_name):
	"""
	Generate the seed to be used to calculate shortest paths for the Voronoi's

	Arguments:
		gdf {geopandas.GeoDataFrame} -- GeoDataFrame with 'nearest' column
		node_mapping {dict} -- dictionary containing the node mapping from networkx.Graph to igraph.Graph

	Returns:
		np.array -- numpy.array with the set of seeds
	"""
	# Get the seed to calculate shortest paths
	return np.array(list(set([node_mapping[i] for i in gdf[f'nearest_{amenity_name}']])))

def get_seeds(gdf, node_mapping, column_name):
	"""
	Generate the seed to be used to calculate shortest paths for the Voronoi's

	Arguments:
		gdf {geopandas.GeoDataFrame} -- GeoDataFrame with 'nearest' column
		node_mapping {dict} -- dictionary containing the node mapping from networkx.Graph to igraph.Graph

	Returns:
		np.array -- numpy.array with the set of seeds
	"""
	# Get the seed to calculate shortest paths
	return np.array(list(set([node_mapping[i] for i in gdf[column_name]])))

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

def create_hexgrid(polygon, hex_res, geometry_col='geometry',buffer=0.000):
	"""
	Takes in a geopandas geodataframe, the desired resolution, the specified geometry column and some map parameters to create a hexagon grid (and potentially plot the hexgrid

	Arguments:
		polygon {geopandas.geoDataFrame} -- geoDataFrame to be used
		hex_res {int} -- Resolution to use

	Keyword Arguments:
		geometry_col {str} -- column in the geoDataFrame that contains the geometry (default: {'geometry'})
		buffer {float} -- buffer to be used (default: {0.000})

	Returns:
		geopandas.geoDataFrame -- geoDataFrame with the hexbins and the hex_id_{resolution} column
	"""
	centroid = list(polygon.centroid.values[0].coords)[0]

	# Explode multipolygon into individual polygons
	exploded = polygon.explode().reset_index(drop=True)

	# Master lists for geodataframe
	hexagon_polygon_list = []
	hexagon_geohash_list = []

	# For each exploded polygon
	for poly in exploded[geometry_col].values:

		# Reverse coords for original polygon
		reversed_coords = [[i[1], i[0]] for i in list(poly.exterior.coords)]

		# Reverse coords for buffered polygon
		buffer_poly = poly.buffer(buffer)
		reversed_buffer_coords = [[i[1], i[0]] for i in list(buffer_poly.exterior.coords)]

		# Format input to the way H3 expects it
		aoi_input = {'type': 'Polygon', 'coordinates': [reversed_buffer_coords]}

		# Generate list geohashes filling the AOI
		geohashes = list(h3.polyfill(aoi_input, hex_res))
		for geohash in geohashes:
			polygons = h3.h3_set_to_multi_polygon([geohash], geo_json=True)
			outlines = [loop for polygon in polygons for loop in polygon]
			polyline_geojson = [outline + [outline[0]] for outline in outlines][0]
			hexagon_polygon_list.append(shapely.geometry.Polygon(polyline_geojson))
			hexagon_geohash_list.append(geohash)

	# Create a geodataframe containing the hexagon geometries and hashes
	hexgrid_gdf = gpd.GeoDataFrame()
	hexgrid_gdf['geometry'] = hexagon_polygon_list
	id_col_name = 'hex_id_' + str(hex_res)
	hexgrid_gdf[id_col_name] = hexagon_geohash_list
	hexgrid_gdf.crs = {'init' :'epsg:4326'}

	# Drop duplicate geometries
	geoms_wkb = hexgrid_gdf["geometry"].apply(lambda geom: geom.wkb)
	hexgrid_gdf = hexgrid_gdf.loc[geoms_wkb.drop_duplicates().index]

	return hexgrid_gdf