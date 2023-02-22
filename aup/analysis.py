################################################################################
# Module: analysis.py
# Set of utility functions 
# developed by: Luis Natera @natera
# 			  nateraluis@gmail.com
# updated: 25/08/2020
################################################################################

import igraph as ig
import numpy as np
import networkx as nx
from .utils import *
from .data import *
import math
from scipy import optimize
from tqdm import tqdm
import rasterio
from pystac.extensions.eo import EOExtension as eo
import planetary_computer as pc
from scipy import stats as st
from rasterio.merge import merge


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

def group_by_hex_mean(nodes, hex_bins, resolution, col_name, osmid=True):
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
	if osmid:
		hex_new = hex_new.drop(['index_right','osmid'],axis=1)
	else:
		hex_new = hex_new.drop(['index_right'],axis=1)
	hex_new[dist_col].apply(lambda x: x+1 if x==0 else x )
	hex_new.fillna(0, inplace=True)
	return hex_new

def socio_polygon_to_points(
	nodes,
	gdf_socio,
	column_start=0,
	column_end=-1,
	cve_column="CVEGEO",
	avg_column=None,
):
	"""
	Assign the proportion of sociodemographic data from polygons to points
	Args:
		nodes {geopandas.GeoDataFrame} -- GeoDataFrame with the nodes to group
		gdf_socio {geopandas.GeoDataFrame} -- GeoDataFrame with the sociodemographic attributes of each AGEB
		column_start (int, optional): Column position were sociodemographic data starts in gdf_population. Defaults to 0.
		column_end (int, optional): Column position were sociodemographic data ends in gdf_population. Defaults to -1.
		cve_column (str, optional): Column name with unique code for identification. Defaults to "CVEGEO".
		avg_column (list, optional): Column name lists with data to average and not divide. Defaults to None.
	Returns:
		geopandas.GeoDataFrame -- nodes GeoDataFrame with the proportion of population by nodes in the AGEB
	"""

	if column_end == -1:
		column_end = len(list(gdf_socio.columns))

	if avg_column is None:
		avg_column = []
	totals = (
		gpd.sjoin(nodes, gdf_socio)
		.groupby(cve_column)
		.count()
		.rename(columns={"x": "nodes_in"})[["nodes_in"]]
		.reset_index()
	)  # caluculate the totals
	# get a temporal dataframe with the totals and columns
	temp = pd.merge(gdf_socio, totals, on=cve_column)
	# get the average for the values
	for col in temp.columns.tolist()[column_start:column_end]:
		if col not in avg_column:
			temp[col] = temp[col] / temp["nodes_in"]
	temp = temp.set_crs("EPSG:4326")
	nodes = gpd.sjoin(nodes, temp)
	nodes.drop(["nodes_in", "index_right"], axis=1, inplace=True)  # drop the nodes_in column
	return nodes  # spatial join the nodes with the values

def socio_points_to_polygon(
	gdf_polygon,
	gdf_socio,
	cve_column,
	string_columns,
	wgt_dict=None,
	avg_column=None):

	"""Group sociodemographic point data in polygons
    Args:
        gdf_polygon (geopandas.GeoDataFrame): GeoDataFrame polygon where sociodemographic data will be grouped
        gdf_socio (geopandas.GeoDataFrame): GeoDataFrame points with sociodemographic data
        cve_column (str): Column name with polygon id in gdf_polygon.
        string_columns (list): List with column names for string data in gdf_socio.
        column_start (int, optional): Column position were sociodemographic data starts in gdf_socio. Defaults to 0.
        column_end (int, optional): Column position were sociodemographic data ends in gdf_socio. Defaults to -1.
        wgt_dict (dict, optional): Dictionary with average column names and weight column names for weighted average. Defaults to None.
        avg_column (list, optional): List with column names with average data. Defaults to None.
    Returns:
        pandas.DataFrame: DataFrame with group sociodemographic data and polygon id

	"""

	dictionary_list = []
	# Adds census data from points to polygon
	gdf_tmp = gpd.sjoin(gdf_socio, gdf_polygon)  # joins points to polygons

	# convert data types
	all_columns = list(gdf_socio.columns)
	numeric_columns = [x for x in all_columns if x not in string_columns]
	type_dict = {"string": string_columns, "float": numeric_columns}
	gdf_tmp = convert_type(gdf_tmp, type_dict)

	#group sociodemographic points to polygon
	for idx in gdf_tmp[cve_column].unique():

		socio_filter = gdf_tmp.loc[gdf_tmp[cve_column]==idx].copy()

		dict_tmp = group_sociodemographic_data(socio_filter, numeric_columns,
		avg_column=avg_column, avg_dict=wgt_dict)
		
		dict_tmp[cve_column] = idx
		
		dictionary_list.append(dict_tmp)
	
	data = pd.DataFrame.from_dict(dictionary_list)

	return data

def group_sociodemographic_data(df_socio, numeric_cols, avg_column=None, avg_dict=None):
	
	"""
    Aggregate sociodemographic variables from DataFrame.
    Args:
        df_socio {pd.DataFrame}: DataFrame containing sociodemographic variables to be aggregated by sum or mean.
        column_start (int, optional): Column number were sociodemographic variables start at DataFrame. Defaults to 1.
        column_end (int, optional): Column number were sociodemographic variables end at DataFrame. Defaults to -1.
        avg_column (list, optional): List of column names to be averaged and not sum. Defaults to None.
        avg_dict (dictionary, optional): Dictionary containing column names to average and
                                            column with which a weighted average will be crated. Defaults to None.
    Returns:
        pd.DataFrame: DataFrame with sum and mean values for sociodemographic data
    """
	# column names with sociodemographic data
	if 'geometry' in numeric_cols:
		numeric_cols.remove('geometry')
	socio_cols = numeric_cols

	# Dictionary to store aggregated variables
	group_dict = {}

	if avg_column is None:
		# creates empty lists to avoid crash for None
		avg_column = []
		avg_dict = []
	# iterate over columns: mean or sum
	for col in socio_cols:
		if col in avg_column:
			# creates weighted averages
			pop_weight = df_socio[avg_dict[col]].sum()
			if pop_weight == 0:
				group_dict[col] = 0
			else:
				tmp_df = df_socio[[avg_dict[col], col]].groupby(col).sum().reset_index()
				tmp_df["weight"] = tmp_df[col] * tmp_df[avg_dict[col]]
				tmp_df["wavg"] = tmp_df["weight"] / pop_weight
				group_dict[col] = tmp_df["wavg"].sum()
		else:
			group_dict[col] = df_socio[col].sum()

	return group_dict

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


def create_network(nodes, edges):

	"""
	Create a network based on nodes and edges without unique ids and to - from attributes.

	Arguments:
		nodes {geopandas.GeoDataFrame} -- GeoDataFrame with nodes for network in EPSG:4326
		edges {geopandas.GeoDataFrame} -- GeoDataFrame with edges for network in EPSG:4326

	Returns:
		geopandas.GeoDataFrame  -- nodes GeoDataFrame with unique ids based on coordinates named osmid in EPSG:4326
		geopandas.GeoDataFrame  -- edges GeoDataFrame with to - from attributes based on nodes ids named u and v respectively in EPSG:4326
	"""

	#Copy edges and nodes to avoid editing original GeoDataFrames
	nodes = nodes.copy()
	edges = edges.copy()

	#Create unique ids for nodes and edges
	##Change coordinate system to meters for unique ids
	nodes = nodes.to_crs("EPSG:6372")
	edges = edges.to_crs("EPSG:6372")

	##Unique id for nodes based on coordinates
	nodes['osmid'] = ((nodes.geometry.x).astype(int)).astype(str)+((nodes.geometry.y).astype(int)).astype(str)

	##Set columns in edges for to [u] from[v] columns
	edges['u'] = np.nan
	edges['v'] = np.nan
	edges.u.astype(str)
	edges.v.astype(str)

	##Extract start and end coordinates for [u,v] columns
	for index, row in edges.iterrows():
		
		edges.at[index,'u'] = str(int(list(row.geometry.coords)[0][0]))+str(int(list(row.geometry.coords)[0][1]))
		edges.at[index,'v'] = str(int(list(row.geometry.coords)[-1][0]))+str(int(list(row.geometry.coords)[-1][1]))

	#Add key column for compatibility with osmnx
	edges['key'] = 0

	#Change [u,v] columns to integer
	edges['u'] = edges.u.astype(int)
	edges['v'] = edges.v.astype(int)
	#Calculate edges lentgh
	edges['length'] = edges.to_crs("EPSG:6372").length
	
	#Change osmid to integer
	nodes['osmid'] = nodes.osmid.astype(int)

	#Transform coordinates
	nodes = nodes.to_crs("EPSG:4326")
	edges = edges.to_crs("EPSG:4326")

	return nodes, edges

	
def gdf_in_hex(grid, gdf, resolution = 10, contain= True):

	"""
	Finds the hexagons that have or do not have a point within

	Arguments:
		grid {geopandas.GeoDataFrame} -- GeoDataFrame with the full H3 hex grid of the city
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
	if contain == False:
		hex_node = hex_merge[hex_merge['_merge']=='right_only']
	## False means it will find hexes with points
	if contain == True:
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
			near[f'hex_id_{resolution}'] = h3.k_ring(idx,1)
			near['a'] = np.nan
			near= near.set_index(f'hex_id_{resolution}')
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

def calculate_isochrone(G, center_node, trip_time, dist_column, subgraph=False):
    """Calculate the isochrone fom the center_node in graph G.
    Args:
        G (networkx.Graph): networkx Graph with travel time (time) attribute.
        center_node (int): id of the node to use
        trip_time (int): maximum travel time allowed
        subgraph (bool, optional): Bool to get the resulting subgraph or only the geometry. Defaults to False.
    Returns:
        sub_G: (optional) subgraph of the covered area.
        geometry: geometry with the covered area
    """
    sub_G = nx.ego_graph(G, center_node, radius=trip_time, distance=dist_column)
    geometry = gpd.GeoSeries([Point((data["x"], data["y"])) for node, data in sub_G.nodes(data=True)]).unary_union.convex_hull
    if subgraph:
        return sub_G, geometry
    else:
        return geometry


def sigmoidal_function(x, di, d0):
	idx_eq = 1 / (1 + math.exp(x * (di - d0)))
	return idx_eq


def sigmoidal_function_constant(positive_limit_value, mid_limit_value):
	tmp_idx = [] # list that stores constant decay values for 0.25 and 0.75

	# calculate 0.75 quarter time
	quarter_limit = mid_limit_value - ((mid_limit_value-positive_limit_value)/2)
	idx_objective = 0.75

	
	def sigmoidal_function(x, di=quarter_limit, d0=mid_limit_value):
		idx_eq = 1 / (1 + math.exp(x * (di - d0)))
		return idx_eq

	def sigmoidal_function_condition(x, di=quarter_limit, d0=mid_limit_value, idx_0=idx_objective):
		return (1 / (1 + math.exp(x * (di - d0)))) - idx_0

	# search for constant decay value in 0.75 quarter_time
	cons = {'type':'eq', 'fun': sigmoidal_function_condition}
	result = optimize.minimize(sigmoidal_function, 0.01, constraints = cons)
	tmp_idx.append(result.x[0])

	# calculate 0.25 quarter time
	quarter_limit = mid_limit_value + ((mid_limit_value-positive_limit_value)/2)
	idx_objective = 0.25

	# search for constant decay value in 0.25 quarter_time
	cons = {'type':'eq', 'fun': sigmoidal_function_condition}
	result = optimize.minimize(sigmoidal_function, 0.01, constraints = cons)
	tmp_idx.append(result.x[0])

	# calculate average constant decay for 0.25 and 0.75
	constant_value_average = sum(tmp_idx) / len(tmp_idx)

	return constant_value_average


def find_asset_by_band_common_name(item, common_name):
    for asset in item.assets.values():
        asset_bands = eo.ext(asset).bands
        if asset_bands and asset_bands[0].common_name == common_name:
            return asset
    raise KeyError(f"{common_name} band not found")

def link_dict(band_name_list, items):
    
    assets_hrefs = {}

    for i in items:
        if i.datetime.date() in list(assets_hrefs.keys()):
            for b in band_name_list:
                assets_hrefs[i.datetime.date()][b].append(pc.sign(find_asset_by_band_common_name(i,b).href))
        else:
            assets_hrefs[i.datetime.date()] = {}
            for b in band_name_list:
                assets_hrefs[i.datetime.date()].update({b:[]})
                assets_hrefs[i.datetime.date()][b].append(pc.sign(find_asset_by_band_common_name(i,b).href))
                
    return assets_hrefs

def filter_links(assets_hrefs, band_name_list):
    max_links_len = st.mode(np.array([len(x[band_name_list[0]]) for x in list(assets_hrefs.values())]))[0][0]
    
    # iterate and remove dates without sufficient data
    for k_date in list(assets_hrefs.keys()):
        # gather data from first band in dictionary - the max value should be the same in all bands
        k_band = list(assets_hrefs[k_date].keys())[0]
        # compare len of that band to max
        if len(assets_hrefs[k_date][k_band]) != max_links_len:
            # if len is less it indicates that is missing data
            # remove date with missing data
            assets_hrefs.pop(k_date)
    
    return assets_hrefs, max_links_len


def df_date_links(assets_hrefs, start_date, periods):
    # dictionary to dataframe
    df_dates = pd.DataFrame.from_dict(assets_hrefs, orient='Index').reset_index().rename(columns={'index':'date'})
    df_dates['date'] = pd.to_datetime(df_dates['date']).dt.date
    df_dates['year'] = df_dates.apply(lambda row: row['date'].year, axis=1)
    df_dates['month'] = df_dates.apply(lambda row: row['date'].month, axis=1)
    
    df_dates_filtered = pd.DataFrame()
    
    # keep only one data point by month
    for y in df_dates['year'].unique():
        for m in df_dates.loc[df_dates['year']==y,'month'].unique():
            df_dates_filtered = pd.concat([df_dates_filtered,
                                         df_dates.loc[(df_dates['year']==y)&
                                                      (df_dates['month']==m)].sample(1)],
                                          ignore_index=True)
    
    # create full range time dataframe
    df_tmp_dates = pd.DataFrame() # temporary date dataframe
    df_tmp_dates['date'] = pd.date_range(start = start_date,   
                               periods = periods,   # there are 30 periods because range from satelite img goes from 01-01-2020 - 30-06-2022
                               freq = "M") # create date range
    # extract year and month
    df_tmp_dates['year'] = df_tmp_dates.apply(lambda row: row['date'].year, axis=1)
    df_tmp_dates['month'] = df_tmp_dates.apply(lambda row: row['date'].month, axis=1)

    # remove date column for merge
    df_tmp_dates.drop(columns=['date'], inplace=True)

    df_complete_dates = df_tmp_dates.merge(df_dates_filtered, left_on=['year','month'],
                                          right_on=['year','month'], how='left')

    # remove date 
    df_complete_dates.drop(columns='date', inplace=True)
    df_complete_dates.sort_values(by=['year','month'], inplace=True)
    
    missing_months = df_complete_dates.nir.isna().sum()
    
    return df_complete_dates, missing_months


def mosaic_raster(raster_asset_list, tmp_dir='tmp/', upscale=False):
    src_files_to_mosaic = []

    for assets in raster_asset_list:
        src = rasterio.open(assets)
        src_files_to_mosaic.append(src)
        
    mosaic, out_trans = merge(src_files_to_mosaic) # mosaic raster
    
    meta = src.meta
    
    if upscale:
        # save raster
        out_meta = src.meta

        out_meta.update({"driver": "GTiff",
                         "dtype": 'float32',
                         "height": mosaic.shape[1],
                         "width": mosaic.shape[2],
                         "transform": out_trans})
        # write raster
        with rasterio.open(tmp_dir+"mosaic_upscale.tif", "w", **out_meta) as dest:
            dest.write(mosaic)

            dest.close()
        # read and upscale
        with rasterio.open(tmp_dir+"mosaic_upscale.tif", "r") as ds:

            upscale_factor = 1/2

            mosaic = ds.read(
                        out_shape=(
                            ds.count,
                            int(ds.height * upscale_factor),
                            int(ds.width * upscale_factor)
                        ),
                        resampling=Resampling.bilinear
                    )

        ds.close()
    src.close()
    
    return mosaic, out_trans, meta

def clean_mask(geom, dataset='', **mask_kw):
    mask_kw.setdefault('crop', True)
    mask_kw.setdefault('all_touched', True)
    mask_kw.setdefault('filled', False)
    masked, mask_transform = rasterio.mask.mask(dataset=dataset, shapes=(geom,),
                                  **mask_kw)
    return masked

def raster_to_hex(hex_gdf, df_complete_dates, r, index_analysis, city, raster_dir):
    # create empty geodataframe to save ndmi by date
    hex_raster = gpd.GeoDataFrame()

    for d in tqdm(range(len(df_complete_dates)),position=0,leave=True):

        month_ = df_complete_dates.iloc[d]['month']
        year_ = df_complete_dates.iloc[d]['year']

        hex_tmp = hex_gdf.loc[hex_gdf.res==r].copy()

        if type(df_complete_dates.iloc[d].nir)==list:

            # read ndmi file
            raster_file = rasterio.open(f"{raster_dir}{city}_{index_analysis}_{month_}_{year_}.tif")

            hex_tmp = hex_tmp.to_crs(raster_file.crs)

            try:

                hex_tmp[index_analysis] = hex_tmp.geometry.apply(lambda geom: clean_mask(geom, raster_file)).apply(np.ma.mean)
            except:
                hex_tmp[index_analysis] = np.nan

        else:
            hex_tmp[index_analysis] = np.nan

        hex_tmp['month'] = month_
        hex_tmp['year'] = year_

        hex_tmp = hex_tmp.to_crs("EPSG:4326")

        # concatenate into single geodataframe
        hex_raster = pd.concat([hex_raster, hex_tmp], 
            ignore_index = True, axis = 0)

        del hex_tmp
        
    return hex_raster
