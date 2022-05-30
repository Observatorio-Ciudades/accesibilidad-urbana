################################################################################
# Module: Data downloader
# saves the original network in the folder ../data/raw/{type}_{city}.{format}
# name of the city as key.
# developed by: Luis Natera @natera
# 			  nateraluis@gmail.com
# updated: 25/08/2020
################################################################################
import csv
import json
import os
from io import StringIO

import geopandas as gpd
import osmnx as ox
import pandas as pd
import numpy as np
import psycopg2
from geoalchemy2 import WKTElement
from shapely.geometry import Polygon, MultiLineString, Point, LineString

from . import utils

ox.config(
    data_folder="../data",
    cache_folder="../data/raw/cache",
    use_cache=True,
    log_console=True,
)


def create_polygon(bbox, city, save=True):
    """Create a polygon from a bounding box and save it to a file

    Arguments:
            bbox {list} -- list containing the coordinates of the bounding box [north, south, east, west]

    Keyword Arguments:
            save {bool} -- boolean to save or not the polygon to a file as a GeoJSON (default: {True})

    Returns:
            polygon -- GoeDataFrame with the geometry of the polygon to be used to download the data
    """
    n_w = bbox[3], bbox[0]
    n_e = bbox[2], bbox[0]
    s_w = bbox[3], bbox[1]
    s_e = bbox[2], bbox[1]
    polygon_geom = Polygon((n_w, n_e, s_e, s_w, n_w))
    crs = {"init": "epsg:4326"}
    polygon = gpd.GeoDataFrame(index=[0], crs=crs, geometry=[polygon_geom])
    if save:
        polygon.to_file(
            filename="../data/raw/bbox_{}.geojson".format(city), driver="GeoJSON"
        )
    return polygon


def download_graph(polygon, city, network_type="walk", save=True):
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
        G = ox.load_graphml(
            "../data/raw/network_{}_{}.graphml".format(city, network_type)
        )
        print(f"{city} retrived graph")
        return G
    except:
        print(f"{city} graph not in data/raw")
        G = ox.graph_from_polygon(
            polygon,
            network_type=network_type,
            simplify=True,
            retain_all=False,
            truncate_by_edge=False,
        )
        print("downloaded")
        if save:
            ox.save_graphml(
                G, filename="raw/network_{}_{}.graphml".format(city, network_type)
            )
        return G


def df_to_geodf(df, x, y, crs):
    """Create a GeoDataFrame from a pandas DataFrame

    Arguments:
            df {pandas.DataFrame} -- pandas data frame with lat, lon or x, y, columns
            x {str} -- Name of the column that contains the x or Longitud values
            y {str} -- Name of the column that contains the y or Latitud values
            crs {dict} -- Coordinate reference system to use

    Returns:
            geopandas.GeoDataFrame -- GeoDataFrame with Points as geometry
    """
    df["y"] = df[y].astype(float)
    df["x"] = df[x].astype(float)
    geometry = [Point(xy) for xy in zip(df.x, df.y)]
    return gpd.GeoDataFrame(df, crs=crs, geometry=geometry)


def load_study_areas():
    """
    Load the study areas json as dict

    Returns:
            dict -- dictionary with the study areas and attributes
    """
    with open("areas.json", "r") as f:
        distros_dict = json.load(f)
    return distros_dict


def convert_type(df, data_dict):
    """Converts columns from DataFrame to specified data type
    Args:
        df (pandas.DataFrame): DataFrame containing all columns
        data_dict (dictionary): Dictionary with the desiered data type as a
                                key {string, integer, float} and a list of columns.
                                For example: {'string':[column1,column2],'integer':[column3,column4]}
    Returns:
        pandas.DataFrame: DataFrame with converted data types for columns
    """
    for d in data_dict:
        if d == "string":
            for c in data_dict[d]:
                df[c] = df[c].astype("str")
        else:
            for c in data_dict[d]:
                df[c] = pd.to_numeric(df[c], downcast=d, errors="ignore")

    return df

def create_schema(schema):
    """create schema in the database if it does not exists already,
    otherwise log if the schema already in the DB.

    Args:
        schema (str): String with the name of the schema to create.
    """
    engine = utils.db_engine()
    # Create schema; if it already exists, skip this
    try:
        engine.execute(f"CREATE SCHEMA IF NOT EXISTS {schema.lower()}")
    except Exception as e:
        utils.log(e)
        pass


def df_to_db(df, name, table, schema, if_exists="fail"):
    """Save a dataframe into the database as a table

    Args:
        df (DataFrame): pandas.DataFrame to upload
        name (str): name of the dataframe to upload (used for logs)
        table (str): name of the table to create/append to.

    """

    create_schema(schema)
    table = table.lower()
    schema = schema.lower()
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_NONNUMERIC, sep=",")
    buffer.seek(0)
    conn = utils.connect()
    cursor = conn.cursor()
    utils.log(f"{name} starting upload to: {table}")
    try:
        cursor.copy_expert(
            f"""COPY {schema}.{table} FROM STDIN WITH (FORMAT CSV)""", buffer
        )
        conn.commit()
        utils.log(f"{name} Copy to {schema}.{table} done.")
        buffer = 0
    except (Exception, psycopg2.DatabaseError) as error:
        utils.log(f"{name} Error: {error}")
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
    conn.close()

def df_to_db_slow(df, name, schema, if_exists='fail'):
     """Upload a Pandas.DataFrame to the database
     Args:
         df (pandas.DataFrame): DataFrame to be uploadead
         name (str): Name of the table to be created
         schema (str): Name of the folder in which to save the geoDataFrame
         if_exists (str): Behaivor if the table already exists in the database ('fail', 'replace', 'append') 'fail' by default.
     """
     create_schema(schema)
     utils.log('Getting DB connection')
     engine = utils.db_engine()
     utils.log(f'Uploading table {name} to database')
     df.to_sql(name=name.lower(), con=engine,
               if_exists=if_exists, index=False, schema=schema.lower(), method='multi', chunksize=50000)
     utils.log(f'Table {name} in DB')


def gdf_to_db_slow(gdf, name, schema, if_exists="fail"):
    """Upload a geoPandas.GeoDataFrame to the database

    Args:
        gdf (geopandas.GeoDataFrame): GeoDataFrame to be uploadead
        name (str): Name of the table to be created
        schema (str): Name of the folder in which to save the geoDataFrame
        if_exists (str): Behaivor if the table already exists in the database
        ('fail', 'replace', 'append') 'fail' by default.
    """
    create_schema(schema)
    utils.log("Getting DB connection")
    engine = utils.db_engine()
    utils.log(f"Uploading table {name} to database")
    gdf.to_postgis(
        name=name.lower(),
        con=engine,
        if_exists=if_exists,
        index=False,
        schema=schema.lower(),
    )
    utils.log(f"Table {name} in DB")


def gdf_to_df_geo(gdf):
    """Convert a GeoDataFrame into a DataFrame with the geometry column as text

    Args:
        gdf (geopandas.GeoDataFrame): GeoDataFrame to be converted

    Returns:
        pandas.DataFrame: DataFrame with the geometry as text
    """
    utils.log("Converting GeoDataFrame to DF with wkt")
    gdf["geom"] = gdf["geometry"].apply(lambda x: WKTElement(x.wkt, srid=4326))
    # drop the geometry column as it is now duplicative
    gdf.drop("geometry", 1, inplace=True)
    gdf.rename(columns={"geom": "geometry"}, inplace=True)
    return gdf


def gdf_to_db(gdf, name, schema, if_exists="fail"):
    """Upload a geoPandas.GeoDataFrame to the database

    Args:
        gdf (geopandas.GeoDataFrame): GeoDataFrame to be uploadead
        name (str): Name of the table to be created
        schema (str): Name of the folder in which to save the geoDataFrame
        if_exists (str): String of what to do if the table already exists in the database
        ('fail','append','replace')
    """
    create_schema(schema)
    utils.log("Getting DB connection")
    utils.log(f"Uploading table {name} to database")
    df_geo = gdf_to_df_geo(gdf)
    df_to_db(df_geo, name, name, schema, if_exists=if_exists)
    utils.log(f"Table {schema}.{name} in DB")


def df_from_db(name, schema):
    """Load a table from the database into a DataFrame

    Args:
        name (str): Name of the table to be loaded
        schema (str): Name of the folder from where to load the geoDataFrame

    Returns:
        pandas.DataFrame: GeoDataFrame with the table from the database.
    """
    engine = utils.db_engine()
    utils.log(f"Getting {name} from DB")
    df = pd.read_sql(f"SELECT * FROM {schema.lower()}.{name.lower()}", engine)
    utils.log(f"{name} retrived")
    return df


def df_from_query(query, index_col=None):
    """Load a table from the database into a DataFrame

    Args:
        query (str): SQL query to get the data

    Returns:
        pandas.DataFrame: GeoDataFrame with the table from the database.
    """
    engine = utils.db_engine()
    utils.log("Getting data from DB")
    df = pd.read_sql(query, engine, index_col=index_col)
    utils.log("Data retrived")
    return df


def gdf_from_query(query, geometry_col="geom", index_col=None):
    """Load a table from the database into a GeoDataFrame

    Args:
        query (str): SQL query to get the data

    Returns:
        geopandas.GeoDataFrame: GeoDataFrame with the table from the database.
    """
    engine = utils.db_engine()
    utils.log("Getting data from DB")
    df = gpd.GeoDataFrame.from_postgis(
        query, engine, geom_col=geometry_col, index_col=index_col
    )
    utils.log("Data retrived")
    return df


def gdf_from_db(name, schema):
    """Load a table from the database into a GeoDataFrame

    Args:
        name (str): Name of the table to be loaded
        schema (str): Name of the folder from where to load the geoDataFrame

    Returns:
        geopandas.GeoDataFrame: GeoDataFrame with the table from the database.
    """
    engine = utils.db_engine()
    utils.log(f"Getting {name} from DB")
    gdf = gpd.read_postgis(
        f"SELECT * FROM {schema.lower()}.{name.lower()}", engine, geom_col="geometry"
    )
    utils.log(f"{name} retrived")
    return gdf


def graph_from_hippo(gdf, schema, edges_folder='edges', nodes_folder='nodes'):
    """Download OSMnx edges and nodes from DataBase according to GeoDataFrame boundary

    Args:
        gdf (geopandas.GeoDataFrame): GeoDataFrame polygon boundary for download
        schema (str): schema from DataBase where edges and nodes are stored
        edges_folder (str): folder name whithin schema where edges stored. Defaults to edges
        nodes_folder (str): folder name whithin schema where nodes stored. Defaults to nodes

    Returns:
        networkx.MultiDiGraph -- Graph with edges and nodes from DataBase
		gpd.GeoDataFrame  -- GeoDataFrame for nodes within boundaries
		gpd.GeoDataFrame  -- GeoDataFrame for edges within boundaries
    """

    gdf = gdf.to_crs("EPSG:6372")
    gdf = gdf.buffer(1).reset_index().rename(columns={0: "geometry"})
    gdf = gdf.to_crs("EPSG:4326")
    poly_wkt = gdf.dissolve().geometry.to_wkt()[0]
    edges_query = f"SELECT * FROM {schema}.{edges_folder} WHERE ST_Intersects(geometry, 'SRID=4326;{poly_wkt}')"
    edges = gdf_from_query(edges_query, geometry_col="geometry")

    nodes_id = list(edges.v.unique())
    u = list(edges.u.unique())
    nodes_id.extend(u)
    myset = set(nodes_id)
    nodes_id = list(myset)
    nodes_query = f"SELECT * FROM {schema}.{nodes_folder} WHERE osmid IN {str(tuple(nodes_id))}"
    nodes = gdf_from_query(nodes_query, geometry_col="geometry", index_col="osmid")

    nodes.drop_duplicates(inplace=True)
    edges.drop_duplicates(inplace=True)

    edges = edges.set_index(["u", "v", "key"])

    nodes = nodes.set_crs("EPSG:4326")
    edges = edges.set_crs("EPSG:4326")

    nodes_tmp = nodes.reset_index().copy()

    edges_tmp = edges.reset_index().copy()

    from_osmid = list(set(edges_tmp['u'].to_list()).difference(set(nodes_tmp.osmid.to_list())))

    nodes_dict = nodes_tmp.to_dict()

    for i in from_osmid:
        row = edges_tmp.loc[(edges_tmp.u==i)].iloc[0]
        coords = [(coords) for coords in list(row['geometry'].coords)]
        first_coord, last_coord = [ coords[i] for i in (0, -1) ]
        
        nodes_dict['osmid'][len(nodes_dict['osmid'])] = i
        nodes_dict['x'][len(nodes_dict['x'])] = first_coord[0]
        nodes_dict['y'][len(nodes_dict['y'])] = first_coord[1]
        nodes_dict['street_count'][len(nodes_dict['street_count'])] = np.nan
        nodes_dict['geometry'][len(nodes_dict['geometry'])] = Point(first_coord)
            
        
    to_osmid = list(set(edges_tmp['v'].to_list()).difference(set(list(nodes_dict['osmid'].values()))))

    for i in to_osmid:
        row = edges_tmp.loc[(edges_tmp.u==i)].iloc[0]
        coords = [(coords) for coords in list(row['geometry'].coords)]
        first_coord, last_coord = [ coords[i] for i in (0, -1) ]
        
        nodes_dict['osmid'][len(nodes_dict['osmid'])] = i
        nodes_dict['x'][len(nodes_dict['x'])] = last_coord[0]
        nodes_dict['y'][len(nodes_dict['y'])] = last_coord[1]
        nodes_dict['street_count'][len(nodes_dict['street_count'])] = np.nan
        nodes_dict['geometry'][len(nodes_dict['geometry'])] = Point(last_coord)
        
    nodes_tmp = pd.DataFrame.from_dict(nodes_dict)
    nodes_tmp = gpd.GeoDataFrame(nodes_tmp, crs="EPSG:4326", geometry='geometry')
    nodes = nodes_tmp.copy()
    nodes.set_index('osmid',inplace=True)

    G = ox.graph_from_gdfs(nodes, edges)


    return G, nodes, edges
