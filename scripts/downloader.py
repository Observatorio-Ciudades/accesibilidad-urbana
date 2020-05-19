import src
import networkx as nx
from shapely.geometry import Polygon
import numpy as np
import igraph as ig
import matplotlib.pyplot as plt
import pandas as pd
import geopandas as gpd
import osmnx as ox
import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)

ox.config(data_folder='../data', cache_folder='../data/raw/cache',
          use_cache=True, log_console=True)


def load_mpos():
    """
    Load Mexico's municipal boundaries

    Returns:
            geopandas.geoDataFrame -- geoDataFrame with all the Mexican municipal boundaries
    """
    mpos = gpd.read_file(
        '../data/external/LimitesPoliticos/MunicipiosMexico_INEGI19_GCS_v1.shp')
    return mpos
