import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
import aup
import json
import geopandas as gpd
import osmnx as ox
from config import *

ox.config(data_folder='../data', cache_folder='../data/raw/cache',
          use_cache=True, log_console=True)



def get_graph(polygon, city):
    """
    Download the graph from a given polygon

    Arguments:
        polygon {shapely.polygon} -- Shapely polygon to use as boundary
        city {str} -- Name of the city/metropolitan area to download
    """
    G = aup.download_graph(polygon,city,network_type='all_private',save=True)
    return G

def load_areas():
    with open('areas.json', 'r') as f:
        distros_dict = json.load(f)
    return distros_dict
    
if __name__ == "__main__":
    mpos = aup.load_mpos()
    cities = load_areas()
    for city, data in cities.items():
        try:
            logging.info(f'starting with {city}')
            mpos_temp = mpos[(mpos['NOMGEO'].isin(data['mpos'])) & (mpos['CVE_ENT'].isin(data['edo']))].dissolve(by='layer')
            mpos_temp.to_file(f"../data/raw/{city}_area.geojson", driver='GeoJSON') 
            logging.info('Area Metropolitana loaded')
            polygon = mpos_temp['geometry'][0]
            get_graph(polygon,city) #Solucionar para Tijuana, tiene islas y OSMnx da un error
            logging.info(f'Done with {city}')
        except:
            logging.error(f'Problems with {city}')
            pass