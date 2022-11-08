import os
import sys

import pandas as pd
import geopandas as gpd
import numpy as np

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup

def main():

    gdf_mun = gdf_from_db('metro_list', 'metropolis')
    
    for city in gdf_mun.city.unique():
        
        hex_gdf = gpd.GeoDataFrame()
        hex_pop = gpd.GeoDataFrame()
        schema = 'prox_analysis'
        nodes_folder = 'nodes_proximity_2020'
        query = f"SELECT * FROM {schema}.{nodes_folder} WHERE \"metropolis\" LIKE \'{city}\'"
        nodes = aup.gdf_from_query(query, geometry_col='geometry')
        nodes_geom = nodes.drop_duplicates(subset='osmid', keep="last")[['osmid','geometry','metropolis']].copy()
        
        nodes_analysis = nodes_geom.copy()

        for amenidad in list(nodes.amenity.unique()):
            nodes_tmp = nodes.loc[nodes.amenity == amenidad,['osmid','time']]
            nodes_tmp = nodes_tmp.rename(columns={'time':amenidad})
            if nodes_tmp[amenidad].mean() == 0:
                nodes_tmp[amenidad] = -1
            nodes_analysis = nodes_analysis.merge(nodes_tmp, on='osmid')
            
            
        idx_cd_cuidadoras = {'Preescolar':['denue_preescolar'],
                         'Primaria':['denue_primaria'],
                         'Secundaria':['denue_secundaria'],
                     'Salud':['clues_primer_nivel'],
                     'Guarderias':['denue_guarderias'],
                     'Alimentos' : ['denue_supermercado','denue_abarrotes',
                                    'denue_carnicerias','sip_mercado'],
                     'Personal' : ['denue_ropa','denue_peluqueria'],
                     'Parques' : ['sip_cancha','sip_unidad_deportiva','sip_espacio_publico']
             }

        wegiht_idx = {'Preescolar': 1,
                                 'Primaria': 1,
                                 'Secundaria': 1,
                             'Salud': 1,
                             'Guarderias': 1,
                             'Alimentos' : 1,
                             'Personal' : 2,
                             'Parques' : 1
                     }
                     
                # create gdf for analysis
        nodes_mean = nodes_analysis.copy()

        column_idx_names = []
        column_max_names = []

        for e in idx_cd_cuidadoras.keys():
            
            column_idx_names.append('idx_'+ e.lower())
            column_max_names.append('max_'+ e.lower())

            if wegiht_idx[e] < len(idx_cd_cuidadoras[e]):
                nodes_mean['idx_'+ e.lower()] = nodes_mean[idx_cd_cuidadoras[e]].min(axis=1)
                nodes_mean['max_'+ e.lower()] = nodes_mean[idx_cd_cuidadoras[e]].min(axis=1)
                
            else:
                nodes_mean['idx_'+ e.lower()] = nodes_mean[idx_cd_cuidadoras[e]].mean(axis=1)
                nodes_mean['max_'+ e.lower()] = nodes_mean[idx_cd_cuidadoras[e]].max(axis=1)
                
            
        nodes_mean['cd_cuidadoras_idx'] = nodes_mean[column_idx_names].mean(axis=1)
        nodes_mean['cd_cuidadoras_max'] = nodes_mean[column_max_names].max(axis=1)

    
    
    
if __name__ == "__main__":

    SCHEMA = 'censo'
    years = [2010, 2020]

    aup.log('--'*20)
    aup.log('\n Starting script')

    for year in years:
        main(year, SCHEMA, save=True) 