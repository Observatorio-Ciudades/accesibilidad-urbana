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



def main(city, cvegeo_list, save=False):

    # donwload hexagons with pop data
    hex_pop = gpd.GeoDataFrame()
    hex_folder = 'hex_bins_pop_2020'
    # Iterates over municipality codes for each metropolitan area or capital
    for cvegeo in cvegeo_list:
        # Downloads municipality polygon according to code
        query = f"SELECT * FROM censo.{hex_folder} WHERE \"CVEGEO\" LIKE \'{cvegeo}%%\'"
        hex_tmp = aup.gdf_from_query(query, geometry_col='geometry')
        hex_pop = pd.concat([hex_pop, hex_tmp],
        ignore_index = True, axis = 0)

    pob_tot = hex_pop.pobtot.sum()
    aup.log(f'Downloaded hex data with a total of {pob_tot} persons')

    # calculate population density
    hex_pop = hex_pop.to_crs("EPSG:6372")
    hex_pop['dens_pobha'] = hex_pop['pobtot'] / (hex_pop.area/10000)
    hex_pop = hex_pop.to_crs("EPSG:4326")

    # calculate age groups
    hex_pop['pob_0a14'] = hex_pop['p_0a2'] + hex_pop['p_3a5'] + hex_pop['p_6a11'] + hex_pop['p_12a14']
    hex_pop['pob_15a24'] = hex_pop['p_15a17'] + hex_pop['p_18a24']
    hex_pop['pob_25a59'] = hex_pop['p_18ymas'] - + hex_pop['p_18a24'] - hex_pop['p_60ymas']

    pop_list = ['hex_id_8','pobtot','pobfem','pobmas',
            'pob_0a14','pob_15a24','pob_25a59',
            'p_60ymas','dens_pobha']

    aup.log('Calculated density and age groups')

    # determine missing elements by group

    # download nodes with distance data for a specified city

    nodes_schema = 'prox_analysis'
    nodes_folder = 'nodes_proximity_2020'
    query = f"SELECT * FROM {nodes_schema}.{nodes_folder} WHERE \"metropolis\" LIKE \'{city}\'"
    nodes = aup.gdf_from_query(query, geometry_col='geometry')
    
    aup.log(f'Downloaded a total of {nodes.shape[0]} nodes')

    # preprocess nodes for time analysis 
    
    # delete duplicastes and keep only one point for each node
    nodes_geom = nodes.drop_duplicates(subset='osmid', keep="last")[['osmid','geometry','metropolis']].copy()

    nodes_analysis = nodes_geom.copy()

    # relate time data to each point
    for amenidad in list(nodes.amenity.unique()):

        nodes_tmp = nodes.loc[nodes.amenity == amenidad,['osmid','time']]
        nodes_tmp = nodes_tmp.rename(columns={'time':amenidad})

        if nodes_tmp[amenidad].mean() == 0:
            nodes_tmp[amenidad] = np.nan

        nodes_analysis = nodes_analysis.merge(nodes_tmp, on='osmid')
        
    aup.log(f'Transformed nodes data')

    # define dictionaries for time analysis
       
    idx_15_min = {'Escuelas':{'Preescolar':['denue_preescolar'],
                         'Primaria':['denue_primaria'],
                         'Secundaria':['denue_secundaria']},
             'Servicios comunitarios':{'Salud':['clues_primer_nivel'],
                                      'Gobierno':['sip_centro_admin'],
                                      'Guarderías':['denue_guarderias'],
                                      'Asistencia social':['denue_dif']},
              'Comercio':{'Alimentos':['denue_supermercado','denue_abarrotes',
                                    'denue_carnicerias','sip_mercado'],
                         'Personal':['denue_peluqueria'],
                          'Farmacias':['denue_farmacias'],
                         'Hogar':['denue_ferreteria_tlapaleria','denue_art_limpieza'],
                         'Complementarios':['denue_ropa','denue_calzado','denue_muebles',
                                           'denue_lavanderia','denue_revistas_periodicos',
                                           'denue_pintura']},
              'Entretenimiento':{'Social':['denue_restaurante_insitu','denue_restaurante_llevar',
                                          'denue_bares','denue_cafe'],
                                'Actividad física':['sip_cancha','sip_unidad_deportiva',
                                                   'sip_espacio_publico','denue_parque_natural'],
                                'Cultural':['denue_cines','denue_museos']} 
             }

    wegiht_idx = {'Escuelas':{'Preescolar':1,
                            'Primaria':1,
                            'Secundaria':1},
                'Servicios comunitarios':{'Salud':1,
                                        'Gobierno':1,
                                        'Guarderías':1,
                                        'Asistencia social':1},
                'Comercio':{'Alimentos':1,
                            'Personal':1,
                            'Farmacias':1,
                            'Hogar':2,
                            'Complementarios':6},
                'Entretenimiento':{'Social':4,
                                    'Actividad física':1,
                                    'Cultural':1}
                }
                    
    # time by ammenity

    column_max_ejes = [] # list with ejes index column names
    column_max_all = [] # list with all max index column names

    for e in idx_15_min.keys():
        
        column_max_ejes.append('max_'+ e.lower())
        column_max_all.append('max_'+ e.lower())
        column_max_amenities = [] # list with amenity index column names
        
        for a in idx_15_min[e].keys():
            
            column_max_amenities.append('max_'+ a.lower())
            column_max_all.append('max_'+ a.lower())

            if wegiht_idx[e][a] < len(idx_15_min[e][a]):
                nodes_analysis['max_'+ a.lower()] = nodes_analysis[idx_15_min[e][a]].min(axis=1)

            else:
                nodes_analysis['max_'+ a.lower()] = nodes_analysis[idx_15_min[e][a]].max(axis=1)
            
        nodes_analysis['max_'+ e.lower()] = nodes_analysis[column_max_amenities].max(axis=1)

    index_column = 'max_idx_15_min' # column name for 15 minute index data
    column_max_all.append(index_column)
    nodes_analysis[index_column] = nodes_analysis[column_max_ejes].max(axis=1)

    # nodes for grouping by hex
    column_max_all.append('osmid')
    column_max_all.append('geometry')
    nodes_analysis_filter = nodes_analysis[column_max_all].copy()

    aup.log(f'Calculated 15 minutes city index by node with an average of {nodes_analysis_filter[index_column].mean()} min')

    # group data by hex
    res = 8
    hex_tmp = hex_pop[['hex_id_8','geometry']]
    hex_res_8_idx = aup.group_by_hex_mean(nodes_analysis_filter, hex_tmp, res, index_column)
    hex_res_8_idx = hex_res_8_idx.loc[hex_res_8_idx[index_column]>0].copy()

    aup.log('Grouped nodes data by hexagons')

    # keep max time data for the 15 minute city index
    idx = hex_res_8_idx.index==hex_res_8_idx[index_column].idxmax()
    hex_res_8_idx = hex_res_8_idx[~idx].copy()

    # add population data
    hex_res_8_idx = pd.merge(hex_res_8_idx, hex_pop[pop_list], on='hex_id_8')

    aup.log(f'Population within 15 min city: {hex_res_8_idx.loc[hex_res_8_idx[index_column]<=15].pobtot.sum()}')
    aup.log(f'Population outside 15 min city: {hex_res_8_idx.loc[hex_res_8_idx[index_column]>15].pobtot.sum()}')

    # upload data
    if save:
        aup.gdf_to_db_slow(hex_res_8_idx, f'hex{res}_15_min', 'prox_analysis', if_exists='append')
    
    
    
if __name__ == "__main__":

    aup.log('--'*20)
    aup.log('Starting script')

    gdf_mun = aup.gdf_from_db('metro_list', 'metropolis')

    for city in gdf_mun.city.unique():

        aup.log(f'\n Starting city {city}')

        cvegeo_list = list(gdf_mun.loc[gdf_mun.city==city]["CVEGEO"].unique())

        main(city, cvegeo_list)

