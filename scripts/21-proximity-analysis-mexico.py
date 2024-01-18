import geopandas as gpd
import pandas as pd
import numpy as np

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup

def main(city, save = False, local_save = True):

    ############################################################### PART 1 ###############################################################
    #################################################### FIND NODES PROXIMITY TO POIS ####################################################
    ###################################################### (PREV. SCRIPT 01 + 02) ########################################################

    # 1.1 --------------- BASE DATA FOR POIS-NODES ANALYSIS
    # ------------------- This first step downloads the area of interest and network used to measure distance.
    
    # Download area of interest
    query = f"SELECT * FROM {metro_schema}.{metro_table} WHERE \"city\" LIKE \'{city}\'"
    mun_gdf = aup.gdf_from_query(query, geometry_col='geometry')
    mun_gdf = mun_gdf.set_crs("EPSG:4326")
    aoi = mun_gdf.dissolve()

    # Download Network used to calculate nearest note to each poi
    G, nodes, edges = aup.graph_from_hippo(aoi,'osmnx',edges_folder = edges_folder)


    # 1.2 --------------- DOWNLOAD POINTS OF INTEREST (clues and sip pois, not denue)
    # ------------------- This step downloads SIP and CLUES points of interest (denue pois are downloaded later.)
    sip_clues_gdf = gpd.GeoDataFrame()

    # CLUES (Salud)
    # Download
    clues_gdf = gdf_from_polygon(aoi, clues_schema, clues_table, geom_col="geometry")
    # Filter
    clues_pois = clues_gdf.loc[clues_gdf['nivel_atencion'] == 'PRIMER NIVEL']
    del clues_gdf
    # Format
    clues_pois.loc[:,'code'] = 8610
    clues_pois = clues_pois[['code','geometry']]
    # Save to pois_tmp
    sip_clues_gdf = pd.concat([sip_clues_gdf,clues_pois])
    del clues_pois

    print(f"Downloaded CLUES pois for {city}.")

    # SIP (Marco geoestadistico)
    # Download
    sip_gdf = gdf_from_polygon(aoi, sip_schema, sip_table, geom_col="geometry")
    sip_amenities = {'GEOGRAFICO':['Mercado','Plaza'], 
                     'TIPO':['Cancha','Unidad Deportiva','Áreas Verdes','Jardín','Parque']}
    # Filter - SIP pois of interest
    sip_amenities_codes = {'Mercado':4721, #sip_mercado
                           'Cancha':93110, #sip_cancha
                           'Unidad Deportiva':93111, #sip_unidad_deportiva 
                           'Áreas Verdes':9321, #sip_espacio_publico 
                           'Jardín':9321, #sip_espacio_publico
                           'Parque':9321, #sip_espacio_publico
                           'Plaza':9321 #sip_espacio_publico
                            }
    # Filter - Iterate over sip_amenities and filter sip gdf
    sip_pois = gpd.GeoDataFrame()
    for col in sip_amenities:
        for amenity in sip_amenities[col]:
            sip_tmp = sip_gdf.loc[sip_gdf[col] == amenity]
            sip_tmp.loc[:,'code'] = sip_amenities_codes[amenity]
            sip_pois = pd.concat([sip_pois,sip_tmp])
    del sip_gdf
    # Format
    sip_pois = sip_pois[['code','geometry']]
    # Save to pois_tmp
    sip_clues_gdf = pd.concat([sip_clues_gdf,sip_pois])
    del sip_pois

    print(f"Downloaded sip pois for {city}.")

    # --------------- ANALYSE POINTS OF INTEREST (If denue, downloads)
    poly_wkt = aoi.dissolve().geometry.to_wkt()[0]

    i = 0
    source_list = []

    for eje in parameters.keys():
        for amenity in parameters[eje]:
            for source in parameters[eje][amenity]:
                print('--'*20)
                print(f'STARTING {source} for {city}.')
                source_list.append(source)
                
                # ANALYSIS - Select source points of interest
                source_pois = gpd.GeoDataFrame()
                for code in parameters[eje][amenity][source]:
                    #If source is denue:
                    if source[0] == 'd': 
                        # Download denue pois
                        query = f"SELECT * FROM {denue_schema}.{denue_table} WHERE (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')) AND (\"codigo_act\" = \'{code}\')"
                        code_pois = aup.gdf_from_query(query, geometry_col='geometry')
                        # Format denue pois
                        code_pois = code_pois[['codigo_act', 'geometry']]
                        code_pois = code_pois.rename(columns={'codigo_act':'code'})
                        code_pois['code'] = code_pois['code'].astype('int64')
                    #If source is clues or sip:
                    elif source[0] == 'c' or source[0] == 's': 
                        code_pois = sip_clues_gdf.loc[sip_clues_gdf['code'] == code]
                        print(f'Download sip/clues code {code} from pois_tmp.')
                    else:
                        print(f'Error, check parameters dicctionary.')
                        intended_crash
                        
                    source_pois = pd.concat([source_pois,code_pois])

                print(f"Downloaded a total of {source_pois.shape[0]} pois for source amenity {source}.")
                
                # ANALYSIS - Calculate times from nodes to source
                source_nodes_time = aup.pois_time(G, nodes, edges, source_pois, source, prox_measure)
                source_nodes_time.rename(columns={'time':source},inplace=True)
                source_nodes_time = source_nodes_time[['osmid',source,'x','y','geometry']]

                # ANALYSIS - Merge all times in one df
                if i == 0: # For the first analysed source
                    nodes_analysis = source_nodes_time.copy()
                else: # For the rest
                    nodes_analysis = pd.merge(nodes_analysis,source_nodes_time[['osmid',source]],on='osmid')

                i = i+1
            
    # Final format for nodes
    column_order = ['osmid'] + source_list + ['x','y','geometry']
    nodes_analysis = nodes_analysis[column_order]
    
    print(f'FINISHED nodes proximity analysis for {city}.')

    ############################################################### PART 2 ###############################################################
    ######################################################### AMENITIES ANALYSIS #########################################################
    ######################################################### (PREV. SCRIPT 15) ##########################################################
    
    # 2.0 --------------- DEFINITIONS DICTIONARY
    # ------------------- On script 15 a dictionary (idx_15_min) is used to calculate the times to amenities.
    # ------------------- This step creates the definitions dicc out of the main parameters dicc.
    
    definitions = {}
    for eje in parameters.keys():
        # Temporary dicc stores amenity:[source_list] for each eje
        tmp_dicc = {}
        for amenity in parameters[eje]:
            items_lst = []
            items = list(parameters[eje][amenity].items())
            for item in items:
                items_lst.append(item[0])
            tmp_dicc[amenity] = items_lst
        # Each eje gets assigned its own tmp_dicc
        definitions[eje] = tmp_dicc

    # 2.1 --------------- FILL FOR MISSING AMENITIES
    # ------------------- This step originates on script 15, where each cities nodes time data was loaded from db.
    # ------------------- Even though its no longer needed, it remains usefull for avoiding crashes.
    # ------------------- Definitions dicc (Previously, on script 15, called idx_15_min dictionary) is used in the next steps.

    all_sources = []
    # Gather all possible sources
    for eje in definitions.keys():
        for amenity in definitions[eje].values():
            for source in amenity:
                all_sources.append(source)
                
    # If source not in currently analized city, fill column with np.nan
    column_list = list(nodes_analysis.columns)
    missing_sourceamenities = []
    for s in all_sources:
            if s not in column_list:
                nodes_analysis[s] = np.nan
                print(f"{s} source amenity is not present in {city}.")
                missing_sourceamenities.append(s)
                
    print(f"Finished missing source amenities analysis. {len(missing_sourceamenities)} not present source amenities were added as np.nan columns")
    
    # 2.2 --------------- AMENITIES ANALYSIS (max_time calculation)
    # ------------------- This step calculates times by amenity (preescolar/primaria/etc) using the previously created 
    # ------------------- definitions dictionary (Previously, on script 15, called idx_15_min dictionary)
    # ------------------- and using weights dictionary to decide which time to use (min/max/other)

    print("Starting proximity to amenities analysis by node.")

    column_max_all = [] # list with all max times column names
    column_max_ejes = [] # list with ejes max times column names

    #Goes through each eje in dictionary:
    for e in definitions.keys():

        #Appends to lists currently examined eje
        column_max_all.append('max_'+ e.lower())
        column_max_ejes.append('max_'+ e.lower())
        column_max_amenities = [] # list with amenities in current eje

        #Goes through each amenity of current eje:
        for a in definitions[e].keys():

            #Appends to lists currently examined amenity:
            column_max_all.append('max_'+ a.lower())
            column_max_amenities.append('max_'+ a.lower())

            #Calculates time to currently examined amenity:
            #Uses source_weight dictionary to decide which time to use.
            if source_weight[e][a] == 'min':
                # To know how far the closest source amenity is.
                nodes_analysis['max_'+ a.lower()] = nodes_analysis[definitions[e][a]].min(axis=1)
            else:
                # To know how far the farthest source amenity is
                nodes_analysis['max_'+ a.lower()] = nodes_analysis[definitions[e][a]].max(axis=1)

        #Calculates time to currently examined eje (max time of its amenities):
        nodes_analysis['max_'+ e.lower()] = nodes_analysis[column_max_amenities].max(axis=1) 

    # Set and calculate all columns analysis (index_column)
    index_column = 'max_time' # column name for maximum time data
    column_max_all.append(index_column) #Adds to column_max_all list the attribute 'max_time'
    nodes_analysis[index_column] = nodes_analysis[column_max_ejes].max(axis=1) #Assigns "max_time" the max time for all ejes   

    # Add to column_max_all list the attributes 'osmid' and 'geometry' to filter nodes_analysis.
    # Looking for data of importance: columns in column_max_all list
    column_max_all.append('osmid')
    column_max_all.append('geometry')
    nodes_analysis_filter = nodes_analysis[column_max_all].copy()
        
    print("Calculated proximity to amenities data by node.")

    # 2.3 --------------- POPULATION DATA
    # ------------------- This step loads hexagons with population data (optional).
    ######################################################################################################################################
    # ------------------- This steps final code must be reviewed according to new pop data names in the db.
    ######################################################################################################################################
    res_list = [8]

    if pop_output:
        # Downloads hex_socio_gdf for city area
        for res in res_list:
            # Download
            hex_socio_gdf = gdf_from_polygon(aoi, pop_schema, pop_table, geom_col="geometry")
            hex_socio_gdf = hex_socio_gdf.set_crs("EPSG:4326")

            # Calculate fields of interest
            hex_socio_gdf_tmp = hex_socio_gdf.to_crs("EPSG:6372")
            hex_socio_gdf_tmp['dens_pob_ha'] = hex_socio_gdf_tmp['pobtot'] / (hex_socio_gdf_tmp.area / 10000)

            # Merge calculated fields
            hex_socio_gdf_tmp = hex_socio_gdf_tmp[['hex_id','dens_pob_ha']]
            hex_socio_gdf = pd.merge(hex_socio_gdf,hex_socio_gdf_tmp,on='hex_id')

            # Format
            hex_socio_gdf.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
            hex_socio_gdf['res'] = res
    
    # 2.4 --------------- GROUP DATA BY HEX
    # ------------------- This groups nodes data by hexagon.
    # ------------------- If pop output, uses previously created hexes. Else, creates hexgrid.
            
    hex_idx = gpd.GeoDataFrame()
    for res in res_list:
        # Load or create hexgrid
        # If pop_output is true, loads previously created hexgrid with pop data
        if pop_output:
            # Load hexgrid
            hex_pop = hex_socio_gdf.loc[hex_socio_gdf['res'] == res]
            # Function group_by_hex_mean requires ID to include resolution
            hex_pop.rename(columns={'hex_id':f'hex_id_{res}'},inplace=True)
            # Create hex_tmp (id and geometry)
            hex_pop = hex_pop.to_crs("EPSG:4326")
            hex_tmp = hex_pop[[f'hex_id_{res}','geometry']].copy()
            print(f"Loaded pop hexgrid of resolution {res}")
        # If pop_output is false, creates hexgrid
        else:
            # Create hexgrid (which already has ID_res)
            hexgrid = aup.create_hexgrid(aoi,res)
            # Create hex_tmp
            hexgrid = hexgrid.set_crs("EPSG:4326")
            hex_tmp = hexgrid.copy()
            print(f"Created hexgrid of resolution {res}")
                
        # Group data by hex
        hex_res_idx = aup.group_by_hex_mean(nodes_analysis_filter, hex_tmp, res, index_column)
        hex_res_idx = hex_res_idx.loc[hex_res_idx[index_column]>0].copy()
        print(f"Grouped nodes data by hexagons res {res}")
        
        # If pop_output is true, add pop data
        if pop_output:
            pop_list = [f'hex_id_{res}','pobtot','dens_pob_ha']
            hex_res_pop = pd.merge(hex_res_idx, hex_pop[pop_list], on=f'hex_id_{res}')
        else:
            hex_res_pop = hex_res_idx.copy()
        
        # After funtion group_by_hex_mean we can remove res from ID and set as a column
        hex_res_pop.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
        hex_res_pop['res'] = res

        # Finally, add to hex_idx each resolution processing
        hex_idx = pd.concat([hex_idx,hex_res_pop])
        print(f"Saved grouped data by hexagons res {res}")

    ############################################################### PART 3 ###############################################################
    #################################################### RECALCULATION AND FINAL DATA ####################################################
    #################################################### (PREV. SCRIPT 15 + NEW DATA) ####################################################

    # 3.1 --------------- RE-CALCULATE MAX TIMES BY HEXAGON
    # ------------------- This step recalculates max time to each eje  
    # ------------------- from max times to calculated amenities 
    
    column_max_ejes = [] # list with ejes index column names

    #Goes (again) through each eje in dictionary:
    for e in definitions.keys():
        column_max_amenities = [] # list with amenities in current eje

        #Goes (again) through each amenity of current eje:    
        for a in definitions[e].keys():
            column_max_amenities.append('max_'+ a.lower())
        #Re-calculates time to currently examined eje (max time of its amenities):        
        hex_idx['max_'+ e.lower()] = hex_idx[column_max_amenities].max(axis=1)

    print('Finished recalculating ejes times in hexagons')   
    
    # 3.2 --------------- CALCULATE AND ADD ADDITIONAL AND FINAL DATA
    # ------------------- This step adds mean, median, city and idx data to each hex

    #Define idx function
    def apply_sigmoidal(x):
        if x == -1:
            return -1
        elif x > 1000:
            return 0
        else:
            val = aup.sigmoidal_function(0.1464814753435666, x, 30)
            return val

    # Create all amenities list (previosly we had amenities list by eje) from column_max_ejes
    max_amenities_cols = [i for i in column_max_all if i not in column_max_ejes]
    max_amenities_cols.remove('max_time')
    max_amenities_cols.remove('osmid')
    max_amenities_cols.remove('geometry')
    # Create list with idx column names
    idx_amenities_cols = []
    for ac in max_amenities_cols:
        idx_col = ac.replace('max','idx')
        hex_idx[idx_col] = hex_idx[ac].apply(apply_sigmoidal)
        idx_amenities_cols.append(idx_col)
    # Add final data
    hex_idx[index_column] = hex_idx[column_max_ejes].max(axis=1)
    hex_idx['mean_time'] = hex_idx[max_amenities_cols].mean(axis=1)
    hex_idx['median_time'] = hex_idx[max_amenities_cols].median(axis=1)
    hex_idx['idx_sum'] = hex_idx[idx_amenities_cols].sum(axis=1)
    hex_idx['city'] = city

    print('Finished calculating index, mean, median and max time.')
    
    # 3.3 --------------- FINAL FORMAT
    # ------------------- This step gives final format to the gdf

    # First elements of ordered column list - ID and geometry
    first_elements = ['hex_id','res','geometry']
    # Second elements of ordered column list - max_ejes and max_amenities 
    # removing max_time, osmid and geometry.
    column_max_ejes_amenities = column_max_all.copy()
    column_max_ejes_amenities.remove('max_time')
    column_max_ejes_amenities.remove('osmid')
    column_max_ejes_amenities.remove('geometry')
    # Third elements of ordered list are listed in idx_amenities_cols
    # Fourth elements of ordered list - Final mean, median, max and idx
    fourth_elements = ['mean_time', 'median_time', 'max_time', 'idx_sum']
    # Fifth elements - If pop is calculated - Pop data
    fifth_elements = ['pobtot', 'dens_pob_ha']
    # Last element - City data
    last_element = ['city']

    if pop_output:
        final_column_ordered_list = first_elements + column_max_ejes_amenities + idx_amenities_cols + fourth_elements + fifth_elements + last_element
    else:
        final_column_ordered_list = first_elements + column_max_ejes_amenities + idx_amenities_cols + fourth_elements + last_element
        
    hex_idx_city = hex_idx[final_column_ordered_list]
        
    print('Finished final format')    

    # 3.4 --------------- SAVING
    # ------------------- This step saves (locally for tests, to db for script running)

    if local_save:
        hex_idx_city.to_file(local_save_dir, driver='GPKG')

    if save:
        aup.gdf_to_db_slow(hex_idx_city, save_table, save_schema, if_exists='append')


if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')

    # BASE DATA REQUIRED
    # Area of interest (city)
    metro_schema = 'metropolis'
    metro_table = 'metro_gdf_2015'
    # Edges folder for distance analysis (also used to generate the network G with which the nearest OSMID is assigned to each poi)
    edges_folder = 'edges_speed'
    # Points of interest - DENUE
    denue_schema = 'denue'
    denue_table = 'denue_2020'
    # Points of interest - CLUES
    clues_schema = 'denue'
    clues_table = 'clues'
    # Points of interest - SIP
    sip_schema = 'denue'
    sip_table = 'sip_2020'
    # Population data
    pop_schema = 'censo'
    pop_table = 'hex_bins_pop_2020'

    # ANALYSIS AND OUTPUT OPTIONS
    # Network distance data used to calculate distance from each node to nearest poi in function pois_time.
    prox_measure = 'time_min' # Must pass 'length' or 'time_min'
    # If pop_output = True, loads pop data from pop_schema and pop_table.
    pop_output = True
    # Hexagon resolutions of output
    res_list = [8,9]

    # SAVING
    # Save final output to db?
    save = False
    save_schema = 'prox_analysis'
    save_table = 'proximityanalysis'
    # Local save? (For tests)
    local_save = True
    local_save_dir = '../data/external/temporal_fromjupyter/proximity_v2/ags_proxanalysis_scriptv2.gpkg'
    if local_save:
        city = 'Aguascalientes'

    # Save disk space by deleting used data that will not be used after?
    # save_space = True

    # PARAMETERS DICTIONARY
    # This dicctionary sets the ejes, amenidades, sources and codes for analysis
            #{Eje (e):
            #            {Amenity (a):
            #                          {Sources (s):
            #                                           [Codes (c)]
            #                           }
            #             }
            #}
    parameters = {'Escuelas':{'Preescolar':{'denue_preescolar':[611111, 611112]},
                            'Primaria':{'denue_primaria':[611121, 611122]},
                            'Secundaria':{'denue_secundaria':[611131, 611132]}
                            },
                'Servicios comunitarios':{'Salud':{'clues_primer_nivel':[8610]},
                                        'Guarderías':{'denue_guarderias':[624411, 624412]},
                                        'Asistencia social':{'denue_dif':[931610]}
                                        },
                'Comercio':{'Alimentos':{'denue_supermercado':[462111],
                                        'denue_abarrotes':[461110], 
                                        'denue_carnicerias': [461121, 461122, 461123],
                                        'sip_mercado':[4721]},
                            'Personal':{'denue_peluqueria':[812110]},
                            'Farmacias':{'denue_farmacias':[464111, 464112]},
                            'Hogar':{'denue_ferreteria_tlapaleria':[467111],
                                    'denue_art_limpieza':[467115]},
                            'Complementarios':{'denue_ropa':[463211, 463212, 463213, 463215, 463216, 463218],
                                                'denue_calzado':[463310], 
                                                'denue_muebles':[466111, 466112, 466113, 466114],
                                                'denue_lavanderia':[812210],
                                                'denue_revistas_periodicos':[465313],
                                                'denue_pintura':[467113]}
                            },
                'Entretenimiento':{'Social':{'denue_restaurante_insitu':[722511, 722512, 722513, 722514, 722519],
                                            'denue_restaurante_llevar':[722516, 722518, 722517],
                                            'denue_bares':[722412],
                                            'denue_cafe':[722515]},
                                    'Actividad física':{'sip_cancha':[93110],
                                                        'sip_unidad_deportiva':[93111],
                                                        'sip_espacio_publico':[9321],
                                                        'denue_parque_natural':[712190]},
                                    'Cultural':{'denue_cines':[512130],
                                                'denue_museos':[712111, 712112]}
                                    } 
                }

    # WEIGHT DICTIONARY
    # If need to measure nearest source for amenity, doesn't matter which, choose 'min'
    # If need to measure access to all of the different sources in an amenity, choose 'max'
    source_weight = {'Escuelas':{'Preescolar':'max', #There is only one source, no effect.
                                'Primaria':'max',  #There is only one source, no effect.
                                'Secundaria':'max'},  #There is only one source, no effect.
                    'Servicios comunitarios':{'Salud':'max',  #There is only one source, no effect.
                                            'Guarderías':'max', #There is only one source, no effect.
                                            'Asistencia social':'max'},  #There is only one source, no effect.
                    'Comercio':{'Alimentos':'min', # /////////////////////////////////////////////////////// Will choose min time to source because measuring access to nearest food source, doesn't matter which.
                                'Personal':'max', #There is only one source, no effect.
                                'Farmacias':'max', #There is only one source, no effect.
                                'Hogar':'min', # ////////////////////////////////////////////////////////// Will choose min time to source because measuring access to nearest source, doesn't matter which.
                                'Complementarios':'min'}, # /////////////////////////////////////////////// Will choose min time to source because measuring access to nearest source, doesn't matter which.
                    'Entretenimiento':{'Social':'max', # ////////////////////////////////////////////////// Will choose max time to source because measuring access to all of them.
                                        'Actividad física':'min', # //////////////////////////////////////// Will choose min time to source because measuring access to nearest source, doesn't matter which.
                                        'Cultural':'min'} # //////////////////////////////////////////////// Will choose min time to source because measuring access to nearest source, doesn't matter which.
                    }
    
    # MAIN FUNCTION RUN
    cities_gdf = aup.gdf_from_db('metro_gdf', 'metropolis')
    cities_gdf = cities_gdf.sort_values(by='city')
    city_list = list(cities_gdf.city.unique())

    del cities_gdf
    
    # prevent cities being analyzed several times in case of a crash
    aup.log('Downloading preprocessed data')
    processed_city_list = []
    try:
        query = f"SELECT city FROM {metro_schema}.{metro_table}"
        processed_city_list = aup.df_from_query(query)
        processed_city_list = list(processed_city_list.city.unique())
    except:
        pass

    for city in city_list:
        if city not in processed_city_list:
            main(city = city, save = save, local_save = local_save)
