import geopandas as gpd
import pandas as pd
import osmnx as ox
import numpy as np

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup

""" 
    For each city in Mexico's metropolis list, this script is an updated version of 
    what Script 01 + Script 02 + Script 15 do.

    Inputs: The script loads for each area of interest (city, aoi), each proximity analysis's points of interest (pois) 
    and a OSMnx network (G, nodes, edges). It also uses a hexgrid (with or without pop data).
    Process: The script calculates the source proximity by node (and saves to db if requested), 
    creates an output for the complete proximity (ejes-amenities) analysis, loads a hexgrid (with or without pop data)
    and re-calculates the source proximity by hexs (and saves to db if requested).
"""

def get_denue_pois(denue_schema, denue_table, poly_wkt, code, version):
    """Downloads DENUE points of interest and filters some data if requested.

    Arguments:
            denue_schema (str): database schema where DENUE table is located.
            denue_table (str): database table where DENUE data will be fetched from.
            poly_wkt (str): geometry of area of interest in Well-Known Text (WKT) format.
            code (int): code (unique poi ID). Based on DENUE's codigo_act col.
            version(int): as of this version (march 2024) accepts 1 or 2. 
                          If version == 2, this function applies a filter to certain pois 
                          (denue_dif and denue_centro_cultural).

    Returns:
            code_pois: GeoDataFrame with the code and geometry of the DENUE points found.
    """

    # Download DENUE pois from database
    query = f"SELECT * FROM {denue_schema}.{denue_table} WHERE (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')) AND (\"codigo_act\" = \'{code}\')"
    code_pois = aup.gdf_from_query(query, geometry_col='geometry')

    # Filter DENUE pois, if requested (version 2)
    if version == 1:
        aup.log(f"-- No filter applied to pois code {code}.")
    elif version == 2:
        if code == 931610: #denue_dif's codigo_act
            aup.log(f"-- Applying filtering to denue_dif pois (code {code}).")
            # DENUE's data regarding DIF are mixed with facilities that are not considered useful.
            # Therefore, denue_dif's pois are filtered by AVOIDING certain words in nom_estab (name of facility) column
            
            dif = code_pois.copy()
            words_toavoid = [#--- Cultural
                            'ARTE', #incl. ARTES, CONARTE
                            'MEDIATECA', 'MUSICA','ORQUESTA', #incl. MUSICAL, ORQUESTAS
                            #--- Institutions
                            'CONAFE','CONACYT',
                            'TRIBUNAL','PROTECCION CIVIL','IMM',
                            #--- Health
                            'IMMS','ISSTE','INAPAM','SEGURO','POPULAR','FOVI', #incl. FOVISSTE, FOVILEON, etc
                            'CAPASITIS',# Centro Ambulatorio para la Prevención y Atención del SIDA e Infecciones de Transmision Sexual
                            'SANITARI', #incl. SANITARIO/SANITARIA
                            'MEDIC', #incl. MEDICO/MEDICA
                            #--- Education
                            'INEA','PRIMARIA','SECUNDARIA','PREPARATORIA','MAESTROS','BECA','ASESORIA','APOYO',
                            'USAER', #Unidad de Servicio de Apoyo a la Educación Regular
                            'EDUCA', #incl. EDUCACION, EDUCACIÓN, EDUCATIVO, EDUCATIVA
                            #--- Housing
                            'VIVIENDA','INFONAVIT',
                            #--- Offices
                            'COORDINA','CORDINA', #incl. COORDINACION, y 'typos' (e.g. CORDINACION)
                            'DIRECCION','DIVISION','INSPECCION','INSTITUTO','JEFATURA','JURISDICCION','OFICINA','PROGRAMA','PROCURADORIA','PROCURADURIA',
                            'RECAUDACION','PAPELERIA','REGION ','REGULACION','SECRETARIA','DELEGACION','SUPERVI',
                            'ADMINISTRA',#incl. ADMINISTRATIVO, ADMINISTRATIVA
                            'ANALISIS', 'SEGUIMIENTO','MICRORED','MICRO RED',
                            #--- Warehouses
                            'ALMACEN','BODEGA','ARCHIVO','ACTIVO',
                            'PROVEED', #incl. PROVEEDOR, PROVEEDORA
                            #--- Other
                            'JUNTA', # (e.g. Juntas de mejoras)
                            'POLIVALENTE',
                            'SERVICIO',
                            'GIMNASIO']
            # Set checker (helps keep all pois, unless changed to 0)
            dif['keep'] = 1
            for word in words_toavoid:
                # Reset word_coincidence_count column
                dif['word_coincidence_count'] = 0
                # Look for word coincidence (0 = absent, 1 = present)
                dif['word_coincidence_count'] = dif['nom_estab'].apply(lambda x: x.count(word))
                # Keep only if the word is NOT present, else set 'keep' to 0
                dif.loc[dif.word_coincidence_count > 0,'keep'] = 0
            # Filter and return to rest of function (Final format at the end)
            dif_filtered = dif.loc[dif['keep'] == 1]
            dif_filtered.drop_duplicates(inplace=True)
            code_pois = dif_filtered.copy()
        
        elif code == 711312: #denue_centro_cultural
            aup.log(f"-- Applying filtering to denue_centro_cultural pois (code {code}).")
            # DENUE's data regarding cultural centers are mixed with facilities that are not considered useful.
            # denue_centro_cultural's pois are filtered by LOOKING FOR certain words in nom_estab column:

            centro_cultural = code_pois.copy()
            amenities_ofinterest = ['CENTRO',
                                    'CULTURA', #incl. CULTURAL
                                    'LIENZO',
                                    'PLAZA',
                                    'ARENA',
                                    'AUDITORIO',
                                    'TEATRO',
                                    'ARTE', # incl. ARTES
                                    'MUSEO']
            # Filter 
            centro_cultural_filtered = gpd.GeoDataFrame()
            for amenity in amenities_ofinterest:
                tmp = centro_cultural.loc[centro_cultural['nom_estab'].str.contains(amenity, regex=False)]
                centro_cultural_filtered = pd.concat([centro_cultural_filtered, tmp])
            # Return to rest of function (Final format at the end)
            centro_cultural_filtered.drop_duplicates(inplace=True)
            code_pois = centro_cultural_filtered.copy()
        else:
            aup.log("-- No filter applied.")
    else:
        aup.log("-- Error in specified proximity analysis version.")
        aup.log("-- Must pass integers 1 or 2.")
        intended_crash

    # Function final format for DENUE pois
    code_pois = code_pois[['codigo_act', 'geometry']]
    code_pois = code_pois.rename(columns={'codigo_act':'code'})
    code_pois['code'] = code_pois['code'].astype('int64')

    return code_pois

def two_method_check(row):
    """This function is used to decide which time to choose for cultural amenities.
       (As of march 2024, applies to version 2 only.) Explanation: 
            In version 2 we added 'Bibliotecas'. Original (DENUE) source contains plenty of pois, and not all of them are
            in good physical condition. Therefore, 'Bibliotecas' are important but might dilute other cultural sources. 
            It was decided that:
            > If 2 or more cultural source amenities are within 15 minutes of a given node, 
                choose max time of the sources within 15 minutes. 
                (Measures proximity to the second amenity, which we know is close.)
            > Else, if just 1 or 0 source amenities are within 15 minutes of a given node,
                choose min time of the amenities outside 15 minutes. 
                (Ignores if only one is close (most likely 'Bibliotecas'), takes next closest.)

    Arguments:
            row (pandas.Series): current row of DataFrame (function is used using .apply())

    Returns:
            row (pandas.Series): current row of DataFrame with chosen time.
    """

    # Case 1: Two or more cultural source amenities are within 15 minutes of a given node.
    #         choose max time of the sources within 15 minutes.
    #         (Measures proximity to an amenity which we know is close.)
    if row['check_count'] > 1: #Meaning, two or more
        # Identify sources within 15 minutes
        close_sources=[]
        for s in check_lst:
            # If <s> source is within 15 minutes, append to close_sources
            if row[s] == 1: 
                close_sources.append(s.replace('_check',''))
        # Find max of those close_sources
        row['max_'+a.lower()] = row[close_sources].max()

    # Case 2: just 1 or 0 source amenities are within 15 minutes of a given node.
    #         chooses min time of the amenities outside 15 minutes. 
    #         (Ignores if only one is close (most likely 'Bibliotecas'), takes next closest)
    else:
        # Identify sources outside 15 minutes
        far_sources=[]
        for s in check_lst:
            # If <s> source is NOT within 15 minutes, append to far_sources
            if row[s] == 0:
                far_sources.append(s.replace('_check',''))
        # Find min of those far sources
        row['max_'+a.lower()] = row[far_sources].min()
        
    return row

def main(city, res_list=[8,9], final_save=False, nodes_save=False, local_save=True):
    aup.log('--'*40)
    aup.log(f'--- STARTING CITY {city}.')

    ############################################################### PART 1 ###############################################################
    #################################################### FIND NODES PROXIMITY TO POIS ####################################################
    ###################################################### (PREV. SCRIPT 01 + 02) ########################################################

    # 1.1 --------------- BASE DATA FOR POIS-NODES ANALYSIS
    # ------------------- This step downloads the area of interest and network used to measure distance.
    
    # Download area of interest (aoi)
    aup.log('--- Downloading area of interest.')
    query = f"SELECT * FROM {metro_schema}.{metro_table} WHERE \"city\" LIKE \'{city}\'"
    mun_gdf = aup.gdf_from_query(query, geometry_col='geometry')
    mun_gdf = mun_gdf.set_crs("EPSG:4326")
    aoi = mun_gdf.dissolve()
    
    # Download Network (G, nodes, edges)
    aup.log('--- Downloading network.')
    G, nodes, edges = aup.graph_from_hippo(aoi, schema=network_schema, edges_folder=edges_table, nodes_folder=nodes_table)

    # 1.2 --------------- DOWNLOAD ALL CLUES AND SIP POINTS OF INTEREST
    # ------------------- This step downloads points of interest from SIP and CLUES.
    # ------------------- (DENUE pois are downloaded later code by code).
    sip_clues_gdf = gpd.GeoDataFrame()

    # CLUES (Health facilities)
    aup.log(f"--- Downloading CLUES pois for {city}.")
    # Download and filter CLUES
    clues_gdf = aup.gdf_from_polygon(aoi, clues_schema, clues_table, geom_col="geometry")
    clues_pois = clues_gdf.loc[clues_gdf['nivel_atencion'] == 'PRIMER NIVEL']
    del clues_gdf
    # Format CLUES
    clues_pois.loc[:,'code'] = 8610
    clues_pois = clues_pois[['code','geometry']]
    # Save CLUES to sip_clues_gdf
    sip_clues_gdf = pd.concat([sip_clues_gdf,clues_pois])
    del clues_pois

    # SIP (INEGI Marco geoestadístico's point data)
    aup.log(f"--- Downloading SIP pois for {city}.")
    # Download and filter SIP
    sip_gdf = aup.gdf_from_polygon(aoi, sip_schema, sip_table, geom_col="geometry")
    sip_amenities = {'GEOGRAFICO':['Mercado','Plaza'], 
                     'TIPO':['Cancha','Unidad Deportiva','Áreas Verdes','Jardín','Parque']}
    sip_amenities_codes = {'Mercado':4721, #assigned to sip_mercado
                           'Cancha':93110, #assigned to sip_cancha
                           'Unidad Deportiva':93111, #assigned to sip_unidad_deportiva 
                           'Áreas Verdes':9321, #assigned to sip_espacio_publico 
                           'Jardín':9321, #assigned to sip_espacio_publico
                           'Parque':9321, #assigned to sip_espacio_publico
                           'Plaza':9321 #assigned to sip_espacio_publico
                            }
    sip_pois = gpd.GeoDataFrame()
    for col in sip_amenities:
        for amenity in sip_amenities[col]:
            # Find in sip_gdf and assigns code from dict
            sip_tmp = sip_gdf.loc[sip_gdf[col] == amenity]
            # If there are pois of current code in city, append
            if len(sip_tmp) > 0:
                sip_tmp.loc[:,'code'] = sip_amenities_codes[amenity]
                sip_pois = pd.concat([sip_pois,sip_tmp])
    del sip_gdf
    # Format SIP
    sip_pois = sip_pois[['code','geometry']]
    # Save SIP to sip_clues_gdf
    sip_clues_gdf = pd.concat([sip_clues_gdf,sip_pois])
    del sip_pois

    # 1.3 --------------- ANALYSE POINTS OF INTEREST (downloads DENUE code by code)
    # ------------------- This step analysis times (and count of pois at given time proximity if requested) 
    # ------------------- using function analysis > pois_time.

    aup.log(f"""
------------------------------------------------------------
STARTING source pois proximity-to-nodes analysis for {city}.""")

    # PREP. FOR ANALYSIS
    poly_wkt = aoi.dissolve().geometry.to_wkt()[0]
    i = 0
    # PREP. FOR ANALYSIS - List of columns used to deliver final format of Script part 1
    all_analysis_cols = []

    # SOURCE LOOP - Calculates source proximity looping over sources from parameters dict.
    for eje in parameters.keys():
        for amenity in parameters[eje]:
            for source in parameters[eje][amenity]:
                source_analysis_cols = []

                aup.log(f"""
Analysing source {source}.""")
                
                # 1.3a) SAVE COL NAMES - Register current source's analysis col names
                # Source col to lists
                source_analysis_cols.append(source)
                all_analysis_cols.append(source)
                # If counting pois, create and append column 
                # count_col formated example: 'denue_preescolar_15min'
                if count_pois[0]:
                    count_col = f'{source}_{count_pois[1]}min'
                    source_analysis_cols.append(count_col)
                    all_analysis_cols.append(count_col)

                # 1.3b) GET POIS - Select source points of interest 
                # (concats all data corresponding to current source in source_pois)
                source_pois = gpd.GeoDataFrame()
                for code in parameters[eje][amenity][source]:
                    #If source is DENUE, download using function:
                    if source[0] == 'd':
                        aup.log(f'--- Downloading DENUE source pois code {code} from db.')
                        code_pois = get_denue_pois(denue_schema,denue_table,poly_wkt,code,version)
                    #If source is CLUES or SIP, fetch from previously generated sip_clues_gdf:
                    elif source[0] == 'c' or source[0] == 's':
                        aup.log(f'--- Getting clues/sip source pois code {code} from previously downloaded.')
                        code_pois = sip_clues_gdf.loc[sip_clues_gdf['code'] == code]
                    else:
                        aup.log(f'--- Error, check parameters dictionary.')
                        aup.log(f'--- As of this version, sources must start with source (denue_, clues_ or sip_).')
                        intended_crash
                    source_pois = pd.concat([source_pois,code_pois])
                aup.log(f"--- {source_pois.shape[0]} {source} pois. Analysing source pois proximity to nodes.")
                
                # 1.3c) SOURCE ANALYSIS
                # Calculate time data from nodes to source
                source_nodes_time = aup.pois_time(G, nodes, edges, source_pois, source, prox_measure, count_pois)
                # Format
                source_nodes_time.rename(columns={'time_'+source:source},inplace=True)
                source_nodes_time = source_nodes_time[['osmid']+source_analysis_cols+['x','y','geometry']]

                # 1.3d) OUTPUT MERGE
                # Merge all sources time data in final output nodes gdf
                if i == 0: # For the first analysed source
                    nodes_analysis = source_nodes_time.copy()
                else: # For the following
                    nodes_analysis = pd.merge(nodes_analysis,source_nodes_time[['osmid']+source_analysis_cols],on='osmid')
   
                i = i+1
                aup.log(f"--- FINISHED source {source}. Mean city time = {nodes_analysis[source].mean()}.")
            
    # 1.3d) Final format for nodes
    column_order = ['osmid'] + all_analysis_cols + ['x','y','geometry']
    nodes_analysis = nodes_analysis[column_order]

    if local_save:
        nodes_analysis.to_file(nodes_local_save_dir, driver='GPKG')
        aup.log(f"--- Saved {city} nodes gdf locally.")

    if nodes_save:
        nodes_analysis['city'] = city
        aup.gdf_to_db_slow(nodes_analysis, nodes_save_table, save_schema, if_exists='append')
        aup.log(f"--- Saved {city} nodes gdf in database.")
    
    aup.log(f"""
FINISHED source pois proximity-to-nodes analysis for {city}.
------------------------------------------------------------""")
    
    if stop: # Used to run script until this point (Functionality used in tests)
        aup.log('Stopped.')
        return city
    
    ############################################################### PART 2 ###############################################################
    ######################################################### AMENITIES ANALYSIS #########################################################
    ######################################################### (PREV. SCRIPT 15) ##########################################################
    
    # 2.0 --------------- DEFINITIONS DICTIONARY
    # ------------------- On script 15 a dictionary (idx_15_min) is used to calculate the times to amenities.
    # ------------------- This step creates the definitions dict out of the main parameters dict.
    
    definitions = {}
    for eje in parameters.keys():
        # tmp_dict stores all {amenity:[source_list]} for each eje
        tmp_dict = {}
        for amenity in parameters[eje]:
            items_lst = []
            items = list(parameters[eje][amenity].items())
            for item in items:
                items_lst.append(item[0])
            tmp_dict[amenity] = items_lst
        # Each eje gets assigned its own tmp_dict
        definitions[eje] = tmp_dict

    # 2.1 --------------- FILL FOR MISSING AMENITIES
    # ------------------- This step originates on script 15, where each city's nodes time data was loaded from db.
    # ------------------- Even though its no longer needed, it remains usefull for preventing crashes.

    all_sources = []
    # Gather all possible sources
    for eje in definitions.keys():
        for amenity in definitions[eje].values():
            for source in amenity:
                all_sources.append(source)
                
    # If source not in currently analized city, fill column with np.nan [Prevents crash]
    column_list = list(nodes_analysis.columns)
    missing_sourceamenities = []
    for s in all_sources:
            if s not in column_list:
                nodes_analysis[s] = np.nan
                aup.log(f"--- {s} source amenity is not present in {city}. Filled up with nans.")
                missing_sourceamenities.append(s)
                
    aup.log(f"--- Finished missing source amenities analysis. {len(missing_sourceamenities)} not present source amenities were added as np.nan columns.")
    
    # 2.2a -------------- AMENITIES ANALYSIS (amenities, ejes and max_time calculation)
    # ------------------- This step calculates times by amenity (preescolar/primaria/etc) using the previously created 
    # ------------------- definitions dictionary (Previously, on script 15, called idx_15_min dictionary)
    # ------------------- and using weights dictionary to decide which time to use (min/max/other)

    aup.log("--- Starting proximity to amenities analysis by node.")

    all_time_columns = [] # list with all time column names, previously called 'column_max_all'
    ejes_time_columns = [] # list with ejes time column names, previously called 'column_max_ejes'

    # Go through each eje in dictionary:
    for e in definitions.keys():

        # Append to lists currently examined eje
        all_time_columns.append('max_'+ e.lower())
        ejes_time_columns.append('max_'+ e.lower())
        amenity_time_columns = [] # list with amenity's time column names in current eje, previously called 'column_max_amenities'

        # Go through each amenity of current eje:
        for a in definitions[e].keys():

            #Append to lists currently examined amenity:
            all_time_columns.append('max_'+ a.lower())
            amenity_time_columns.append('max_'+ a.lower())

            # Calculate time to currently examined amenity:
            # (Uses source_weight dictionary to decide which time to use).
            weight = source_weight[e][a]
            if weight == 'min': # Used to know distance to closest source amenity.
                                # If it doesn't matter which one is closest (e.g. Alimentos).
                nodes_analysis['max_'+ a.lower()] = nodes_analysis[definitions[e][a]].min(axis=1)

            elif weight == 'max': # Used to know distance to farthest source amenity.
                                  # If need to know proximity to all of the options (e.g. Social)
                nodes_analysis['max_'+ a.lower()] = nodes_analysis[definitions[e][a]].max(axis=1)

            elif weight == 'two-method': #'two-method' (for cultural amenity's sources).
                                         # See two_method_check function definition for explanation.
                # Check which sources are within 15 minutes (data used in two_method_check)
                check_lst = []
                for s in definitions[e][a]:
                    nodes_analysis[s+'_check'] = nodes_analysis[s].apply(lambda x: 1 if x <= 15 else 0)
                    check_lst.append(s+'_check')
                # Check how many sources are within 15 minutes
                nodes_analysis['check_count'] = nodes_analysis[check_lst].sum(axis=1)
                # Apply two method check
                nodes_analysis = nodes_analysis.apply(two_method_check,axis='columns')
                # Drop columns used for checking
                check_lst.append('check_count')
                nodes_analysis.drop(columns=check_lst,inplace=True)
            else:
                # Crash on purpose and raise error
                aup.log("--- Error in source_weight dict.")
                aup.log("--- Must pass 'min', 'max' or 'two-method'.")
                intended_crash

        # Calculate time to currently examined eje (max time of its amenities):
        nodes_analysis['max_'+ e.lower()] = nodes_analysis[amenity_time_columns].max(axis=1) 

    # Set and calculate max time
    index_column = 'max_time' # column name for maximum time data
    all_time_columns.append(index_column) #Add to all_time_columns list the attribute 'max_time'
    nodes_analysis[index_column] = nodes_analysis[ejes_time_columns].max(axis=1) #Assign "max_time" the max time for all ejes   

    # Keep in nodes_analysis all_time_columns + node data (osmid + geometry)
    keep_time_columns = all_time_columns.copy()
    keep_time_columns.append('osmid')
    keep_time_columns.append('geometry')
    nodes_time_analysis_filter = nodes_analysis[keep_time_columns].copy()

    aup.log("--- Calculated proximity to amenities data by node.")

    # 2.2b -------------- AMENITIES COUNT ANALYSIS (amenities at given time count, optional)
    # ------------------- Similar to previous amenities analysis, this step (optional, added later)
    # ------------------- calculates how many amenities there are at a given time proximity (count_pois = (Boolean,time))

    if count_pois[0]:

        aup.log("--- Starting counting close amenities by node.")

        all_count_columns = []
        
        # Go through each eje
        for eje in definitions.keys():
            # Name of eje's count column
            eje_count_colname = f'{eje}_{count_pois[1]}min'.lower()
            # Append to lists
            all_count_columns.append(eje_count_colname)
        
            # Go through eje's amenities
            amenities_count_columns = []
            for amenity in definitions[eje]:
                # Name of count amenity
                amenity_count_colname = f'{amenity}_{count_pois[1]}min'.lower()
                # Append to lists
                all_count_columns.append(amenity_count_colname)
                amenities_count_columns.append(amenity_count_colname)
        
                # Gather amenities sources
                sources_count_columns = [] # Just used for sum function, not added at final output
                for source in definitions[eje][amenity]:
                    # Add to sources list
                    source_count_colname = f'{source}_{count_pois[1]}min'
                    sources_count_columns.append(source_count_colname)
                # Find sum of all sources found within given time of each node (For current amenity)
                nodes_analysis[amenity_count_colname] = nodes_analysis[sources_count_columns].sum(axis=1)
            # Find sum of all sources found within given time of each node (For current eje)
            nodes_analysis[eje_count_colname] = nodes_analysis[amenities_count_columns].sum(axis=1)
        
        # Keep in nodes_analysis all_count_columns + node data for merging (osmid)
        keep_count_columns = all_count_columns.copy()
        keep_count_columns.append('osmid') # Column used for merging
        nodes_count_analysis_filter = nodes_analysis[keep_count_columns]
        nodes_analysis_filter = pd.merge(nodes_time_analysis_filter, nodes_count_analysis_filter, on='osmid')

        aup.log("--- Counted close amenities by node.")

    else:
        aup.log("--- Not counting close amenities by node (count_pois=(False,)).")
        nodes_analysis_filter = nodes_time_analysis_filter.copy()
            
    ######################################################################################################################################
    # 2.3 --------------- GROUP DATA BY HEX [WORK IN PROGRESS, MUST TEST]
    # ------------------- This step groups nodes data by hexagon.
    # ------------------- If pop_output = True, also adds pop data. Else, creates hexgrid.

    # 2.3) 0. Resolution check. Prevent crashing from trying not available resolutions.
    checked_res_list = []
    for res in res_list:
        # Pop gdf in database is available in res 8 and 9
        if pop_output:
            allowed_res = [8,9]
            if res in allowed_res:
                checked_res_list.append(res)
                aup.log(f"--- Checking resolutions - approved {res}.")
            else:
                aup.log(f"--- Resolution {res} removed from res_list. This res is not available in pop output.")
        # Hexgrid 2020 gdf in database is available in res 8,9,10 and 11
        else:
            allowed_res = [8,9,10,11]
            if res in allowed_res:
                checked_res_list.append(res)
                aup.log(f"--- Checking resolutions - approved {res}.")
            else:
                aup.log(f"--- Resolution {res} removed from res_list. This res is not available in hexgrid 2020.")
    # Remove not allowed resolutions from hexgrid by copying checked_res_list.
    res_list = checked_res_list.copy()
    aup.log(f"--- Processing data to hex for resolutions {res_list}.")

    hex_idx = gpd.GeoDataFrame()
    # For each approved resolution
    for res in res_list:

        # (a) If not adding population data, just group proximity data by hex.
        if not pop_output:
            # 2.3a) (1) Load res hexagons for function group_by_hex_mean
            # Query and load for each particular res (table name has res)
            hex_table = f'hexgrid_{res}_city_2020'
            query = f"SELECT * FROM {hex_schema}.{hex_table} WHERE \"city\" LIKE \'{city}\'"
            hex_tmp = aup.gdf_from_query(query, geometry_col='geometry')
            hex_tmp = hex_tmp.set_crs("EPSG:4326")
            # Fields of interest for group_by_mean
            hex_tmp = hex_tmp[[f'hex_id_{res}','geometry']]
            aup.log(f"--- Loaded hexgrid of resolution {res}.")

            #2.3a) (2) Group data by hex
            hex_res_idx = aup.group_by_hex_mean(nodes_analysis_filter, hex_tmp, res, index_column)
            # Filter for hexagons with data
            hex_res_idx = hex_res_idx.loc[hex_res_idx[index_column]>0].copy()
            aup.log(f"--- Grouped nodes data by hexagons res {res}.")

            #2.3a) (3) Format col {hex_id_res} to cols {hex_id, res}
            hex_res_idx.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
            hex_res_idx['res'] = res

            #2.3a) (4) Add currently processed resolution to hex_idx
            hex_idx = pd.concat([hex_idx,hex_res_idx])
            aup.log(f"--- Saved proximity data by hexagons res {res}.")
        
        # (b) If adding population data, load and calculate pop data, group proximity data by hex and format.
        else:
            # 2.3b) (1) Load res hexagons with pop data
            hex_tmp_pop = aup.gdf_from_polygon(aoi, pop_schema, pop_table, geom_col="geometry")
            hex_tmp_pop = hex_tmp_pop.set_crs("EPSG:4326")
            aup.log(f"--- Loaded hexgrid with pop data of resolution {res}.")
            
            # 2.3b) (2) Calculate additional pop fields
            # Calculate age groups [Childhood (p_6a11) and Young adult (p_18a24) already exist]
            hex_tmp_pop['p_0a5'] = hex_tmp_pop['p_0a2'] + hex_tmp_pop['p_3a5'] #Early childhood
            hex_tmp_pop['p_12a17'] = hex_tmp_pop['p_12a14'] + hex_tmp_pop['p_15a17'] # Pub-adolescence
            hex_tmp_pop['p_25a59'] = hex_tmp_pop['p_18ymas'] - (hex_tmp_pop['p_18a24'] + hex_tmp_pop['p_60ymas']) #Adult
            # Calculate population density in hex
            hex_tmp_pop = hex_tmp_pop.to_crs("EPSG:6372")
            hex_tmp_pop['dens_pob_ha'] = hex_tmp_pop['pobtot'] / (hex_tmp_pop.area / 10000)
            hex_tmp_pop = hex_tmp_pop.to_crs("EPSG:4326")
            # Keep fields of interest
            pop_fields = ['pobtot','pobfem','pobmas',
                            'p_0a5','p_6a11','p_12a17','p_18a24','p_25a59','p_60ymas',
                            'pcon_disc','dens_pob_ha']
            hex_tmp_pop = hex_tmp_pop[[f'hex_id','res']+pop_fields+['geometry']]
            aup.log(f"--- Calculated pop data by hex for res {res}.")

            #2.3b) (3) Group data by hex
            # Difference with just loading hexgrid: pop gdf has columns {hex_id, res} separate,
            # but function group_data_by_hex requires col to be named {hex_id_res}.
            # Therefore, create hex_tmp that has {hex_id, res} separate.
            hex_tmp = hex_tmp_pop[['hex_id','geometry']].copy()
            hex_tmp.rename(columns={f'hex_id':'hex_id_{res}'},inplace=True)
            hex_res_idx = aup.group_by_hex_mean(nodes_analysis_filter, hex_tmp, res, index_column)
            # Filter for hexagons with data
            hex_res_idx = hex_res_idx.loc[hex_res_idx[index_column]>0].copy()
            aup.log(f"--- Grouped nodes data by hexagons res {res}.")

            #2.3b) (4) Format back from col {hex_id_res} to cols {hex_id, res}
            hex_res_idx.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
            hex_res_idx['res'] = res

            #2.3b) (5) Add downloaded and calculated pop data to hex_res_idx
            merge_list = pop_fields.copy()
            merge_list.append(f'hex_id')
            hex_res_idx_pop = pd.merge(hex_res_idx, hex_tmp_pop[merge_list], on=f'hex_id')

            #2.3b) (6) Add currently processed resolution to hex_idx
            hex_idx = pd.concat([hex_idx,hex_res_idx_pop])
            aup.log(f"--- Saved proximity and pop data by hexagons res {res}.")

    ############################################################### PART 3 ###############################################################
    #################################################### RECALCULATION AND FINAL DATA ####################################################
    #################################################### (PREV. SCRIPT 15 + NEW DATA) ####################################################

    # 3.1 --------------- RE-CALCULATE MAX TIMES BY HEXAGON
    # ------------------- This step recalculates max time to each eje  
    # ------------------- from max times to calculated amenities 

    #Goes (again) through each eje in dictionary:
    for e in definitions.keys():
        amenity_time_columns = [] # list with amenities in current eje

        #Goes (again) through each amenity of current eje:    
        for a in definitions[e].keys():
            amenity_time_columns.append('max_'+ a.lower())
        #Re-calculates time to currently examined eje (max time of its amenities):        
        hex_idx['max_'+ e.lower()] = hex_idx[amenity_time_columns].max(axis=1)

    aup.log('--- Finished recalculating ejes times in hexagons.')   
    
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

    # Extract all amenities (previosly we had amenities list by eje, not all) from all_time_columns
    max_amenities_cols = [i for i in all_time_columns if i not in ejes_time_columns]
    max_amenities_cols.remove('max_time')
    # Create list with idx column names
    idx_amenities_cols = []
    for ac in max_amenities_cols:
        idx_col = ac.replace('max','idx')
        hex_idx[idx_col] = hex_idx[ac].apply(apply_sigmoidal)
        idx_amenities_cols.append(idx_col)
    # Add final data
    hex_idx[index_column] = hex_idx[ejes_time_columns].max(axis=1)
    hex_idx['mean_time'] = hex_idx[max_amenities_cols].mean(axis=1)
    hex_idx['median_time'] = hex_idx[max_amenities_cols].median(axis=1)
    hex_idx['idx_sum'] = hex_idx[idx_amenities_cols].sum(axis=1)
    hex_idx['city'] = city

    aup.log('--- Finished calculating index, mean, median and max time.')
    
    # 3.3 --------------- FINAL FORMAT
    # ------------------- This step gives final format to the gdf

    # First elements of ordered column list - ID and geometry
    final_column_ordered_list = ['hex_id','res','geometry']

    # Second elements of ordered column list - max_ejes and max_amenities 
    # removing max_time, osmid and geometry.
    ejes_time_columns_amenities = all_time_columns.copy()
    ejes_time_columns_amenities.remove('max_time')
    final_column_ordered_list = final_column_ordered_list + ejes_time_columns_amenities

    # Third elements of ordered column list - count pois columns (if requested)
    # removing osmid and geometry.
    if count_pois[0]:
        third_elements = all_count_columns.copy()
        final_column_ordered_list = final_column_ordered_list + third_elements

    # Fourth elements of ordered list are listed in idx_amenities_cols
    final_column_ordered_list = final_column_ordered_list + idx_amenities_cols

    # Fifth elements of ordered list - Final mean, median, max and idx
    fifth_elements = ['mean_time', 'median_time', 'max_time', 'idx_sum']
    final_column_ordered_list = final_column_ordered_list + fifth_elements

    # Sixth elements - If pop is calculated - Pop data
    if pop_output:
        final_column_ordered_list = final_column_ordered_list + pop_fields

    # Last element - City data
    final_column_ordered_list.append('city')

    # Filter/reorder final output    
    hex_idx_city = hex_idx[final_column_ordered_list]
        
    aup.log('--- Finished final format for gdf.')   

    # 3.4 --------------- SAVING
    # ------------------- This step saves (locally for tests, to db for script running)

    if local_save:
        hex_idx_city.to_file(final_local_save_dir, driver='GPKG')
        aup.log(f"--- Saved {city} gdf locally.")

    if final_save:
        aup.gdf_to_db_slow(hex_idx_city, final_save_table, save_schema, if_exists='append')
        aup.log(f"--- Saved {city} gdf in database.")


if __name__ == "__main__":
 
    # ---------------------------- SCRIPT CONFIGURATION - VERSION ----------------------------
    # Prox analysis version (Must pass integers 1 or 2)
    # If version = 1, does proximity analysis as it was done in 2020.
    # If version = 2:
        # > Filters denue_dif for reviewed points of interest
        # > Introduces new method to choose times (used in cultural amenity) 
        # > Includes and filters pois to cultural amenity: 
        #   denue_bibliotecas --> "Bibliotecas y archivos del sector privado." + "Bibliotecas y archivos del sector privado."
        #   denue_centrocultural --> "Promotores del sector público de espectáculos artísticos, culturales, deportivos y similares que cuentan con instalaciones para presentarlos."
    version = 1

    if version == 1: #Prox analysis 2020 version
        cultural_dict = {'denue_cines':[512130],
                         'denue_museos':[712111, 712112]}
        cultural_weight =  'min' # Will choose min time to source because measuring access to nearest source, doesn't matter which.

    elif version == 2: #Prox analysis 2024 version
        cultural_dict = {'denue_cines':[512130],
                        'denue_museos':[712111, 712112],
                        'denue_bibliotecas':[519121,519122],
                        'denue_centrocultural':[711312]}
        cultural_weight =  'two-method'
    else:
        aup.log("--- Error in specified proximity analysis version.")
        aup.log("--- Must pass integers 1 or 2.")
        intended_crash

    # ---------------------------- SCRIPT CONFIGURATION - DATABASE SCHEMAS AND TABLES ----------------------------
    # Area of interest (city)
    metro_schema = 'projects_research' #metropolis_analysis: 'metropolis'
    metro_table = 'femsainfancias_missingcities_metrogdf2020' #metropolis_analysis: 'metro_gdf_2020'

    # Network data (nodes and edges table for distance analysis,
    # also used to generate the network G with which the nearest OSMID is assigned to each poi)
    network_schema = 'projects_research' #metropolis_analysis: 'osmnx'
    nodes_table = 'femsainfancias_missingcities_nodes' #metropolis_analysis: 'nodes' or 'nodes_osmnx_23_point'
    edges_table = 'femsainfancias_missingcities_edgesspeed' #metropolis_analysis: 'edges_speed' or 'edges_speed_23_line'

    # Points of interest - DENUE
    denue_schema = 'denue'
    denue_table = 'denue_2020' #metropolis_analysis: 'denue_2020' or 'denue_23_point'

    # Points of interest - CLUES
    clues_schema = 'denue'
    clues_table = 'clues' #metropolis_analysis: 'clues' or 'clues_23_point'

    # Points of interest - SIP
    sip_schema = 'denue'
    sip_table = 'sip_2020' #metropolis_analysis: 'sip_2020' or 'sip_23_point'

    # Hexgrid
    hex_schema = 'hexgrid'
    # VERIFY ON SCRIPT hex_table.
    # metropolis analysis's data depends on res ['hexgrid_{res}_city_2020' (deprecated: 'hexgrid_{res}_city')], 
    # Verify table name (created inside Main function for each res output).

    ######### POP DATA IS WORK IN PROGRESS
    # Population data 
    pop_schema = 'projects_research' #metropolis_analysis: censo
    pop_table = 'femsainfancias_missingcities_censoageb_hex' #metropolis_analysis: 'pobcenso_inegi_20_mzaageb_hex' or 'censo_inegi_20_ageb_hex' (deprecated:'hex_bins_pop_2020', had res8 only)
    ######### POP DATA IS WORK IN PROGRESS

    # ---------------------------- SCRIPT CONFIGURATION - ANALYSIS AND OUTPUT OPTIONS ----------------------------
    # Network distance method used in function pois_time. (If length, assumes pedestrian speed of 4km/hr.)
    prox_measure = 'time_min' # Must pass 'length' or 'time_min'

    # Count available amenities at given time proximity (minutes)?
    count_pois = (False,15) # Must pass a tupple containing a boolean (True or False) and time proximity of interest in minutes (Boolean,time)

    # If pop_output = True, loads pop data from pop_schema and pop_table.
    # If pop_output = False, loads empty hexgrid.
    pop_output = True

    # Hexagon resolutions of output
    res_list = [8,9]

    # Do not process city-list
    # If intentionally skipping cities, add here. Else, leave empty list.
    skip_city_list = []

    # Stop at any given point of script's main function?
    stop = False

    # ---------------------------- SCRIPT CONFIGURATION - SAVING ----------------------------
    save_schema = 'projects_research' #metropolis_analysis: 'prox_analysis'
    # Save nodes with proximity data to db?
    nodes_save = False
    nodes_save_table = 'femsainfancias_missingcities_proxnodes' #metropolis_analysis: 'nodesproximity_24'
    # Save final output to db?
    final_save = True 
    final_save_table = 'femsainfancias_missingcities_proxhexs' #metropolis_analysis: 'proximityanalysis_24_ageb_hex'
    # If local_save is activated, script runs Aguascalientes only.
    local_save = False
    nodes_local_save_dir = f"../data/processed/proximity_v2/test_ags_proxanalysis_scriptv{version}_nodes.gpkg"
    final_local_save_dir = f"../data/processed/proximity_v2/test_ags_proxanalysis_scriptv{version}_hex.gpkg"

    # ---------------------------- SCRIPT CONFIGURATION - POIS STRUCTURE ----------------------------
    # PARAMETERS DICTIONARY (Required)
    # This dictionary sets the ejes, amenidades, sources and codes for analysis
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
                                    'Cultural':cultural_dict
                                    } 
                }

    # WEIGHT DICTIONARY (Required)
    # If need to measure nearest source for amenity, doesn't matter which, choose 'min'
    # If need to measure access to all of the different sources in an amenity, choose 'max'
    source_weight = {'Escuelas':{'Preescolar':'max', #There is only one source, no effect.
                                'Primaria':'max',  #There is only one source, no effect.
                                'Secundaria':'max'},  #There is only one source, no effect.
                    'Servicios comunitarios':{'Salud':'max',  #There is only one source, no effect.
                                            'Guarderías':'max', #There is only one source, no effect.
                                            'Asistencia social':'max'},  #There is only one source, no effect.
                    'Comercio':{'Alimentos':'min', # /////////////////////////////////// Will choose min time to source because measuring access to nearest food source, doesn't matter which.
                                'Personal':'max', #There is only one source, no effect.
                                'Farmacias':'max', #There is only one source, no effect.
                                'Hogar':'min', # /////////////////////////////////////// Will choose min time to source because measuring access to nearest source, doesn't matter which.
                                'Complementarios':'min'}, # //////////////////////////// Will choose min time to source because measuring access to nearest source, doesn't matter which.
                    'Entretenimiento':{'Social':'max', # /////////////////////////////// Will choose MAX time to source because measuring access to all of them (restaurantes, bares AND cafes)
                                        'Actividad física':'min', # //////////////////// Will choose min time to source because measuring access to nearest source, doesn't matter which.
                                        'Cultural':cultural_weight} # ////////////////// Depends on version (v1 will choose min, v2 two-method.)
                    }

    # ---------------------------- SCRIPT START ----------------------------
    aup.log('--'*50)
    aup.log(f"--- STARTING SCRIPT 21 USING VERSION {version}.")
    
    # Script mode:
    if local_save: # Local save activates test mode (Aguascalientes only)
        city_list = ['Aguascalientes']
        processed_city_list = []
        i = 0
        k = len(city_list)
    
    else: # Else, script's goal is to run all cities
        cities_gdf = aup.gdf_from_db(metro_table, metro_schema)
        cities_gdf = cities_gdf.sort_values(by='city')
        city_list = list(cities_gdf.city.unique())
        del cities_gdf

        # Prevent cities being analyzed several times in case of a crash
        aup.log('--- Looking for alredy saved data in db.')
        processed_city_list = []
        try:
            query = f"SELECT city FROM {save_schema}.{final_save_table}"
            processed_city_list = aup.df_from_query(query)
            processed_city_list = list(processed_city_list.city.unique())
        except:
            pass
        
        # Add to processed_city_list all cities in skip_city_list
        for skip_city in skip_city_list:
            processed_city_list.append(skip_city)

        # Current progress
        i = len(processed_city_list)
        # Sum of all cities to be processed
        k = len(city_list)

    # Run
    for city in city_list:
        if city not in processed_city_list:
            aup.log("--"*40)
            i = i + 1
            aup.log(f"--- Running Script city {i}/{k}: {city}")
            main(city, res_list, final_save, nodes_save, local_save)
