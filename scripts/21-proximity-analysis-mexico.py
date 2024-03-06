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

def get_denue_pois(denue_schema,denue_table,poly_wkt,code,version):
    # This function downloads the codigo_act denue poi requested for the analysis.
    # If it is version 2.0, applies a filter to certain pois.

    # Download denue pois
    query = f"SELECT * FROM {denue_schema}.{denue_table} WHERE (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')) AND (\"codigo_act\" = \'{code}\')"
    code_pois = aup.gdf_from_query(query, geometry_col='geometry')

    # Version 2.0 pois filter
    if version == 1:
        aup.log("--- No filter applied.")
    elif version == 2:
        if code == 931610: #denue_dif
            aup.log(f"--- Applying filtering to code {code}.")
            dif = code_pois.copy()
            # Sets word of amenity to avoid in nom_estab
            words_toavoid = [# Culturales
                            'ARTE', #incluye ARTES, CONARTE
                            'MEDIATECA', 'MUSICA','ORQUESTA', #incluye MUSICAL, ORQUESTAS
                            # Instituciones
                            'CONAFE','CONACYT', #incluye CULTURAL
                            'TRIBUNAL','PROTECCION CIVIL','IMM',
                            # Salud
                            'IMMS','ISSTE','INAPAM','SEGURO','POPULAR','FOVI', #incluye FOVISSTE, FOVILEON, etc #
                            'CAPASITIS',#Centro ambulatorio para la prevención y atención del SIDA e infecciones de transmision sexual
                            'SANITARI', #SANITARIO/SANITARIA
                            'MEDIC', #MEDICO/MEDICA
                            # Educación
                            'INEA','PRIMARIA','SECUNDARIA','PREPARATORIA','MAESTROS','BECA','ASESORIA','APOYO',
                            'USAER', #Unidad de Servicio de Apoyo a la Educación Regular
                            'EDUCA', #EDUCACION, EDUCACIÓN, EDUCATIVO, EDUCATIVA
                            # Vivienda
                            'VIVIENDA','INFONAVIT',
                            # Oficinas
                            'COORDINA','CORDINA', #incluye COORDINACION, y typos (CORDINACION)
                            'DIRECCION','DIVISION','INSPECCION','INSTITUTO','JEFATURA','JURISDICCION','OFICINA','PROGRAMA','PROCURADORIA','PROCURADURIA',
                            'RECAUDACION','PAPELERIA','REGION ','REGULACION','SECRETARIA','DELEGACION','SUPERVI',
                            'ADMINISTRA',#ADMINISTRATIVO, ADMINISTRATIVA
                            'ANALISIS', 'SEGUIMIENTO','MICRORED','MICRO RED',
                            # Almacenes y bodegas
                            'ALMACEN','BODEGA','ARCHIVO','ACTIVO',
                            'PROVEED', #PROVEEDOR, PROVEEDORA
                            # Otros
                            'JUNTA', # para juntas de mejoras
                            'POLIVALENTE',
                            'SERVICIO',
                            'GIMNASIO']
            # Set checker
            dif['keep'] = 1
            for word in words_toavoid:
                # Reset word_coincidence_count column
                dif['word_coincidence_count'] = 0
                # Look for word coincidence (0 = absent, 1 = present)
                dif['word_coincidence_count'] = dif['nom_estab'].apply(lambda x: x.count(word))
                # If the word is present, do not keep
                dif.loc[dif.word_coincidence_count > 0,'keep'] = 0
            # Filter and return to rest of function (Formats later)
            dif_filtered = dif.loc[dif['keep'] == 1]
            dif_filtered.drop_duplicates(inplace=True)
            code_pois = dif_filtered.copy()
        
        elif code == 711312: #denue_centro_cultural
            aup.log(f"--- Applying filtering to code {code}.")
            centro_cultural = code_pois.copy()
            amenities_ofinterest = ['CENTRO',
                                    'CULTURA', #incluye CULTURAL
                                    'LIENZO',
                                    'PLAZA',
                                    'ARENA',
                                    'AUDITORIO',
                                    'TEATRO',
                                    'ARTE', # incluye ARTES
                                    'MUSEO']
            # Filter 
            centro_cultural_filtered = gpd.GeoDataFrame()
            for amenity in amenities_ofinterest:
                tmp = centro_cultural.loc[centro_cultural['nom_estab'].str.contains(amenity, regex=False)]
                centro_cultural_filtered = pd.concat([centro_cultural_filtered, tmp])
            # Return to rest of function
            centro_cultural_filtered.drop_duplicates(inplace=True)
            code_pois = centro_cultural_filtered.copy()
        else:
            aup.log("--- No filter applied.")
    else:
        aup.log("--- Error in specified proximity analysis version.")
        aup.log("--- Must pass integers 1 or 2.")
        intended_crash

    # Format denue pois
    code_pois = code_pois[['codigo_act', 'geometry']]
    code_pois = code_pois.rename(columns={'codigo_act':'code'})
    code_pois['code'] = code_pois['code'].astype('int64')

    return code_pois

def two_method_check(row):
    # This function is used to decide which time to choose for cultural amenities.
    # Why:
        # In version 2 we aded 'Bibliotecas'. The source contains plenty of pois.
        # This might dilute other cultural sources. Therefore:

    # If 2 or more source amenities are within 15 minutes, 
    # chooses max time of the sources within 15 minutes.
    # (Measures proximity to an amenity which we know is close.)
    if row['check_count'] > 1:
        # Identify sources within 15 minutes
        prox_sources=[]
        for s in check_lst:
            if row[s] == 1:
                prox_sources.append(s.replace('_check',''))
        # Find max of those sources
        row['max_'+a.lower()] = row[prox_sources].max()

    # Else (just 1 or 0 source amenities are within 15 minutes),
    # chooses min time of the amenities outside 15 minutes. 
    # (Ignores if only one is close (most likely bibliotecas), takes next closest)
    else:
        # Identify sources outside 15 minutes
        prox_sources=[]
        for s in check_lst:
            if row[s] == 0:
                prox_sources.append(s.replace('_check',''))
        # Find min of those sources
        row['max_'+a.lower()] = row[prox_sources].min()
        
    return row

def main(city, final_save=False, nodes_save=False, local_save=True):
    aup.log('--'*40)
    aup.log(f'--- STARTING CITY {city}.')

    ############################################################### PART 1 ###############################################################
    #################################################### FIND NODES PROXIMITY TO POIS ####################################################
    ###################################################### (PREV. SCRIPT 01 + 02) ########################################################

    # 1.1 --------------- BASE DATA FOR POIS-NODES ANALYSIS
    # ------------------- This first step downloads the area of interest and network used to measure distance.
    
    # Download area of interest
    aup.log('--- Downloading area of interest.')
    query = f"SELECT * FROM {metro_schema}.{metro_table} WHERE \"city\" LIKE \'{city}\'"
    mun_gdf = aup.gdf_from_query(query, geometry_col='geometry')
    mun_gdf = mun_gdf.set_crs("EPSG:4326")
    aoi = mun_gdf.dissolve()
    
    # Download Network used to calculate nearest note to each poi
    aup.log('--- Downloading network.')
    G, nodes, edges = aup.graph_from_hippo(aoi, schema=network_schema, edges_folder=edges_table, nodes_folder=nodes_table)

    # 1.2 --------------- DOWNLOAD POINTS OF INTEREST (clues and sip pois, not denue)
    # ------------------- This step downloads SIP and CLUES points of interest (denue pois are downloaded later.)
    sip_clues_gdf = gpd.GeoDataFrame()

    # CLUES (Salud)
    aup.log(f"--- Downloading CLUES pois for {city}.")
    # Download
    clues_gdf = aup.gdf_from_polygon(aoi, clues_schema, clues_table, geom_col="geometry")
    # Filter
    clues_pois = clues_gdf.loc[clues_gdf['nivel_atencion'] == 'PRIMER NIVEL']
    del clues_gdf
    # Format
    clues_pois.loc[:,'code'] = 8610
    clues_pois = clues_pois[['code','geometry']]
    # Save to pois_tmp
    sip_clues_gdf = pd.concat([sip_clues_gdf,clues_pois])
    del clues_pois

    # SIP (Marco geoestadistico)
    aup.log(f"--- Downloading SIP pois for {city}.")
    # Download
    sip_gdf = aup.gdf_from_polygon(aoi, sip_schema, sip_table, geom_col="geometry")
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

    # 1.3 --------------- ANALYSE POINTS OF INTEREST (If denue, downloads)
    # ------------------- This step analysis times (and count of pois at given time proximity if requested) using function aup.pois_time.

    aup.log(f"""
------------------------------------------------------------
STARTING source pois proximity to nodes analysis for {city}.""")

    # PREP. FOR ANALYSIS
    poly_wkt = aoi.dissolve().geometry.to_wkt()[0]
    i = 0
    # PREP. FOR ANALYSIS - List of columns used to deliver final format of Script part 1
    all_analysis_cols = []

    # SOURCE ANALYSIS
    for eje in parameters.keys():
        for amenity in parameters[eje]:
            for source in parameters[eje][amenity]:
                source_analysis_cols = []

                aup.log(f"""
Analysing source {source}.""")
                
                # ANALYSIS COLS - Add source col to lists
                source_analysis_cols.append(source)
                all_analysis_cols.append(source)

                # ANALYSIS COLS  - If counting pois, create and append column (count_col formated example: 'denue_preescolar_15min')
                if count_pois[0]:
                    count_col = f'{source}_{count_pois[1]}min'
                    source_analysis_cols.append(count_col)
                    all_analysis_cols.append(count_col)

                # GET POIS - Select source points of interest (concats all data of current source's codes in source_pois)
                source_pois = gpd.GeoDataFrame()
                for code in parameters[eje][amenity][source]:
                    #If source is denue:
                    if source[0] == 'd':
                        aup.log(f'--- Downloading denue source pois code {code} from db.')
                        code_pois = get_denue_pois(denue_schema,denue_table,poly_wkt,code,version)
                    #If source is clues or sip:
                    elif source[0] == 'c' or source[0] == 's':
                        aup.log(f'--- Getting clues/sip source pois code {code} from previously downloaded.')
                        code_pois = sip_clues_gdf.loc[sip_clues_gdf['code'] == code]
                    else:
                        aup.log(f'--- Error, check parameters dicctionary.')
                        aup.log(f'--- Sources must start with denue_, clues_ or sip_.')
                        intended_crash
                        
                    source_pois = pd.concat([source_pois,code_pois])

                aup.log(f"--- {source_pois.shape[0]} {source} pois. Analysing source pois proximity to nodes.")
                
                # ANALYSIS - Calculate time data from nodes to source
                source_nodes_time = aup.pois_time(G, nodes, edges, source_pois, source, prox_measure,count_pois)
                # ANALYSIS - Format
                source_nodes_time.rename(columns={'time_'+source:source},inplace=True)
                source_nodes_time = source_nodes_time[['osmid']+source_analysis_cols+['x','y','geometry']]

                # SOURCE MERGE - Merge all sources time data in final output nodes gdf
                if i == 0: # For the first analysed source
                    nodes_analysis = source_nodes_time.copy()
                else: # For the following
                    nodes_analysis = pd.merge(nodes_analysis,source_nodes_time[['osmid']+source_analysis_cols],on='osmid')
   
                i = i+1

                aup.log(f"--- FINISHED source {source}. Mean city time = {nodes_analysis[source].mean()}")
            
    # Final format for nodes
    column_order = ['osmid'] + all_analysis_cols + ['x','y','geometry']
    nodes_analysis = nodes_analysis[column_order]

    if local_save:
        nodes_analysis.to_file(nodes_local_save_dir, driver='GPKG')
        aup.log(f"--- Saved {city} nodes gdf locally.")

    if nodes_save:
        nodes_analysis['city'] = city
        aup.gdf_to_db_slow(nodes_timeanalysis_filter, nodes_save_table, save_schema, if_exists='append')
        aup.log(f"--- Saved {city} nodes gdf in database.")

    if stop:
        aup.log('Stopped.')
        return city
    
    aup.log(f"""
------------------------------------------------------------
FINISHED source pois proximity to nodes analysis for {city}.""")
    
    ############################################################### PART 2 ###############################################################
    ######################################################### AMENITIES ANALYSIS #########################################################
    ######################################################### (PREV. SCRIPT 15) ##########################################################
    
    # 2.0 --------------- DEFINITIONS DICTIONARY
    # ------------------- On script 15 a dictionary (idx_15_min) is used to calculate the times to amenities.
    # ------------------- This step creates the definitions dicc out of the main parameters dicc.
    
    definitions = {}
    for eje in parameters.keys():
        # tmp_dicc is {amenity:[source_list]} for each eje
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
    # ------------------- Definitions dicc (Previously, on script 15, called idx_15_min dictionary) is also used in the next steps.

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
                aup.log(f"--- {s} source amenity is not present in {city}.")
                missing_sourceamenities.append(s)
                
    aup.log(f"--- Finished missing source amenities analysis. {len(missing_sourceamenities)} not present source amenities were added as np.nan columns.")
    
    # 2.2a -------------- AMENITIES ANALYSIS (amenities, ejes and max_time calculation)
    # ------------------- This step calculates times by amenity (preescolar/primaria/etc) using the previously created 
    # ------------------- definitions dictionary (Previously, on script 15, called idx_15_min dictionary)
    # ------------------- and using weights dictionary to decide which time to use (min/max/other)

    aup.log("--- Starting proximity to amenities analysis by node.")

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
            weight = source_weight[e][a]
            if weight == 'min': # To know distance to closest source amenity.
                                # If it doesn't matter which one is closest (e.g. Alimentos).
                nodes_analysis['max_'+ a.lower()] = nodes_analysis[definitions[e][a]].min(axis=1)

            elif weight == 'max': # To know distance to farthest source amenity.
                                  # If need to know proximity to all of the options (e.g. Social)
                nodes_analysis['max_'+ a.lower()] = nodes_analysis[definitions[e][a]].max(axis=1)

            elif weight == 'two-method': #'two-method' (for cultural amenity's sources).
                                         # See two_method_check function definition for explanation.
                # Check which sources are within 15 minutes
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
                aup.log("--- Error in source_weight dicc.")
                aup.log("--- Must pass 'min', 'max' or 'two-method'")
                intended_crash

        #Calculates time to currently examined eje (max time of its amenities):
        nodes_analysis['max_'+ e.lower()] = nodes_analysis[column_max_amenities].max(axis=1) 

    # Set and calculate max time
    index_column = 'max_time' # column name for maximum time data
    column_max_all.append(index_column) #Adds to column_max_all list the attribute 'max_time'
    nodes_analysis[index_column] = nodes_analysis[column_max_ejes].max(axis=1) #Assigns "max_time" the max time for all ejes   

    # Add to column_max_all list the attributes 'osmid' and 'geometry' to filter nodes_analysis.
    # Looking for data of importance: columns in column_max_all list
    column_max_all.append('osmid')
    column_max_all.append('geometry')
    nodes_timeanalysis_filter = nodes_analysis[column_max_all].copy()

    aup.log("--- Calculated proximity to amenities data by node.")

    # 2.2b -------------- AMENITIES COUNT ANALYSIS (amenities at given time count, optional)
    # ------------------- Similar to previous amenities analysis, this step (optional, added later)
    # ------------------- calculates how many amenities there are at a given time proximity (count_pois = (Boolean,time))

    if count_pois[0]:
        column_count_all = []
        
        # Go through each eje
        for eje in definitions.keys():
            # Name of count eje column
            eje_count_colname = f'{eje}_{count_pois[1]}min'.lower()
            # Append to lists
            column_count_all.append(eje_count_colname)
        
            # Go through eje's amenities
            column_count_amenities = []
            for amenity in definitions[eje]:
                # Name of count amenity
                amenity_count_colname = f'{amenity}_{count_pois[1]}min'.lower()
                # Append to lists
                column_count_all.append(amenity_count_colname)
                column_count_amenities.append(amenity_count_colname)
        
                # Gather amenities sources
                column_count_sources = [] # Just used for sum function, not added at final output
                for source in definitions[eje][amenity]:
                    # Add to sources list
                    source_count_colname = f'{source}_{count_pois[1]}min'
                    column_count_sources.append(source_count_colname)
                # Find sum of all sources found within given time of each node (For current amenity)
                nodes_analysis[amenity_count_colname] = nodes_analysis[column_count_sources].sum(axis=1)
            # Find sum of all sources found within given time of each node (For current eje)
            nodes_analysis[eje_count_colname] = nodes_analysis[column_count_amenities].sum(axis=1)
        
        # Filter for columns of interest
        column_count_all.append('osmid') # Column used for merging
        nodes_countanalysis_filter = nodes_analysis[column_count_all]
        nodes_analysis_filter = pd.merge(nodes_timeanalysis_filter,nodes_countanalysis_filter,on='osmid')

    else:
        nodes_analysis_filter = nodes_timeanalysis_filter.copy()
            
    # 2.3 --------------- POPULATION DATA
    # ------------------- This step (optional) loads hexagons with population data.
    ######################################################################################################################################
    # ------------------- This steps final code must be reviewed according to new pop data names in the db.
    # ------------------- Currently, only hex_bins_pop_2020 is 8
    if pop_output:
        res_list = [8]
        aup.log(f"--- Set res_list to 8 only. pop_output currently only generates res 8 data.")
    ######################################################################################################################################

    if pop_output:
        hex_socio_gdf = gpd.GeoDataFrame()
        # Downloads hex_socio_gdf for city area
        for res in res_list:
            # Download
            hex_pop_res = aup.gdf_from_polygon(aoi, pop_schema, pop_table, geom_col="geometry")
            hex_pop_res = hex_pop_res.set_crs("EPSG:4326")
            aup.log(f"--- Downloaded pop gdf res {res}.")

            # Format
            hex_pop_res.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
            hex_pop_res['res'] = res
            
            # Calculate fields of interest
            hex_pop_res_tmp = hex_pop_res.to_crs("EPSG:6372")
            hex_pop_res_tmp['dens_pob_ha'] = hex_pop_res_tmp['pobtot'] / (hex_pop_res_tmp.area / 10000)

            # Merge calculated fields to hex_pop_res gdf
            hex_pop_res_tmp = hex_pop_res_tmp[['hex_id','dens_pob_ha']]
            hex_pop_res = pd.merge(hex_pop_res,hex_pop_res_tmp,on='hex_id')

            # Save fields of interest for current res
            pop_fields = ['pobtot','dens_pob_ha']
            hex_socio_gdf = pd.concat([hex_socio_gdf,hex_pop_res[['hex_id','res']+pop_fields+['geometry']]])
            aup.log(f"--- Saved pop gdf res {res}.")
    
    # 2.4 --------------- GROUP DATA BY HEX
    # ------------------- This groups nodes data by hexagon.
    # ------------------- If pop output, uses previously created hexes. Else, creates hexgrid.
    
    # Prevent crashing from trying not allowed resolutions.
    checked_res_list = []
    if version == 1:
        allowed_res = [8,9]
        for res in res_list:
            if res in allowed_res:
                checked_res_list.append(res)
            else:
                print(f"--- Resolution {res} removed from res_list. This res is not allowed in version {version}.")
    elif version == 2:
        allowed_res = [8,9,10,11]
        for res in res_list:
            if res in allowed_res:
                checked_res_list.append(res)
            else:
                print(f"--- Resolution {res} removed from res_list. This res is not allowed in version {version}.")
    else:
            aup.log("--- Error in specified proximity analysis version.")
            aup.log("--- Must pass integers 1 or 2.")
            intended_crash
    res_list = checked_res_list.copy()
     
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
            aup.log(f"--- Loaded pop hexgrid of resolution {res}.")

        # If pop_output is false, creates hexgrid
        else:
            if version == 1:
                hex_table = f'hexgrid_{res}_city'
                query = f"SELECT * FROM {hex_schema}.{hex_table} WHERE \"metropolis\" LIKE \'{city}\'"
            elif version == 2:
                query = f"SELECT * FROM {hex_schema}.{hex_table} WHERE \"city\" LIKE \'{city}\'"
                hex_table = f'hexgrid_{res}_city_2020'
            else:
                aup.log("--- Error in specified proximity analysis version.")
                aup.log("--- Must pass integers 1 or 2.")
                intended_crash

            # Load hexgrid (which already has ID_res)
            hexgrid = aup.gdf_from_query(query, geometry_col='geometry')
            # Create hex_tmp
            hex_tmp = hexgrid.set_crs("EPSG:4326")
            hex_tmp = hex_tmp[[f'hex_id_{res}','geometry']].copy()
            aup.log(f"--- Loaded hexgrid of resolution {res}.")
        
        # Group data by hex
        hex_res_idx = aup.group_by_hex_mean(nodes_analysis_filter, hex_tmp, res, index_column)
        hex_res_idx = hex_res_idx.loc[hex_res_idx[index_column]>0].copy()
        aup.log(f"--- Grouped nodes data by hexagons res {res}.")
        
        # If pop_output is true, add pop data
        if pop_output:
            pop_list = pop_fields.copy()
            pop_list.append(f'hex_id_{res}')
            hex_res_pop = pd.merge(hex_res_idx, hex_pop[pop_list], on=f'hex_id_{res}')
        else:
            hex_res_pop = hex_res_idx.copy()
        
        # After funtion group_by_hex_mean we can remove res from ID and set as a column
        hex_res_pop.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
        hex_res_pop['res'] = res

        # Finally, add to hex_idx each resolution processing
        hex_idx = pd.concat([hex_idx,hex_res_pop])
        aup.log(f"--- Saved grouped data by hexagons res {res}.")

    ############################################################### PART 3 ###############################################################
    #################################################### RECALCULATION AND FINAL DATA ####################################################
    #################################################### (PREV. SCRIPT 15 + NEW DATA) ####################################################

    # 3.1 --------------- RE-CALCULATE MAX TIMES BY HEXAGON
    # ------------------- This step recalculates max time to each eje  
    # ------------------- from max times to calculated amenities 

    #Goes (again) through each eje in dictionary:
    for e in definitions.keys():
        column_max_amenities = [] # list with amenities in current eje

        #Goes (again) through each amenity of current eje:    
        for a in definitions[e].keys():
            column_max_amenities.append('max_'+ a.lower())
        #Re-calculates time to currently examined eje (max time of its amenities):        
        hex_idx['max_'+ e.lower()] = hex_idx[column_max_amenities].max(axis=1)

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

    aup.log('--- Finished calculating index, mean, median and max time.')
    
    # 3.3 --------------- FINAL FORMAT
    # ------------------- This step gives final format to the gdf

    # First elements of ordered column list - ID and geometry
    final_column_ordered_list = ['hex_id','res','geometry']

    # Second elements of ordered column list - max_ejes and max_amenities 
    # removing max_time, osmid and geometry.
    column_max_ejes_amenities = column_max_all.copy()
    column_max_ejes_amenities.remove('max_time')
    column_max_ejes_amenities.remove('osmid')
    column_max_ejes_amenities.remove('geometry')
    final_column_ordered_list = final_column_ordered_list + column_max_ejes_amenities

    # Third elements of ordered column list - count pois columns (if requested)
    # removing osmid and geometry.
    if count_pois[0]:
        third_elements = column_count_all.copy()
        third_elements.remove("osmid")
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
        cultural_dicc = {'denue_cines':[512130],
                         'denue_museos':[712111, 712112]}
        cultural_weight =  'min' # Will choose min time to source because measuring access to nearest source, doesn't matter which.

    elif version == 2: #Prox analysis 2024 version
        cultural_dicc = {'denue_cines':[512130],
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
    metro_schema = 'metropolis'
    metro_table = 'metro_gdf_2020'
    # Network data (nodes and edges table for distance analysis,
    # also used to generate the network G with which the nearest OSMID is assigned to each poi)
    network_schema = 'osmnx'
    nodes_table = 'nodes' # 'nodes' or 'nodes_osmnx_23_point'
    edges_table = 'edges_speed' # 'edges_speed' or 'edges_speed_23_line'
    # Points of interest - DENUE
    denue_schema = 'denue'
    denue_table = 'denue_2020' # 'denue_2020' or 'denue_23_point'
    # Points of interest - CLUES
    clues_schema = 'denue'
    clues_table = 'clues' # 'clues' or 'clues_23_point'
    # Points of interest - SIP
    sip_schema = 'denue'
    sip_table = 'sip_2020' # 'sip_2020' or 'sip_23_point'
    # Hexgrid
    hex_schema = 'hexgrid'
    # Population data
    pop_schema = 'censo'
    pop_table = 'hex_bins_pop_2020' ################################# POP DATA IS WORK IN PROGRESS

    # ---------------------------- SCRIPT CONFIGURATION - ANALYSIS AND OUTPUT OPTIONS ----------------------------
    # Network distance method used in function pois_time. (If length, assumes pedestrian speed of 4km/hr.)
    prox_measure = 'time_min' # Must pass 'length' or 'time_min'

    # Count available amenities at given time proximity (minutes)?
    count_pois = (False,15) # Must pass a tupple containing a boolean (True or False) and time proximity of interest in minutes (Boolean,time)

    # If pop_output = True, loads pop data from pop_schema and pop_table.
    # If pop_output = False, loads empty hexgrid.
    pop_output = True

    # Hexagon resolutions of output
    res_list = [8]

    # Stop at any given point of main function?
    stop = True

    # ---------------------------- SCRIPT CONFIGURATION - SAVING ----------------------------
    save_schema = 'prox_analysis'
    # Save nodes with proximity data to db?
    nodes_save = False
    nodes_save_table = 'nodesproximity_24'
    # Save final output to db?
    final_save = False 
    final_save_table = 'proximityanalysis_24_ageb_hex'

    # ---------------------------- SCRIPT CONFIGURATION - LOCAL SAVE (TESTS) ----------------------------
    # If local_save is activated, script runs Aguascalientes only.
    local_save = True
    nodes_local_save_dir = '../data/external/temporal_fromjupyter/proximity_v2/test_proxanalysis_scriptv2_nodes.gpkg'
    final_local_save_dir = '../data/external/temporal_fromjupyter/proximity_v2/test_proxanalysis_scriptv2.gpkg'

    # ---------------------------- SCRIPT START ----------------------------
    aup.log('--'*50)
    aup.log(f"--- STARTING SCRIPT USING VERSION {version}.")

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
                                    'Cultural':cultural_dicc
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
                    'Comercio':{'Alimentos':'min', # /////////////////////////////////// Will choose min time to source because measuring access to nearest food source, doesn't matter which.
                                'Personal':'max', #There is only one source, no effect.
                                'Farmacias':'max', #There is only one source, no effect.
                                'Hogar':'min', # /////////////////////////////////////// Will choose min time to source because measuring access to nearest source, doesn't matter which.
                                'Complementarios':'min'}, # //////////////////////////// Will choose min time to source because measuring access to nearest source, doesn't matter which.
                    'Entretenimiento':{'Social':'max', # /////////////////////////////// Will choose MAX time to source because measuring access to all of them (restaurantes, bares AND cafes)
                                        'Actividad física':'min', # //////////////////// Will choose min time to source because measuring access to nearest source, doesn't matter which.
                                        'Cultural':cultural_weight} # ////////////////// Depends on version (v1 will choose min, v2 two-method.)
                    }
    
    # Load city list
    if local_save: # Local save activates test mode (Aguascalientes only)
        city_list = ['Aguascalientes']
    else: #script run
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
    
    # If intentionally skipping cities, add here:
    #processed_city_list.append('city')
    processed_city_list.append('ZMVM')
    processed_city_list.append('CDMX')

    # Run
    for city in city_list:
        if city not in processed_city_list:
            main(city, final_save, nodes_save, local_save)
