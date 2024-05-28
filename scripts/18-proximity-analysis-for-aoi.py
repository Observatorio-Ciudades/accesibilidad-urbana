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


def apply_sigmoidal(x):
    if x == -1:
        return -1
    elif x > 1000:
        return 0
    else:
        val = aup.sigmoidal_function(0.1464814753435666, x, 30)
        return val


def main(pop_output, db_save=False, local_save=True, save_space=False):

    ##########################################################################################
    # STEP 1: CREATE OSMNX NETWORK
    # ------------------- This step downloads the osmnx network for the area of interest.

    # Read area of interest (aoi)
    aoi = gpd.read_file(aoi_dir)
    aoi = aoi.to_crs("EPSG:4326")
    aup.log(f"--- Starting creation of osmnx network.")

    if database_network:
        G, nodes, edges = aup.graph_from_hippo(aoi, network_schema, edges_table, nodes_table, projected_crs)
    else:
        # Download osmnx network (G, nodes and edges from bounding box of aoi)
        G, nodes, edges = aup.create_osmnx_network(aoi,how='from_bbox')
        aup.log(f"--- Finished creating osmnx network.")

    ##########################################################################################
    # STEP 2: ANALYSE POINTS OF INTEREST
    # ------------------- This step analysis times (and count of pois at given time proximity if requested) 
    # ------------------- using function aup.pois_time. This step is based on script 21.
    # ------------------- Main difference lies in how pois are read.

    # Read points of interest (pois)
    aup.log(f"--- Loading all points of interest.")
    pois = gpd.read_file(pois_dir)
    pois = pois[['code','geometry']]
    pois = pois.set_crs("EPSG:4326")
    aup.log(f"--- Loaded {len(pois)} points of interest.")

    aup.log(f"""
------------------------------------------------------------
STARTING source pois proximity to nodes analysis for {city}.""")
    # PREP. FOR ANALYSIS
    i = 0
    # PREP. FOR ANALYSIS - List of columns used to deliver final format of Script part 1
    all_analysis_cols = []
    
    # SOURCE LOOP
    for eje in parameters.keys():
        for amenity in parameters[eje]:
            for source in parameters[eje][amenity]:
                source_analysis_cols = []

                aup.log(f"""
Analysing source {source}.""")
                
                # 2.1 --------------- SAVE ANALYSIS COLUMN NAMES
                # Source col to lists
                source_analysis_cols.append(source)
                all_analysis_cols.append(source)
                # If counting pois, create and append column 
                # count_col formated example: 'denue_preescolar_15min'
                if count_pois[0]:
                    count_col = f'{source}_{count_pois[1]}min'
                    source_analysis_cols.append(count_col)
                    all_analysis_cols.append(count_col)

                # 2.2 --------------- GET POIS - Select source points of interest
                # (concats all data corresponding to current source in source_pois)
                source_pois = gpd.GeoDataFrame()
                for code in parameters[eje][amenity][source]:
                    code_pois = pois.loc[pois['code'] == code]
                    source_pois = pd.concat([source_pois,code_pois])
                aup.log(f"--- {source_pois.shape[0]} {source} pois. Analysing source pois proximity to nodes.")
                
                # 2.3 --------------- SOURCE ANALYSIS
                # Calculate time data from nodes to source
                if database_network:
                    #Uses Network available in database, which has time_min field.
                    source_nodes_time = aup.pois_time(G, nodes, edges, source_pois, source, prox_measure='time_min', count_pois=count_pois)
                else:
                    #Uses OSMnx created network 
                    source_nodes_time = aup.pois_time(G, nodes, edges, source_pois, source, prox_measure='length', count_pois=count_pois)
                # Format
                source_nodes_time.rename(columns={'time_'+source:source},inplace=True)
                source_nodes_time = source_nodes_time[['osmid']+source_analysis_cols+['x','y','geometry']]

                # 2.4 --------------- OUTPUT MERGE
                # Merge all sources time data in final output nodes gdf
                if i == 0: # For the first analysed source
                    nodes_analysis = source_nodes_time.copy()
                else: # For the following
                    nodes_analysis = pd.merge(nodes_analysis,source_nodes_time[['osmid']+source_analysis_cols],on='osmid')
   
                i = i+1

                if save_space:
                    del source_nodes_time
                    aup.log("Saved space by deleting used data.")

                aup.log(f"--- FINISHED source {source}. Mean city time = {nodes_analysis[source].mean()}")
            
    # 2.5 --------------- Final format for nodes
    column_order = ['osmid'] + all_analysis_cols + ['x','y','geometry']
    nodes_analysis = nodes_analysis[column_order]

    if test:
        nodes_analysis.to_file(nodes_local_save_dir, driver='GPKG')
        aup.log(f"--- Saved {city} nodes gdf locally.")

    if db_save:
        nodes_analysis['city'] = city
        aup.gdf_to_db_slow(nodes_analysis, nodes_save_table, save_schema, if_exists='append')
        aup.log(f"--- Saved {city} nodes gdf in database.")
    
    aup.log(f"""
------------------------------------------------------------
FINISHED source pois proximity to nodes analysis for {city}.""")

    ##########################################################################################
    # STEP 3: AMENITIES ANALYSIS
    # ------------------- This step is based on Script 21.

    # 3.0 --------------- DEFINITIONS DICTIONARY
    # ------------------- On script 15 a dictionary (idx_15_min) is used to calculate the times to amenities.
    # ------------------- This step creates the definitions dicc out of the main parameters dicc.
    
    definitions = {}
    for eje in parameters.keys():
        # tmp_dicc stores all {amenity:[source_list]} for each eje
        tmp_dicc = {}
        for amenity in parameters[eje]:
            items_lst = []
            items = list(parameters[eje][amenity].items())
            for item in items:
                items_lst.append(item[0])
            tmp_dicc[amenity] = items_lst
        # Each eje gets assigned its own tmp_dicc
        definitions[eje] = tmp_dicc
    
    # 3.1 --------------- FILL FOR MISSING AMENITIES
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
    
    # 3.2a -------------- AMENITIES ANALYSIS (amenities, ejes and max_time calculation)
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

            elif weight == 'max': # To know distance to farthest closest-source-amenity.
                                  # If need to know proximity to all of the options (e.g. Social)
                nodes_analysis['max_'+ a.lower()] = nodes_analysis[definitions[e][a]].max(axis=1)

            else:
                # Crash on purpose and raise error
                aup.log("--- Error in source_weight dicc.")
                aup.log("--- Must pass 'min' or 'max'.")
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

    # 3.2b -------------- AMENITIES COUNT ANALYSIS (amenities at given time count, optional)
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

        if save_space:
            del nodes_timeanalysis_filter
            del nodes_countanalysis_filter
            aup.log("Saved space by deleting used data.")

    else:
        nodes_analysis_filter = nodes_timeanalysis_filter.copy()

        if save_space:
            del nodes_timeanalysis_filter
            aup.log("Saved space by deleting used data.")

    ##########################################################################################
    # STEP 4: GROUP DATA BY HEX
    # ------------------- This step groups nodes data by hexagon.
    # ------------------- If pop output, also adds pop data. Else, creates hexgrid.
    
    # If pop_output = True, will create a hexgrid that contains population data for all res in res_list.
    if pop_output:
        hex_socio_gdf = aup.create_popdata_hexgrid(aoi,pop_dir,pop_index_column,pop_columns,res_list,projected_crs)

    hex_idx = gpd.GeoDataFrame()

    for res in res_list:
        
        # (a) If not adding population data, just group proximity data by hex.
        if not pop_output:
            # 4a) (1) Create empty hex_tmp of current resolution (already has hex_ID_res col)
            hex_tmp = aup.create_hexgrid(aoi,res)
            hex_tmp = hex_tmp.set_crs("EPSG:4326")
            aup.log(f"Created hexgrid of resolution {res}")
            
            # 4a) (2) Group proximity data by hex
            hex_res_idx = aup.group_by_hex_mean(nodes_analysis_filter, hex_tmp, res, index_column)
            hex_res_idx = hex_res_idx.loc[hex_res_idx[index_column]>0].copy()
            aup.log(f"Grouped nodes data by hexagons res {res}")

            # 4a) (3) Does not add pop data, just renames
            hex_res_pop = hex_res_idx.copy()
        
        # (b) If adding population data, load and calculate pop data, group proximity data by hex and format.
        else:
            # 4b) (1) Load hexgrid that contains population data and create empty hex_tmp
            # Load hex_pop for current resolution
            hex_pop = hex_socio_gdf.loc[hex_socio_gdf['res'] == res]
            # Prepare for function aup.group_by_hex_mean (Requires hex_ID_res col)
            hex_pop.rename(columns={'hex_id':f'hex_id_{res}'},inplace=True)
            hex_pop = hex_pop.set_crs("EPSG:4326")
            hex_tmp = hex_pop[[f'hex_id_{res}','geometry']].copy()
            aup.log(f"Loaded pop hexgrid of resolution {res}")
            
            # 4b) (2) Group proximity data by hex
            hex_res_idx = aup.group_by_hex_mean(nodes_analysis_filter, hex_tmp, res, index_column)
            hex_res_idx = hex_res_idx.loc[hex_res_idx[index_column]>0].copy()
            aup.log(f"Grouped nodes data by hexagons res {res}")

            # 4b (3) Add previously calculated pop data
            pop_fields = pop_columns+['dens_pob_ha']
            pop_list = [f'hex_id_{res}'] + pop_fields
            hex_res_pop = pd.merge(hex_res_idx, hex_pop[pop_list], on=f'hex_id_{res}')

        # 4.4) Format back from col {hex_id_res} to cols {hex_id, res}
        hex_res_pop.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
        hex_res_pop['res'] = res

        # 4.5) Add currently processed resolution to hex_idx
        hex_idx = pd.concat([hex_idx,hex_res_pop])
 
        aup.log(f"Saved grouped data by hexagons res {res}")
        
    if save_space:
        del hex_tmp
        del nodes_analysis_filter
        del hex_res_idx
        if pop_output:
            del hex_socio_gdf #pop_output=True
            del hex_pop #pop_output=True
            del hex_res_pop #pop_output=True      
        aup.log("Saved space by deleting used data.")

    ##########################################################################################
    # STEP 5: RECALCULATION, FINAL DATA AND SAVING
    # ------------------- This step finishes the analysis by hexagon by recalculating and
    # ------------------- adding additional data, finally saves output as instructed.
    
    # 5.1 --------------- RE-CALCULATE MAX TIMES BY HEXAGON
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

    aup.log('Finished recalculating times in hexagons')

    # 5.2 --------------- CALCULATE AND ADD ADDITIONAL AND FINAL DATA
    # ------------------- This step adds mean, median, city and idx data to each hex

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

    aup.log('Finished final format for gdf.')
         
    if save_space:
        del hex_idx
        aup.log("Saved space by deleting used data.")

    # 5.4 --------------- SAVING
    # ------------------- This step saves (locally for tests, to db for script running)
    
    if local_save:
        hex_idx_city.to_file(final_local_save_dir, driver='GPKG')
        aup.log(f"--- Saved {city} gdf locally.")
    if db_save:
        aup.gdf_to_db_slow(hex_idx_city, final_local_save_dir, save_schema, if_exists='append')
        aup.log(f"--- Saved {city} gdf in database.")


if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('Starting script 18.')

    # ------------------------------ BASE DATA REQUIRED ------------------------------
    # Name of area of interest (Required)
    city = 'Aguascalientes'
    # Projected coordinate reference system depending on the global location of the area of interest
    projected_crs = "EPSG:6372" # (For this Script, required. Normally defaults to "EPSG:6372.")
    # Shape of the area of interest (Required directory)
    aoi_dir = "../data/external/temporal_todocker/prox_aoi/aoi_ags.gpkg"

    # Points of interest (Required directory)
    # pois gdf must have a col named 'code' with a unique ID for each type of point of interest.
    # This code will be searched in dicc parameters to be assigned to a source-->amenity-->eje.
    pois_dir = "../data/external/temporal_todocker/prox_aoi/pois_ags.gpkg"
    
    # ---------------------------- SCRIPT CONFIGURATION - ANALYSIS AND OUTPUT OPTIONS ----------------------------
    # Resolutions of hexgrid output (Required)
    res_list = [8,9]
    # Count available amenities at given time proximity (minutes)? (Required)
    count_pois = (False,15) # Must pass a tupple containing a boolean (True or False) and time proximity of interest in minutes (Boolean,time)
    # Save disk space by deleting used data that will not be used after? (Required)
    save_space = True

    # OPTIONAL - Network
    # If true, will download a network from database (Allows us to calculate times using previously processed edges with walking speed - edges_speed)
    # If false, will create a OSMnx Network and use a pedestrian speed of 4km/hr to calculate times
    database_network = True
    # Database locations
    network_schema = 'osmnx' #(Required if database_network = True)
    nodes_table = 'nodes' #(Required if database_network = True)
    edges_table = 'edges_speed' #(Required if database_network = True)

    # OPTIONAL - Population data
    pop_output = True
    # Pop data by block file directory (Required if pop_output = True)
    # Pop data is converted to hex data by using centroids.
    pop_dir = "../data/external/temporal_todocker/prox_aoi/pop_gdf_ags.gpkg"
    # List of columns with pop data. with total pop data (Required if pop_output = True)
    # First item of list must be name of total population column in order to calculate density.
    pop_columns = ['pobtot','pobfem','pobmas']
    # Pop gdf index column (Required if pop_output = True)
    pop_index_column = 'cvegeo'

    # ---------------------------- SCRIPT CONFIGURATION - SAVING ----------------------------
    # Save final output to database?
    db_save = False
    save_schema = 'prox_analysis'
    nodes_save_table = 'nodesproximity_aoi'
    hex_save_table = 'proximityanalysis_aoi'
    # Test - (If testing, Script saves it ONLY locally. (Make sure directory exists)
    test = True
    nodes_local_save_dir = f"../data/processed/prox_aoi/test_script18_{city}_nodes.gpkg"
    final_local_save_dir = f"../data/processed/prox_aoi/test_script18_{city}_hex.gpkg"

    # ---------------------------- SCRIPT CONFIGURATION - POIS STRUCTURE ----------------------------
    # PARAMETERS DICTIONARY (Required)
    # Set the ejes, amenidades, sources and codes for analysis
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

    # WEIGHT DICTIONARY (Required)
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
    
    # ---------------------------- SCRIPT START ----------------------------
    aup.log("--"*40)
    aup.log(f"--- Running Script for city: {city}")
    
    # Script mode:
    if test:
        main(pop_output, db_save=False, local_save=True, save_space=save_space)
    else:
        main(pop_output, db_save=True, local_save=False, save_space=save_space)


