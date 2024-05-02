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


def download_osmnx(aoi):
    #Read area of interest as a polygon
    poly = aoi.geometry

    # Extracts coordinates from polygon as DataFrame
    coord_val = poly.bounds

    # Gets coordinates for bounding box
    n = coord_val.maxy.max()
    s = coord_val.miny.min()
    e = coord_val.maxx.max()
    w = coord_val.minx.min()

    aup.log(f"Extracted min and max coordinates from the municipality. Polygon N:{round(n,5)}, S:{round(s,5)}, E{round(e,5)}, W{round(w,5)}.")

    # Downloads OSMnx graph from bounding box
    G = ox.graph_from_bbox(n, s, e, w, network_type="all_private")

    aup.log("Downloaded data from OSMnx.")

    #Transforms graph to nodes and edges Geodataframe
    nodes, edges = ox.graph_to_gdfs(G)

    #Resets index to access osmid as a column
    nodes.reset_index(inplace=True)

    #Resets index to acces u and v as columns
    edges.reset_index(inplace=True)

    aup.log(f"Converted OSMnx graph to {len(nodes)} nodes and {len(edges)} edges GeoDataFrame.")

    # Defines columns of interest for nodes and edges
    nodes_columns = ["osmid", "x", "y", "street_count", "geometry"]
    edges_columns = [
        "osmid",
        "v",
        "u",
        "key",
        "oneway",
        "lanes",
        "name",
        "highway",
        "maxspeed",
        "length",
        "geometry",
        "bridge",
        "ref",
        "junction",
        "tunnel",
        "access",
        "width",
        "service",
    ]

    # if column doesn't exist it creates it as nan
    for c in nodes_columns:
        if c not in nodes.columns:
            nodes[c] = np.nan

            aup.log(f"Added column {c} for nodes.")

    for c in edges_columns:
        if c not in edges.columns:
            edges[c] = np.nan

            aup.log(f"Added column {c} for edges.")

    # Filters GeoDataFrames for relevant columns
    nodes = nodes[nodes_columns]
    edges = edges[edges_columns]

    aup.log("Filtered columns.")
    
    # Converts columns with lists to strings to allow saving to local and further processes.
    for col in nodes.columns:
        if any(isinstance(val, list) for val in nodes[col]):
            nodes[col] = nodes[col].astype('string')

            aup.log(f"Column: {col} in nodes gdf, has a list in it, the column data was converted to string.")
    
    for col in edges.columns:
        if any(isinstance(val, list) for val in edges[col]):
            edges[col] = edges[col].astype('string')

            aup.log(f"Column: {col} in nodes gdf, has a list in it, the column data was converted to string.")
    
    return G,nodes,edges


def create_popdata_hexgrid(aoi,pop_dir,pop_column,pop_index_column,res_list):
    
    pop_gdf = gpd.read_file(pop_dir)
    
    # Format and isolate data of interest
    pop_gdf = pop_gdf.to_crs("EPSG:4326")
    pop_index_column = pop_index_column.lower()
    pop_column = pop_column.lower()
    pop_gdf.columns = pop_gdf.columns.str.lower()
    block_pop = pop_gdf[[pop_index_column,pop_column,'geometry']]

    # Extract point from polygon
    block_pop = block_pop.to_crs("EPSG:6372")
    block_pop = block_pop.set_index(pop_index_column)
    point_within_polygon = gpd.GeoDataFrame(geometry=block_pop.representative_point())

    # Add census data to points
    centroid_block_pop = point_within_polygon.merge(block_pop, right_index=True, left_index=True) 

    # Format centroid with pop data
    centroid_block_pop.drop(columns=['geometry_y'], inplace=True)
    centroid_block_pop.rename(columns={'geometry_x':'geometry'}, inplace=True)
    centroid_block_pop = gpd.GeoDataFrame(centroid_block_pop, geometry='geometry')
    centroid_block_pop = centroid_block_pop.to_crs("EPSG:4326")
    centroid_block_pop = centroid_block_pop.reset_index()
    centroid_block_pop.rename(columns={pop_column:'pobtot'},inplace=True)

    aup.log(f"Converted to centroids with {centroid_block_pop.pobtot.sum()} " + f"pop vs {block_pop[pob_column].sum()} pop in original gdf.")
    
    # create buffer for aoi to include outer blocks when creating hexgrid
    aoi_buffer = aoi.copy()
    aoi_buffer = aoi_buffer.dissolve()
    aoi_buffer = aoi_buffer.to_crs("EPSG:6372").buffer(2500)
    aoi_buffer = gpd.GeoDataFrame(geometry=aoi_buffer)
    aoi_buffer = aoi_buffer.to_crs("EPSG:4326")

    hex_socio_gdf = gpd.GeoDataFrame()

    for res in res_list:
        # Generate hexagon gdf
        hex_gdf = aup.create_hexgrid(aoi_buffer, res)
        hex_gdf = hex_gdf.set_crs("EPSG:4326")

        # Format - Remove res from index name and add column with res
        hex_gdf.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
        hex_gdf['res'] = res

        aup.log(f"Created hex_grid with {res} resolution")
   
        # Group pop data
        string_columns = [pop_index_column]
        hex_socio_df = aup.socio_points_to_polygon(hex_gdf, centroid_block_pop,'hex_id', string_columns)
   
        aup.log(f"Agregated socio data to hex with a total of {hex_socio_df.pobtot.sum()} population for resolution {res}.")

        # Hexagons to GeoDataFrame
        hex_socio_gdf_tmp = hex_gdf.merge(hex_socio_df, on='hex_id')

        hectares = hex_socio_gdf_tmp.to_crs("EPSG:6372").area / 10000
        hex_socio_gdf_tmp['dens_pob_ha'] = hex_socio_gdf_tmp['pobtot'] / hectares
   
        aup.log(f"Calculated an average density of {hex_socio_gdf_tmp.dens_pob_ha.mean()}")

        hex_socio_gdf = pd.concat([hex_socio_gdf,hex_socio_gdf_tmp])    

    aup.log(f"Finished calculating population by hexgrid for res {res_list}.")
    
    return hex_socio_gdf


def apply_sigmoidal(x):
    if x == -1:
        return -1
    elif x > 1000:
        return 0
    else:
        val = aup.sigmoidal_function(0.1464814753435666, x, 30)
        return val


def main(save = False, save_space = False):

    ##########################################################################################
    # STEP 1: CREATE OSMNX NETWORK
    # ------------------- This step downloads the osmnx network for the area of interest.

    # Read area of interest (aoi)
    aoi = gpd.read_file(aoi_dir)
    aoi = aoi.to_crs("EPSG:4326")
    aup.log(f"--- Starting creation of osmnx network.")

    # Download osmnx network (G, nodes and edges from bounding box of aoi)
    G, nodes, edges = aup.create_osmnx_network(aoi,how='from_bbox')
    aup.log(f"--- Finished creating osmnx network.")

    ##########################################################################################
    # STEP 2: ANALYSE POINTS OF INTEREST
    # ------------------- This step analysis times (and count of pois at given time proximity if requested) using function aup.pois_time.

    # Read points of interest (pois)
    aup.log(f"--- Loading points of interest.")
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

                aup.log(f"--- FINISHED source {source}. Mean city time = {nodes_analysis[source].mean()}")
            
    # 2.5 --------------- Final format for nodes
    column_order = ['osmid'] + all_analysis_cols + ['x','y','geometry']
    nodes_analysis = nodes_analysis[column_order]

    if test:
        nodes_analysis.to_file(nodes_local_save_dir, driver='GPKG')
        aup.log(f"--- Saved {city} nodes gdf locally.")

    if save:
        nodes_analysis['city'] = city
        aup.gdf_to_db_slow(nodes_analysis, nodes_save_table, save_schema, if_exists='append')
        aup.log(f"--- Saved {city} nodes gdf in database.")
    
    aup.log(f"""
------------------------------------------------------------
FINISHED source pois proximity to nodes analysis for {city}.""")

    ##########################################################################################
    # STEP 3: AMENITIES ANALYSIS
    # ------------------- This step is based on Script 15.

    # 3.0 --------------- DEFINITIONS DICTIONARY
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

            elif weight == 'max': # To know distance to farthest source amenity.
                                  # If need to know proximity to all of the options (e.g. Social)
                nodes_analysis['max_'+ a.lower()] = nodes_analysis[definitions[e][a]].max(axis=1)

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

    else:
        nodes_analysis_filter = nodes_timeanalysis_filter.copy()

    ##########################################################################################
    # STEP 4: GROUP DATA BY HEX
    # ------------------- This step groups nodes data by hexagon.
    # ------------------- If pop output, also adds pop data. Else, creates hexgrid.
    
    # If pop_output = True, will create a hexgrid that contains population data.
    if pop_output:
        hex_socio_gdf = create_popdata_hexgrid(aoi,pop_dir,pop_column,pop_index_column,res_list)

    hex_idx = gpd.GeoDataFrame()

    for res in res_list:
        
        #/////////////////////////////////////////////// HEXGRID DEPENDS ON POP DATA BEING CALCULATED OR NOT ///////////////////////////////////////////////
        # If pop_output is true, loads previously created hexgrid with pop data
        if pop_output:
            # Load hexgrid
            hex_pop = hex_socio_gdf.loc[hex_socio_gdf['res'] == res]
            #Function group_by_hex_mean requires ID to include resolution
            hex_pop.rename(columns={'hex_id':f'hex_id_{res}'},inplace=True)
            # Create hex_tmp
            hex_pop = hex_pop.set_crs("EPSG:4326")
            hex_tmp = hex_pop[[f'hex_id_{res}','geometry']].copy()
            
            aup.log(f"Loaded pop hexgrid of resolution {res}")
            
        # If pop_output is false, creates hexgrid
        else:
            # Create hexgrid (which already has ID_res)
            hexgrid = aup.create_hexgrid(aoi,res)
            # Create hex_tmp
            hexgrid = hexgrid.set_crs("EPSG:4326")
            hex_tmp = hexgrid.copy()

            aup.log(f"Created hexgrid of resolution {res}")
            
        #///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        
        # group data by hex
        hex_res_idx = aup.group_by_hex_mean(nodes_analysis_filter, hex_tmp, res, index_column)
        hex_res_idx = hex_res_idx.loc[hex_res_idx[index_column]>0].copy()
        
        aup.log(f"Grouped nodes data by hexagons res {res}")
        
        #////////////////////////////////////////////////////// ADD POP DATA IF POP DATA IS CONSIDERED /////////////////////////////////////////////////////
        # Add pop data
        if pop_output:
            pop_list = [f'hex_id_{res}','pobtot','dens_pob_ha']
            hex_res_pop = pd.merge(hex_res_idx, hex_pop[pop_list], on=f'hex_id_{res}')
        else:
            hex_res_pop = hex_res_idx.copy()
        #///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////    
        
        # After funtion group_by_hex_mean we can remove res from ID and set as a column
        hex_res_pop.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
        hex_res_pop['res'] = res
        hex_idx = hex_idx.append(hex_res_pop)
 
        aup.log(f"Saved grouped data by hexagons res {res}")
        
    if save_space:
        if pop_output:
            del hex_socio_gdf #pop_output=True
            del hex_pop #pop_output=True
            del hex_tmp
            del nodes_analysis_filter
            del hex_res_idx
            del hex_res_pop #pop_output=True
        else:
            del hexgrid #pop_output=False
            del hex_tmp
            del nodes_analysis_filter
            del hex_res_idx
        aup.log("Saved space by deleting used data.")

    # Recalculate ejes max times by hexagon ------------------------------------------------------------------------ 
    
    # This step recalculates max time to each eje from max times to calculated amenities and max_time from max eje
    column_max_ejes = [] # list with ejes index column names
    #Goes (again) through each eje in dictionary:
    for e in definitions.keys():
        column_max_ejes.append('max_'+ e.lower())
        column_max_amenities = [] # list with amenities in current eje
        #Goes (again) through each amenity of current eje:    
        for a in definitions[e].keys():
            column_max_amenities.append('max_'+ a.lower())
        #Re-calculates time to currently examined eje (max time of its amenities):        
        hex_idx['max_'+ e.lower()] = hex_idx[column_max_amenities].max(axis=1)

    aup.log('Finished recalculating times in hexagons')

    # Calculate index and additional data ------------------------------------------------------------------------ 
    
    # Index, median and mean calculation
    # Index - Apply sigmodial function to amenities columns without ejes
    max_amenities_cols = [i for i in column_max_all if i not in column_max_ejes]
    max_amenities_cols.remove('max_time')
    max_amenities_cols.remove('osmid')
    max_amenities_cols.remove('geometry')

    idx_amenities_cols = [] # list with idx amenity column names
    for ac in max_amenities_cols:
        idx_col = ac.replace('max','idx')
        hex_idx[idx_col] = hex_idx[ac].apply(apply_sigmoidal)
        idx_amenities_cols.append(idx_col)

    # Mean, median and city data
    hex_idx[index_column] = hex_idx[column_max_ejes].max(axis=1)
    hex_idx['mean_time'] = hex_idx[max_amenities_cols].mean(axis=1)
    hex_idx['median_time'] = hex_idx[max_amenities_cols].median(axis=1)
    hex_idx['idx_sum'] = hex_idx[idx_amenities_cols].sum(axis=1)
    hex_idx['city'] = city
   
    aup.log('Finished calculating index, mean and median time')

    # Final format (column reordering) ------------------------------------------------------------------------ 
    
    # First elements of ordered list - ID and geometry
    first_elements = ['hex_id','res','geometry']
    # Second elements of ordered list - max_ejes and max_amenities removing max_time, osmid and geometry.
    column_max_ejes_amenities = column_max_all.copy()
    column_max_ejes_amenities.remove('max_time')
    column_max_ejes_amenities.remove('osmid')
    column_max_ejes_amenities.remove('geometry')
    # Third elements of ordered list are listed in idx_amenities_cols
    # Fourth elements of ordered list - Mean, median, max and idx
    fourth_elements = ['mean_time', 'median_time', 'max_time', 'idx_sum']
    # Fifth elements - If pop is calculated - Pop data
    fifth_elements = ['pobtot', 'dens_pob_ha']
    # Last element - City data
    last_element = ['city']

    # New order
    if pop_output:
        final_column_ordered_list = first_elements + column_max_ejes_amenities + idx_amenities_cols + fourth_elements + fifth_elements + last_element
    else:
        final_column_ordered_list = first_elements + column_max_ejes_amenities + idx_amenities_cols + fourth_elements + last_element
    
    # Apply new order
    hex_idx_city = hex_idx[final_column_ordered_list]

    if save_space:
        del hex_idx
        aup.log("Saved space by deleting used data.")

    aup.log('Finished final format')

    #-----------------------------------------------------------------------------------------------------------------------------------------------------
    # STEP 5: Saving
    #-----------------------------------------------------------------------------------------------------------------------------------------------------

    if save:
        aup.df_to_db_slow(hex_idx_city, hex_save_table, save_schema, if_exists='append')


if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('Starting script 18.')

    # REQUIRED DATA
    # Name of area of interest
    city = 'Aguascalientes'
    # Resolutions of hexgrid output
    res_list = [8,9]
    # Save final output to db?
    save = True
    save_schema = 'prox_analysis'
    nodes_save_table = 'nodesproximity_aoi'
    hex_save_table = 'proximityanalysis_aoi'
    # Save disk space by deleting used data that will not be used after?
    save_space = True

    # Test - (If testing, Script saves it ONLY locally. (Make sure directory exists)
    test = True
    nodes_local_save_dir = f"../data/processed/prox_aoi/test_{city}_script18_nodes.gpkg"
    final_local_save_dir = f"../data/processed/prox_aoi/test_{city}_script18_hex.gpkg"

    # Required directories
    aoi_dir = "../../data/external/prox_latam/aoi_ags.gpkg"
    pois_dir = "../../data/external/prox_latam/pois_ags.gpkg"
    
    # ---------------------------- SCRIPT CONFIGURATION - ANALYSIS AND OUTPUT OPTIONS ----------------------------
    # Network distance method used in function pois_time. (If length, assumes pedestrian speed of 4km/hr.)
    prox_measure = 'time_min' # Must pass 'length' or 'time_min'
    # Count available amenities at given time proximity (minutes)?
    count_pois = (False,15) # Must pass a tupple containing a boolean (True or False) and time proximity of interest in minutes (Boolean,time)
    # OPTIONAL (required if pop_output = True)
    pop_output = True
    # Pop data file directory
    pop_dir = "../../data/external/prox_latam/pop_gdf_ags.gpkg"
    # Column with total pop data
    pop_column = 'pobtot'
    # Pop gdf index column
    pop_index_column = 'cvegeo'

    # PARAMETERS DICTIONARY
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
    
    aup.log("--"*40)
    aup.log(f"--- Running Script for city: {city}")
    if test:
        main(pop_output, save=False, local_save=True, save_space=save_space)
    else:
        main(pop_output, save=True, local_save=False, save_space=save_space)


