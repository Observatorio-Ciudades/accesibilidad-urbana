import geopandas as gpd
import pandas as pd
import numpy as np

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from shapely.geometry import Point
import osmnx as ox

from tqdm import tqdm
import h3

import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


def create_filtered_navigable_network(public_space_quality_dir, projected_crs, filtering_column, filtering_value):

    # 1.0 --------------- LOAD DATA
    # ------------------- This step loads the public space quality index gdf for the current project
    # Load data
    pub_space_qty = gpd.read_file(public_space_quality_dir)
    # Set CRS
    pub_space_qty = pub_space_qty.set_crs(projected_crs)
    # Filter for data of relevance
    gdf = pub_space_qty[[filtering_column,'geometry']].copy()
    gdf = gdf.to_crs("EPSG:4326")

    # 2.0 --------------- EXTRACT VERTICES
    # ------------------- This step extracts points from each linestring and stores them in gdf_points.
    # Explode multi-part geometries into single parts
    gdf_exploded = gdf.explode(index_parts=False)
    # Reset index
    gdf_exploded.reset_index(inplace=True)
    gdf_exploded.drop(columns=['index'],inplace=True)
    #Initialize an empty list to store the points and its values
    points = []
    attributes = []
    #Iterate through each LineString and extract its vertices
    for idx, row in gdf_exploded.iterrows():
        line = row.geometry
        for coord in line.coords:
            points.append(Point(coord))
            attributes.append(row[filtering_column])
    # Create a new GeoDataFrame from the points
    gdf_points = gpd.GeoDataFrame(attributes,geometry=points)
    # Rename data
    gdf_points.rename(columns={0:filtering_column},inplace=True)

    # 3.0 --------------- CREATE NODES AND EDGES COMPATIBLE WITH OSMnx AND FILTER THEM.
    # ------------------- This step uses the lines and points available to create nodes and edges, then filters by filtering value.
    # Create nodes and edges
    nodes = gdf_points.copy()
    edges = gdf_exploded.copy()

    # Set gdf CRS
    try:
        nodes = nodes.to_crs("EPSG:4326")
    except:
        nodes = nodes.set_crs("EPSG:4326")
    try:
        edges = edges.to_crs("EPSG:4326")
    except:
        edges = edges.set_crs("EPSG:4326")

    nodes, edges = aup.create_network(nodes, edges, projected_crs)
    # Filter them
    edges_filt = edges.loc[edges[filtering_column] >= filtering_value]

    # 4.0 --------------- CREATE NAVIGABLE NETWORK
    # ------------------- This step creates G from the previous nodes and edges_filt.
    # Format nodes and edges
    nodes_gdf = nodes.copy()
    nodes_gdf.set_index('osmid',inplace=True)
    edges_gdf = edges_filt.copy()
    edges_gdf.set_index(['u','v','key'],inplace=True)
    # Set x and y columns
    nodes_gdf['x'] = nodes_gdf['geometry'].x
    nodes_gdf['y'] = nodes_gdf['geometry'].y
    # Create network G
    G = ox.graph_from_gdfs(nodes_gdf, edges_gdf)

    return G, nodes_gdf, edges_gdf

##########################################################################################################################################
# SCALE FUNCTIONS

def rare_fn(cont):
    if cont == 0:
        res_val = 0
    elif cont > 0 and cont < 2:
        res_val = res_val_regression(0, 2, 0, 2.5, cont)
    elif cont >= 2 and cont < 4:
        res_val = res_val_regression(2, 4, 2.5, 5, cont)
    elif cont >= 4 and cont < 7:
        res_val = res_val_regression(4, 7, 5, 7.5, cont)
    elif cont >= 7 and cont < 10:
        res_val = res_val_regression(7, 10, 7.5, 10, cont)
    elif cont >= 10:
        res_val = 10
    
    return res_val


def very_rare_fn(cont):
    min_x = 0
    max_x = 1
    min_y = 0
    max_y = 10
    
    return res_val_regression(min_x, max_x, min_y, max_y, cont)


def frequent_fn(cont):
    if cont == 0:
        res_val = 0
    elif cont > 0 and cont < 6:
        res_val = res_val_regression(0, 6, 0, 2.5, cont)
    elif cont >= 6 and cont < 12:
        res_val = res_val_regression(6, 12, 2.5, 5, cont)
    elif cont >= 12 and cont < 18:
        res_val = res_val_regression(12, 18, 5, 7.5, cont)
    elif cont >= 18 and cont < 25:
        res_val = res_val_regression(18, 25, 7.5, 10, cont)
    elif cont >= 25:
        res_val = 10
    
    return res_val


def res_val_regression(min_x, max_x, min_y, max_y, cont):
    slope = (max_y-min_y)/(max_x-min_x)
    intersect = min_y - slope * min_x
    res_val = slope * cont + intersect
    if cont > max_x:
        res_val = max_y
        
    return res_val


def office_fn(cont):
    if cont == 0:
        res_val = 0
    elif cont > 0 and cont < 2.823938308:
        res_val = res_val_regression(0, 2.823938308, 0, 2.5, cont)
    elif cont >= 2.823938308 and cont <  5.539263604:
        res_val = res_val_regression(2.823938308, 5.539263604, 2.5, 5, cont)
    elif cont >= 5.539263604 and cont < 10.96991420:
        res_val = res_val_regression(5.539263604, 10.96991420, 5, 7.5, cont)
    elif cont >= 10.96991420 and cont < 16.40056479:
        res_val = res_val_regression(10.96991420, 16.40056479, 7.5, 10, cont)
    elif cont >= 16.40056479:
        res_val = 10
    
    return res_val


def ndvi_fn(cont):
    min_x = 0
    max_x = 0.4
    min_y = 0
    max_y = 10
    if cont > max_x:
        return 10
    elif cont <= min_x:
        return 0
    else:
        return res_val_regression(min_x, max_x, min_y, max_y, cont)


def inter_fn(cont):
    min_x = 20
    max_x = 100
    min_y = 0
    max_y = 10
    if cont > max_x:
        return 10
    elif cont < min_x:
        return 0
    else:
        return res_val_regression(min_x, max_x, min_y, max_y, cont)


def noise_fn(cont):
    min_x = 55
    max_x = 70
    min_y = 10
    max_y = 0
    if cont > max_x:
        return 0
    elif cont < min_x:
        return 10
    else:
        return res_val_regression(min_x, max_x, min_y, max_y, cont)


def temp_fn(area_analysis, cont, mean, std):
    if cont >= (mean + 2*std):
        res_val = 0
    elif cont < (mean + 2*std) and cont >= (mean + std):
        res_val = res_val_regression((mean + std), (mean + 2*std), 2.5, 0, cont)
    elif cont < (mean + std) and cont >= (mean):
        res_val = res_val_regression((mean), (mean + std), 5, 2.5, cont)
    elif cont < (mean) and cont >= (mean - std):
        res_val = res_val_regression((mean - std), (mean), 7.5, 5, cont)
    elif cont < (mean - std) and cont >= (mean - 2*std):
        res_val = res_val_regression((mean - 2*std), (mean - std), 10, 7.5, cont)
    elif cont < (mean - 2*std):
        res_val = 10
    if area_analysis == 'santiago':
        res_val = 5
    
    return res_val


def household_fn(cont):
    res_val = res_val_regression(0, 50, 0, 10, cont)
    
    return res_val

    
def social_viv_fn(cont):
    min_x = 0
    max_x = 20
    min_y = 0
    max_y = 10
    if cont > max_x:
        return 10
    elif cont < min_x:
        return 0
    else:
        return res_val_regression(min_x, max_x, min_y, max_y, cont)
    

def specific_fn(cont, source, area_analysis, mean, std):
    if 'ndvi' in source:
        return ndvi_fn(cont)
    elif 'inter' in source:
        return inter_fn(cont)
    elif 'noise' in source:
        return noise_fn(cont)
    elif 'temp' in source:
        return temp_fn(area_analysis, cont, mean, std)
    elif 'houses' in source:
        return household_fn(cont)
    elif 'social_viv' in source:
        return social_viv_fn(cont)
    elif 'oficinas' in source:
        return office_fn(cont)


def scale_source_fn(cont, source, weight_dict, area_analysis, mean, std):
    if weight_dict[source] == 'rare':
        return rare_fn(cont)
    elif weight_dict[source] == 'very_rare':
        return very_rare_fn(cont)
    elif weight_dict[source] == 'frequent':
        return frequent_fn(cont)
    elif weight_dict[source] == 'specific':
        return specific_fn(cont, source, area_analysis, mean, std)
    

def neighbour_mean(hex_id, hex_id_name, hex_bins, col_name):
    return hex_bins.loc[hex_bins[hex_id_name].isin(h3.k_ring(hex_id,1)),col_name].mean()

##########################################################################################################################################
# HQSL FUNCTIONS

def hqsl_fn(hex_gdf, parameters_dict, code_column):

    hex_gdf = hex_gdf.copy()
    
    social_function_list = []
    
    for social_function in parameters_dict.keys():
        social_function_list.append(social_function)
    
    hex_gdf['hqsl'] = hex_gdf[social_function_list].sum(axis=1)

    base_columns = [code_column,'geometry']
    filter_list = ['hqsl']
    filter_list.extend(base_columns)
    hex_gdf = hex_gdf[filter_list].copy()
    
    return hex_gdf


def social_fn(hex_gdf, parameters_dict, code_column):
    
    hex_gdf = hex_gdf.copy()
    
    for social_function in parameters_dict.keys():
        source_list = []
        
        for indicator in parameters_dict[social_function].keys():
            source_list.extend(parameters_dict[social_function][indicator])
        
        source_list = [s+'_scaled' for s in source_list]
        hex_gdf[social_function] = hex_gdf[source_list].mean(axis=1)

    base_columns = [code_column,'geometry']
    filter_list = list(parameters_dict.keys())
    filter_list.extend(base_columns)
    hex_gdf = hex_gdf[filter_list].copy()
    
    return hex_gdf


def indicator_fn(hex_gdf, parameters_dict, code_column):
    hex_ind = hex_gdf.copy()

    filter_list = []
    
    indicator_list = list(set().union(*parameters_dict.values()))
    for indicator in indicator_list:
        social_indicator = []
        
        for social_function in parameters_dict.keys():
            social_indicator.append(indicator+'_'+social_function)
            
            source_indicator = parameters_dict[social_function][indicator]
            source_indicator = [s+'_scaled' for s in source_indicator]
            
            hex_ind[indicator+'_'+social_function] = hex_ind[source_indicator].mean(axis=1)
    
        hex_ind[indicator] = hex_ind[social_indicator].sum(axis=1)
        filter_list.extend(social_indicator)
        filter_list.append(indicator)
    
    base_columns = [code_column,'geometry']
    filter_list.extend(base_columns)
    hex_ind = hex_ind[filter_list].copy()
            
    return hex_ind

##########################################################################################################################################
# MAIN FUNCTION

def main(source_list, aoi, G, nodes, edges, walking_speed, local_save,santiago_tmp_fix):
    
    ############################################################### PART 1 ###############################################################
    #################################################### FIND NODES PROXIMITY TO POIS ####################################################
    # ------------------- This step loads each source of interest, calculates nodes proximity and saves it to nodes_analysis

    aup.log(f"STARTING PART 1: NODES PROXIMITY TO POINTS OF INTEREST.")

    k = len(source_list)
    i = 1
    all_source_cols =[]

    for source in source_list:

        source_cols =[]

        # ----------
        # UNIQUE ID CONSIDERATION
        # Check if current source has a unique ID that needs to be considered in the process
        if source in unique_id_sources:
            unique_id = True
        else:
            unique_id = False
        # ----------

        aup.log("--"*40)
        aup.log(f"--- Starting nodes proximity to pois for source {i}/{k}: {source}. ")

        # 1.1) Read pois from pois dir
        aup.log(f"--- Source {i}/{k} (1.1) - Reading pois dir.")
        # Directory where pois to be examined are located
        pois_dir = all_pois_dir + f'{source}.gpkg'
        # Load all pois from directory
        pois = gpd.read_file(pois_dir)

        # ----------
        # UNIQUE ID AND SMALL PARKS CONSIDERATION
        if unique_id:
            if source == 'ep_plaza_small(canceled)':
                # 'ep_plaza_small' different processing was canceled due to implementation
                # of new nearest data.
                a="""
                # For small parks, area is relevant to sub-divide process (below 2000m2 --> pois_time(), above 2000m2 --> id_pois_time())
                pois = pois[['area_ha','ID','geometry']]
                """
            else:
                # For the rest, keep already existing unique ID and geometry
                pois = pois[['ID','geometry']]
        else:
            # If not unique_ID, ID col is source name (irrelevant), keeps geometry
            pois['ID'] = source
            pois = pois[['ID','geometry']]
        # ----------

        # Format
        try:
            pois = pois.to_crs("EPSG:4326")
        except:
            pois = pois.set_crs("EPSG:4326")

        # 1.2) Clip pois to aoi
        source_pois = gpd.sjoin(pois, aoi)

        # ----------
        # SMALL PARKS CONSIDERATION
        if source == 'ep_plaza_small(canceled)':
            # 'ep_plaza_small' different processing was canceled due to implementation
            # of new nearest data.
            a="""
            # For small parks, area is relevant to sub-divide process
            source_pois = source_pois[['area_ha','ID','geometry']]
            """
        else:
            source_pois = source_pois[['ID','geometry']]
        # ----------

        aup.log(f"--- Source {i}/{k} (1.2) - Keeping {len(source_pois)} pois inside aoi from original {len(pois)} pois.")

        if save_space:
            del pois

        # 1.3) Calculate nodes proximity
        # ----------
        # UNIQUE ID AND SMALL PARKS CONSIDERATION
        if unique_id:
            #################################################### SMALL PARKS ONLY [SECTION STARTS]
            if source == 'ep_plaza_small(canceled)':
                # 'ep_plaza_small' different processing was canceled due to implementation
                # of new nearest data.
                a = """
                aup.log(f"--- Source {i}/{k} (1.3) - Calculating nodes proximity for special case.")

                # pois_time() [for public spaces below 2000m2]
                # For VERY small public spaces (below 2000m2), the proximity analysis will consider any poi derived from the geometry of interest (goi, polygon) because anyway it is small.
                # Because we just care about one poi only (any), this step filters and drops duplicate IDs, keeping the first occurrence.
                very_small_source_pois = source_pois.loc[source_pois['area_ha']<0.2].copy().drop_duplicates(subset='ID')
                # Calculate time data from nodes to source for very_small_source_pois (Has 1 pois for each goi)
                aup.log(f"--- Calculating very small {source} nodes proximity with function pois_time().")
                source_nodes_time_1 = aup.pois_time(G, nodes, edges, very_small_source_pois, source,'length',
                                                    walking_speed, count_pois, projected_crs)
                if save_space:
                    del very_small_source_pois
                
                # id_pois_time() [for public spaces above 2000m2]
                # For larger public spaces (above 2000m2), having several accesses becomes relevant, and goi IDs becomes necessary (needs id_pois_time() function)
                small_source_pois = source_pois.loc[source_pois['area_ha']>=0.2].copy()
                # Calculate time data from nodes to source for small_source_pois (Has n pois for each goi, needs goi_id)
                aup.log(f"--- Calculating not that small {source} nodes proximity with function id_pois_time().")
                source_nodes_time_2 = aup.id_pois_time(G, nodes, edges, small_source_pois, source,'length',
                                                       walking_speed, goi_id='ID', count_pois=count_pois, projected_crs=projected_crs)
                if save_space:
                    del small_source_pois

                # Now merge source_nodes_time_1 results with source_nodes_time_2 results.
                if count_pois[0]:
                    source_nodes_time_all = source_nodes_time_1.merge(source_nodes_time_2[['osmid', 'time_'+source, f'{source}_{count_pois[1]}min']],on='osmid')
                else:
                    source_nodes_time_all = source_nodes_time_1.merge(source_nodes_time_2[['osmid', 'time_'+source]],on='osmid')

                if save_space:
                    del source_nodes_time_1
                    del source_nodes_time_2
                
                # For time data, find *min* time between both source_nodes_time.
                time_cols = [f'time_{source}_x', f'time_{source}_y']
                source_nodes_time_all[f'time_{source}'] = source_nodes_time_all[time_cols].min(axis=1)
                source_nodes_time_all.drop(columns=time_cols,inplace=True)

                # For count data, find *sum* of counted pois for both source_nodes_time
                if count_pois[0]:
                    count_cols = [f'{source}_{count_pois[1]}min_x',f'{source}_{count_pois[1]}min_y']
                    source_nodes_time_all[f'{source}_{count_pois[1]}min'] = source_nodes_time_all[count_cols].sum(axis=1)
                    source_nodes_time_all.drop(columns=count_cols,inplace=True)

                # Finally, rename result
                source_nodes_time = source_nodes_time_all.copy()
                
                if save_space:
                    del source_nodes_time_all
                    """
            #################################################### SMALL PARKS ONLY [SECTION ENDS]

            else:
                aup.log(f"--- Source {i}/{k} (1.3) - Calculating nodes proximity for unique ID case.")
                # Function id_pois_time() consideres the unique ID belonging to each geometry of interest (goi).
                source_nodes_time = aup.id_pois_time(G, nodes, edges, source_pois, source, 'length', walking_speed, 
                                                    goi_id='ID', count_pois=count_pois, projected_crs=projected_crs,
                                                    santiago_tmp_fix=santiago_tmp_fix)
        else:
            aup.log(f"--- Source {i}/{k} (1.3) - Calculating nodes proximity for regular case.")
            # Function pois_time() calculates proximity data from nodes to source (all) without considering any unique ID.
            source_nodes_time = aup.pois_time(G, nodes, edges, source_pois, source,'length',walking_speed, 
                                              count_pois, projected_crs,
                                              santiago_tmp_fix=santiago_tmp_fix)
        # ----------

        if save_space:
            del source_pois

        #### Changes when comparing to Script 23, 23b and notebook 04b:
        # Previously we formated nodes analysis as tidy format in order to be able to loop-upload nodes proximity data.
        # That was relevant as new data was flowing each day. However, that's no longer needed.
        # Instead, data is formated directly and added to nodes_analysis
        ####

        # 1.4) New nodes_analysis format (Not tidy data)
        # Rename time column
        source_nodes_time.rename(columns={'time_'+source:f'{source}_time'}, inplace=True)
        # Register time column
        source_cols.append(f'{source}_time') # Current source only
        all_source_cols.append(f'{source}_time') # All sources, this list will be used in PART 2.

        # Rename and format count column
        if count_pois[0]:
            source_nodes_time.rename(columns={f'{source}_{count_pois[1]}min':f'{source}_count_{count_pois[1]}min'}, inplace=True)
            source_nodes_time[f'{source}_count_{count_pois[1]}min'] = source_nodes_time[f'{source}_count_{count_pois[1]}min'].astype(int)
            # Register count column
            source_cols.append(f'{source}_count_{count_pois[1]}min') # Current source only
            all_source_cols.append(f'{source}_count_{count_pois[1]}min') # All sources, this list will be used in PART 2.

        # Create or append to nodes_analysis
        if i == 1:
            nodes_analysis = source_nodes_time[['osmid','geometry']+source_cols]
            aup.log(f"--- Source {i}/{k} (1.4) - Created nodes analysis with {len(source_nodes_time)} for the first time.")
        else:
            nodes_analysis = nodes_analysis.merge(source_nodes_time[['osmid']+source_cols], on='osmid', how='left')
            aup.log(f"--- Source {i}/{k} (1.4) - Appended {len(source_nodes_time)} nodes to nodes analysis.")
        
        if save_space:
            del source_nodes_time
        
        i+=1
    
    ############################################################### PART 2 ###############################################################
    #################################################### NODES DATA TO AREA OF ANALYSIS ##################################################
    # Avoid overestimating universities
    nodes_analysis.loc[nodes_analysis.universidad_count_15min > 3, 'universidad_count_15min'] = 3

    area_dict = {'unidadesvecinales':'COD_UNICO_',
                 'zonascensales':'GEOCODI',
                 'hex':'hex_id'
                 }
    
    k = len(area_dict.keys())
    i = 1

    for area_analysis in area_dict.keys():

        aup.log(f"CALCULATING PROXIMITY AND HQSL FOR AREA OF ANALYSIS {i}/{k}: {area_analysis}.")

        aup.log(f"--- STARTING PART 2: NODES DATA TO {area_analysis}.")

        # 2.1 --------------- LOAD AND FORMAT AREA OF ANALYSIS GDF
        # ------------------- This step loads the current area of analysis and prepares it as an empty container

        code_column = area_dict[area_analysis]
        
        # Load area of analysis gdf
        if area_analysis == 'unidadesvecinales':
            gdf = gpd.read_file(areas_dir+"santiago_unidadesvecinales_zonaurbana.geojson")
            gdf = gdf[[code_column,'geometry']].copy()
            aup.log(f"--- Area of analysis {i}/{k} (2.1) - Loaded area of analysis gdf.")

        elif area_analysis == 'zonascensales':
            gdf = gpd.read_file(areas_dir+"zonas_censales_hogares_RM.shp")
            gdf = gdf[[code_column,'geometry']].copy()
            aup.log(f"--- Area of analysis {i}/{k} (2.1) - Loaded area of analysis gdf.")

        elif area_analysis == 'hex':
            # For this script, will only use res=10
            res = 10

            gdf = aup.create_hexgrid(aoi, res)
            gdf.rename(columns={f'hex_id_{res}':'hex_id'}, inplace=True)
            gdf['res'] = res
            gdf = gdf[[code_column,'res','geometry']].copy()  
            aup.log(f"--- Area of analysis {i}/{k} (2.1) - Created {len(gdf)} hexagons at resolution {res}.")
        
        # Set gdf CRS
        try:
            gdf = gdf.to_crs("EPSG:4326")
        except:
            gdf = gdf.set_crs("EPSG:4326")
        
        # Explode area of analysis gdf
        gdf = gdf.explode(ignore_index=True)

        # Clip area of analysis gdf to area of interest 
        # (Data available within area of interest only, not clipping causes problems when computing neighbors data.)
        gdf_cut = gpd.sjoin(gdf, aoi[['geometry']])
        gdf_cut.drop(columns=['index_right'],inplace=True)
        gdf = gdf_cut.copy()

        # 2.2 --------------- GROUP DATA BY AREA OF ANALYSIS
        # ------------------- This groups proximity data by area of analysis

        if area_analysis == 'hex':

            hex_gdf = gdf.copy()
            poly_proximity = gpd.GeoDataFrame()

            for r in hex_gdf.res.unique():

                # Calculate mean proximity within area of analysis
                hex_tmp = hex_gdf[hex_gdf.res == r].copy()
                hex_tmp = aup.group_by_hex_mean(nodes_analysis, hex_tmp, r, all_source_cols, 'hex_id')
                hex_tmp = hex_tmp.drop(columns=['res_x','res_y'])
                hex_tmp['res'] = r
                aup.log(f"--- Area of analysis {i}/{k} (2.2) - Calculated mean proximity for {len(hex_tmp)} hexagons at resolution {r}.")

                # Merge to poly_proximity gdf
                poly_proximity = pd.concat([poly_proximity, hex_tmp], 
                                           ignore_index = True, 
                                           axis = 0)
                aup.log(f"--- Area of analysis {i}/{k} (2.2) - Merged {len(hex_tmp)} hexagons to poly_proximity gdf.")

                del hex_tmp
        
        # If not hex
        else:
            r = 0 # no resolution needed for polygons different from h3 hexagons
            poly_proximity = aup.group_by_hex_mean(nodes_analysis, gdf, r, all_source_cols, code_column)
        aup.log(f"--- Area of analysis {i}/{k} (2.2) - Calculated mean proximity for {len(poly_proximity)} polygons.")


        # 2.3 --------------- FINAL FORMAT AND SAVE
        # ------------------- This step gives final formating to proximity data and saves it localy
        aup.log(f"--- Area of analysis {i}/{k} (2.3) - Giving final format and saving {area_analysis} proximity data.")

        poly_proximity = poly_proximity.set_geometry('geometry')
        try:
            poly_proximity = poly_proximity.to_crs("EPSG:4326")
        except:
            poly_proximity = poly_proximity.set_crs("EPSG:4326")
        
        poly_proximity['city'] = 'Santiago'

        if local_save:
            area_proximity_table = f"santiago_{area_analysis}proximity_{str_walk_speed}_kmh.gpkg"
            poly_proximity.to_file(local_save_dir + area_proximity_table, driver='GPKG')
            aup.log(f"--- Area of analysis {i}/{k} (2.3) - Saved {area_analysis} proximity data locally.")

    ########################################################## PART 3 ####################################################################
    ########################################################### HQSL #####################################################################

        aup.log(f"--- STARTING PART 3 (HQSL) FOR {area_analysis}.")

        prox_gdf = poly_proximity.copy()

        # 3.1 --------------- AREAL DATA
        # ------------------- This step loads areal data (Not processed through proximity analysis).
        aup.log(f"--- Area of analysis {i}/{k} (3.1) - Loading areal data.")

        if area_analysis == 'hex':
            poly_areal = gpd.read_file(areal_dir+f'{area_analysis}_areal_res{res}.gpkg')
        else:
            poly_areal = gpd.read_file(areal_dir+f'{area_analysis}_areal.gpkg')
            
        poly_areal = poly_areal.rename(columns={'oficinas_sum':'oficinas_count',
                                                'pct_social_viv':'social_viv_count',
                                                'viv_sum':'houses_count',
                                                'pct_hotel':'hotel_count',
                                                'ndvi_mean':'ndvi_count'})
        
        # Clip poly_aereal gdf to area of interest 
        # (Data available within area of interest only, not clipping causes problems when computing neighbors data.)
        poly_areal_cut = gpd.sjoin(poly_areal, aoi[['geometry']])
        poly_areal_cut.drop(columns=['index_right'],inplace=True)
        poly_areal = poly_areal_cut.copy()
        
        # 3.2 --------------- DATA TREATMENT
        # ------------------- This step prepares proximity data and merges it with areal data
        aup.log(f"--- Area of analysis {i}/{k} (3.2) - Joining _priv and _pub pois in {area_analysis}.")
        
        join_pois_list = ['hospital','clinica','consult_ado', 'museos','vacunatorio','eq_deportivo',]
        
        for source in join_pois_list:
            # join count columns for private and public in one encompassing column
            prox_gdf[f"{source}_count_15min"] = prox_gdf[f"{source}_priv_count_15min"] + prox_gdf[f"{source}_pub_count_15min"]
            # remove 0 values from time
            prox_gdf.loc[prox_gdf[f"{source}_pub_time"]==0] = np.nan
            prox_gdf.loc[prox_gdf[f"{source}_priv_time"]==0] = np.nan
            # assign general minimum time
            prox_gdf[f"{source}_time"] = prox_gdf[[f"{source}_pub_time", f"{source}_priv_time"]].min(axis=1)
            # remove duplicate info columns
            prox_gdf = prox_gdf.drop(columns=[f"{source}_pub_count_15min", f"{source}_priv_count_15min",
                                              f"{source}_pub_time", f"{source}_priv_time"])
            # fill na with 0 for future processing
            prox_gdf['hospital_time'].fillna(0, inplace=True)

        # Merge areal and proximity data
        poly_analysis = poly_areal.merge(prox_gdf.drop(columns='geometry'), on=code_column, how='left')
        poly_analysis = poly_analysis.explode(ignore_index=True)
        poly_analysis = poly_analysis.dissolve(by=code_column)
        poly_analysis = poly_analysis.reset_index()

        # 3.3 --------------- HQSL Function - Variables analysis
        # ------------------- This step scales data
        aup.log(f"--- Area of analysis {i}/{k} (3.3) - Processing variables analysis.")
        # ------------------------------
        # use scale functions for each column
        for j in tqdm(range(len(weight_dict.keys())),position=0,leave=True):
            # gather specific source
            source = list(weight_dict.keys())[j]
            # iterate over columns
            for col_name in poly_analysis.columns:
                # select column with count information -- refers to the amount of opportunities available at 15 min
                if source in col_name and 'count' in col_name:
                    if f'{source}_time' in poly_analysis.columns:
                        poly_analysis[f'{source}_time'].fillna(0, inplace=True)
                    poly_analysis[col_name].fillna(0, inplace=True)

                    # source scaling
                    poly_analysis[f'{source}_scaled'] = poly_analysis[col_name].apply(lambda x:scale_source_fn(x,
                                                                                                               source,
                                                                                                               weight_dict,
                                                                                                               area_analysis,
                                                                                                               poly_analysis[col_name].mean(),
                                                                                                               poly_analysis[col_name].std()))
                    # treat 0 time values -- hexagons without nodes 
                    if area_analysis == 'hex':
                        if weight_dict[source] != 'specific':
                            # assign nan values to hexagons without nodes to avoid affecting the mean calculation process
                            #if source in join_pois_list:
                            #    hex_analysis.loc[hex_analysis.supermercado_time==0,f'{source}_scaled'] = np.nan
                            if source == 'hotel' or source == 'oficinas':
                                continue
                            else:
                                poly_analysis.loc[poly_analysis[f'{source}_time']==0,f'{source}_scaled'] = np.nan
                                
                            # calculate mean count value
                            poly_analysis.loc[poly_analysis[f'{source}_time']==0, f'{source}_scaled'] = poly_analysis.loc[poly_analysis[f'{source}_time']==0].apply(lambda x: neighbour_mean(x['hex_id'],
                                                                                                                                                                                             'hex_id',
                                                                                                                                                                                             poly_analysis,
                                                                                                                                                                                             f'{source}_scaled'), axis=1)
        # 3.4 --------------- HQSL Function - HQSL Index calculation
        # ------------------- This step calculates HQSL
        aup.log(f"--- Area of analysis {i}/{k} (3.4) - Calculating HQSL.")

        # ------------------------------
        hex_ind = indicator_fn(poly_analysis, parameters_dict, code_column)
        hex_social_fn = social_fn(poly_analysis, parameters_dict, code_column)
        hex_hqsl = hqsl_fn(hex_social_fn, parameters_dict, code_column)
        
        hex_idx = hex_ind.merge(hex_social_fn.drop(columns='geometry'), on=code_column)
        hex_idx = hex_idx.merge(hex_hqsl.drop(columns='geometry'), on=code_column)

        # 3.5 --------------- SAVING
        # ------------------- This step saves HQSL result.
        if area_analysis == 'hex':
            hex_idx['res'] = res
        
        hex_idx = hex_idx.dropna()
                                
        if local_save:
            aup.log(f"--- Area of analysis {i}/{k} (3.5) - Saving HQSL index locally.")
            hex_idx.to_file(gral_dir +'output/'+ f'santiago_{area_analysis}analysis_{str_walk_speed}_kmh.gpkg', driver='GPKG')
        
        i+=1

if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('--- STARTING SCRIPT 27.')

    #################################################### DATA FOR PART 1 and 2 ###########################################################
    ############################################## NAVIGABLE NETWORK AND PROXIMITY DATA ##################################################

    # ------------------------------ BASE DATA REQUIRED ------------------------------
    
    # --------------- LIST OF POIS TO BE EXAMINED
    # This list should contain the source_name that will be assigned to each processed poi.
    # That source_name will be stored in a 'source' column at first and be turned into a column name after all pois are processed.
    # That source_name must also be the name of the file stored in pois_dir (.gpkg)
    # e.g if source_list = ['vacunatorio_pub'], vacanatorio_pub.gpkg must exist.

    source_list = ['carniceria','hogar','bakeries','supermercado','banco', #supplying-wellbeing
                   #supplying-sociability
                   'ferias','local_mini_market','correos', 
                   #supplying-environmental impact
                   'centro_recyc',

                   #caring-wellbeing
                   'hospital_priv','hospital_pub','clinica_priv','clinica_pub','farmacia','vacunatorio_priv','vacunatorio_pub','consult_ado_priv','consult_ado_pub','salud_mental','labs_priv','residencia_adumayor',
                   #caring-sociability
                   'eq_deportivo_priv','eq_deportivo_pub','club_deportivo',
                   #caring-environmental impact [areal data: 'noise','temp']

                   #living-wellbeing
                   'civic_office','tax_collection','social_security','police','bomberos',
                   #living-sociability [areal data: 'houses','social_viv','hotel']
                   #living-environmental impact [areal_data: 'inter']
                   
                   #enjoying-wellbeing [areal data: 'ndvi']
                   'museos_priv','museos_pub','cines','sitios_historicos',
                   #enjoying-sociability
                   'restaurantes_bar_cafe','librerias','ep_plaza_small',
                   #enjoying-environmental impact
                   'ep_plaza_big',

                   #learning-wellbeing
                   'edu_basica_pub','edu_media_pub','jardin_inf_pub','universidad', 'edu_tecnica',
                   #learning-sociability
                   'edu_adultos_pub','edu_especial_pub','bibliotecas',
                   #learning-environmental impact
                   'centro_edu_amb',

                   #working-wellbeing
                   'paradas_tp_ruta','paradas_tp_metro','paradas_tp_tren',
                   #working-sociability [areal data: 'oficinas']
                   #working-environmental impact
                   'ciclovias','estaciones_bicicletas']
    
    # --------------- UNIQUE ID POIS (Special proximity cases)
    # From source_list, sources that have an unique ID and require special processing (id_pois_time() function instead of pois_time() function)
    # Unique ID for each of them is 'ID'.
    unique_id_sources = ['ferias','ep_plaza_small','ep_plaza_big','ciclovias']

    # --------------- LOCAL INPUT AND OUTPUT DIRECTORIES
    # IMPORTANT NOTE: Make sure all directories exist.
    # general directory (All directories derive from here)
    gral_dir = '../data/external/santiago/'
    
    # Dir 1 - If not using OSMnx network (INPUT NETWORK), will use this file to create a filtered network.
    #public_space_quality_dir = gral_dir + "calidad_ep/red_buena_calidad.shp"
    #NOT WORKING. Use QGIS files and edit before main function.
    
    # "calidad_ep/redvial2019_buffer_3750m_c_utilidad_2.shp"
    # "calidad_ep/red_buena_calidad.shp"
    # "calidad_ep/red_buena_calidad_pza_italia.shp"

    # Dir 2 - Local directory where pois files are located
    all_pois_dir = gral_dir + "pois/"

    # Dir 3 - Local directory where areas of analysis are located (Used in PART 2 and PART 3)
    areas_dir = gral_dir + "areas_of_analysis/"

    # Dir 4 - Local directory where outputs are saved
    local_save_dir = gral_dir + "output/"

    # Dir 5 - Local directory where areal data is located
    areal_dir = gral_dir + 'areal_data/'
    
    # --------------- AREA OF INTEREST
    # Area of interest (aoi)
    aoi_schema = 'projects_research'
    aoi_table = 'santiago_aoi'
    # 'AM_Santiago' represents Santiago's metropolitan area, 'alamedabuffer_4500m' also available
    city = 'alamedabuffer_4500m'

    # --------------- PROJECTION
    # Projection to be used whenever necessary
    projected_crs = 'EPSG:32719'

    # --------------- METHODOLOGY
    # Pois proximity methodology - Count pois at a given time proximity? (If true, second tupple value is distance in minutes)
    count_pois = (True,15)

    # walking_speed (float): Decimal number containing walking speed (in km/hr) to be used if prox_measure="length",
	#						 or if prox_measure="time_min" but needing to fill time_min NaNs.
    walking_speed_list = [4.5] #[3.5,4.5,5,12,24,20,40]
    
    # --------------- INPUT NETWORK
    # If using previously downloaded OSMnx network available in database, set following to true
    osmnx_network = False
    # If true, set schemas and tables
    network_schema = 'projects_research'
    edges_table = 'santiago_edges'
    nodes_table = 'santiago_nodes'
    # Else (osmnx_network = False), set external network data (Allows for filtering network according to a given column value)
    # IMPORTANT NOTE: Make sure public_space_quality_dir file exists.
    filtering_column = 'pje_ep'
    filtering_value = 0.5 # Will keep equal or more than this value

    # --------------- SAVING SPACE IN DISK
    # Save space in disk by deleting data that won't be used again?
    save_space = True

    # --------------- SAVING DATA
    # IMPORTANT NOTE: Make sure local_save_dir exists
    local_save = True # save output to local?

    ###################################################### DATA FOR PART 3 ###############################################################
    ########################################################### HQSL #####################################################################

    # --------------- PARAMETERS AND WEIGHT DICTS
    # Structure: {social_functions:{themes:[source_names]}}
    parameters_dict = {'supplying':{'wellbeing':['carniceria', #Accessibility to Butcher/Fish Shops
                                                'hogar', #Accessibility to Hardware/Paint Shops
                                                #Not available: Accessibility to Greengrocers
                                                'bakeries', #Accessibility to Bakeries and delis
                                                'supermercado',#Accessibility to supermarkets
                                                'banco'#Accessibility to bank
                                                ],
                                    'sociability':['ferias',#Accessibility to city fairs/markets
                                                'local_mini_market',#Accessibility to local and mini markets
                                                'correos'#ADDED: MAIL SERVICE
                                                ],
                                    'environmental_impact':['centro_recyc'#Accessibility to recycling center
                                                            #Not available: Accessibility to compost
                                                        ]
                                },
                    'caring':{'wellbeing':['hospital', #Accessibility to hospital
                                            'clinica',#Accessibility to public clinics
                                            'farmacia',#Accessibility to pharmacies
                                            'vacunatorio',#Accessibility to vaccination center
                                            'consult_ado',#Accessibility to optician/audiologist(###ADDED DENTIST)
                                            'salud_mental',###ADDED: MENTAL HEALTH
                                            'labs_priv',###ADDED: LABORATORIES
                                            'residencia_adumayor'###ADDED: ELDERLY PERMANENT RESIDENCIES
                                            ],
                                'sociability':['eq_deportivo',#Accessibility to sports equipments
                                                'club_deportivo'#Accessibility to sport clubs
                                            ],
                                'environmental_impact':['noise',
                                                        'temp'
                                    #Not available: Air polution
                                                        ]
                                },
                    'living':{'wellbeing':['civic_office',#Accessibility to civic offices
                                            #Not available: Number of street bentches
                                            'tax_collection',#ADDED: AFIP(TAX COLLECTOR)
                                            'social_security',#ADDED: SOCIAL SECURITY
                                            'police',#Accessibility to police(###MOVED FROM LIVING TO CARING)
                                            'bomberos'#Accessibility to fire stations
                                            #Not available: Accessibility to street lamp
                                            ],
                                'sociability':['houses',#Accessibility to permanent residencies
                                                'social_viv',#Accessibility to social housing
                                                #Not available: Accessibility to student housing
                                                'hotel'#ADDED: HOTELS
                                            ],
                                'environmental_impact':['inter',
                                                        #Not available: Corrected compactness
                                                        #Not available: Width of sidewalks
                                                        ],
                                },
                    'enjoying':{'wellbeing':['museos',#Accessibility to museums
                                                #Not available: Accessibility to theater,operas
                                                'cines',#Accessibility to cinemas
                                                'sitios_historicos',#Accessibility to historical places
                                                'ndvi'#Number of trees
                                            ],
                                'sociability':['restaurantes_bar_cafe',#Accessibility to bars/cafes + Accessibility to restaurants
                                                'librerias',#Accessibility to record and book stores, galleries, fairs
                                                #Not available: Accessibility to cultural and/or formative spaces
                                                #Not available: Accessibility to places of workship
                                                'ep_plaza_small'#Accessibility to boulevards, linear parks, small squares + Accessibility to squares
                                                ],
                                'environmental_impact':['ep_plaza_big'#Accessibility to big parks
                                                        #Not available: Accessibility to shared gardens
                                                        #Not available: Accessibility to urban playgrounds
                                                        ]
                                },
                    'learning':{'wellbeing':['edu_basica_pub',#'edu_basica_priv',#Accessibility to public elementary school
                                                'edu_media_pub',#'edu_media_priv',#Accessibility to public high school
                                                'jardin_inf_pub',#'jardin_inf_priv',#Similar to Accessibility to childcare
                                                'universidad',#Accessibility to university
                                                'edu_tecnica',#ADDED: TECHNICAL EDUCATION
                                            ],
                                'sociability':['edu_adultos_pub',#'edu_adultos_priv',#Accessibility to adult formation centers
                                                'edu_especial_pub',#'edu_especial_priv',#Accessibility to specialized educational centers
                                                #Not available: Accesibility to establishments and services for disabled adults
                                                'bibliotecas'#Accessibility to libraries(###MOVED FROM ENJOYING TO LEARNING)
                                                ],
                                'environmental_impact':['centro_edu_amb'#Accessibility to centers for learning environmental activities
                                                        #Not available: Accessibility to gardening schools
                                                        ],
                                },
                    'working':{'wellbeing':['paradas_tp_ruta',#Accessibility to bus stop
                                            'paradas_tp_metro',#Accessibility to metro
                                            'paradas_tp_tren'#Accessibility to train stop
                                            ],
                                'sociability':['oficinas'#Accessibility to office
                                                #Not available: Accessibility to incubators
                                                #Not available: AccSeveral other articles cite 60dB as a safe noise zone. essibility to coworking places
                                            ],
                                'environmental_impact':['ciclovias',
                                                        'estaciones_bicicletas'#Accessibility to bike lanes
                                                        #Not available: Accessibility to shared bike stations
                                                        ]
                                }
                    }

    weight_dict = {'carniceria':'rare', #SUPPLYING
                'hogar':'rare',
                'bakeries':'rare',
                'supermercado':'rare',
                'banco':'rare',
                'ferias':'rare',
                'local_mini_market':'rare',
                'correos':'very_rare',
                'centro_recyc':'rare',
                #CARING
                'hospital':'very_rare',
                'clinica':'rare',
                'farmacia':'rare',
                'vacunatorio':'very_rare',
                'consult_ado':'very_rare',
                'salud_mental':'very_rare',
                'labs_priv':'very_rare',
                'residencia_adumayor':'rare',
                'eq_deportivo':'rare',
                'club_deportivo':'rare',
                'noise':'specific',
                'temp':'specific',
                #LIVING
                'civic_office':'rare', 
                'tax_collection':'very_rare',
                'social_security':'very_rare',
                'police':'very_rare',
                'bomberos':'very_rare',
                'houses':'specific',
                'social_viv':'specific',
                'hotel':'rare',
                'inter':'specific',
                #ENJOYING
                'museos':'very_rare',
                'cines':'very_rare',
                'sitios_historicos':'rare',
                'ndvi':'specific',
                'restaurantes_bar_cafe':'frequent',
                'librerias':'rare',
                'ep_plaza_small':'frequent',
                'ep_plaza_big':'rare',
                #LEARNING
                'edu_basica_pub':'rare', 
                'edu_media_pub':'rare',
                'jardin_inf_pub':'rare',
                'universidad':'very_rare',
                'edu_tecnica':'very_rare',
                'edu_adultos_pub':'rare',
                'edu_especial_pub':'rare',
                'bibliotecas':'very_rare',
                'centro_edu_amb':'very_rare',
                #WORKING
                'paradas_tp_ruta':'frequent',
                'paradas_tp_metro':'very_rare',
                'paradas_tp_tren':'very_rare',
                'oficinas':'specific',
                'ciclovias':'rare',
                'estaciones_bicicletas':'rare',
                }
    
    ######################################################################################################################################
    ########################################################## SCRIPT START ##############################################################
    ######################################################################################################################################

    # Area of interest (aoi)
    aup.log("--- Downloading area of interest.")
    query = f"SELECT * FROM {aoi_schema}.{aoi_table} WHERE \"city\" LIKE \'{city}\'"
    aoi = aup.gdf_from_query(query, geometry_col='geometry')
    aoi = aoi.set_crs("EPSG:4326")

    # Network
    if osmnx_network:
        aup.log("--- Downloading OSMnx network.")
        G, nodes, edges = aup.graph_from_hippo(aoi, network_schema, edges_table, nodes_table, projected_crs)
        # Temporal edit in pois_time() and id_pois_time() functions allows for using external nearest file
        santiago_tmp_fix = False
    else:
        aup.log("--- Converting local data to OSMnx format network.")
        #G, nodes, edges = create_filtered_navigable_network(public_space_quality_dir, projected_crs, filtering_column, filtering_value)

        ################################## FUNCTION NOT WORKING, TEMPORAL QGIS FIX
        # Filtered network - Load edges
        edges_file = gpd.read_file(gral_dir+'calidad_ep/red_buena_calidad_single_parts.gpkg')
        edges_file = edges_file.set_crs(projected_crs)
        # Filtered network - Load nodes
        nodes_file = gpd.read_file(gral_dir +'calidad_ep/red_buena_calidad_nodes.shp')
        nodes_file = nodes_file.set_crs("EPSG:32719")
        # Filtered network - Create navigable network
        nodes, edges = aup.create_network(nodes_file, edges_file,"EPSG:32719")
        nodes = nodes.drop_duplicates(subset=['osmid'])
        # Filtered network - Filter navigable network
        edges_filt = edges.loc[edges[filtering_column] >= filtering_value]
        # Filtered network - Prepare nodes
        nodes_gdf = nodes.copy()
        nodes_gdf.set_index('osmid',inplace=True)
        nodes_gdf['x'] = nodes_gdf['geometry'].x
        nodes_gdf['y'] = nodes_gdf['geometry'].y
        # Filtered network -  Prepare edges
        edges_gdf = edges_filt.copy()
        edges_gdf.set_index(['u','v','key'],inplace=True)
        # Filtered network - Create G and rename nodes and edges
        G = ox.graph_from_gdfs(nodes_gdf, edges_gdf)
        nodes = nodes_gdf.copy()
        edges = edges_gdf.copy()
        ################################## FUNCTION NOT WORKING, TEMPORAL QGIS FIX

        # Temporal edit in pois_time() and id_pois_time() functions allows for using external nearest file
        santiago_tmp_fix = True

    # Main function
    for walking_speed in walking_speed_list:
        aup.log('--'*45)
        str_walk_speed = str(walking_speed).replace('.','_')
            
        # Proceed to main function
        aup.log(f"--- Running Script for speed: {walking_speed}km/hr.")
        main(source_list, aoi, G, nodes, edges, walking_speed, local_save,santiago_tmp_fix)