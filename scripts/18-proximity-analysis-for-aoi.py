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


def download_osmnx(aoi, save_space = True):
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
    
    # Saves space in disk if instructed
    if save_space:
        del nodes_columns
        del edges_columns
        del c

        aup.log("Saved space by deleting used data.")
    
    return G,nodes,edges


def create_popdata_hexgrid(aoi,pop_dir,pop_column,pop_index_column,res_list):
    
    pop_gdf = gpd.read_file(pop_dir)
    
    # Format and isolate data of interest
    pop_gdf = pop_gdf.to_crs("EPSG:4326")
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

    aup.log(f"Converted to centroids with {centroid_block_pop.pobtot.sum()} " + f"pop vs {block_pop.pobtot.sum()} pop in original gdf.")
    
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

    #-----------------------------------------------------------------------------------------------------------------------------------------------------
    # STEP 1: Create osmnx network (Based on Script 07-download_osmnx)
    #-----------------------------------------------------------------------------------------------------------------------------------------------------

    # Read area of interest (aoi)
    aoi = gpd.read_file(aoi_dir)
    aoi = aoi.to_crs("EPSG:4326")

    aup.log(f"Starting creation of osmnx network.")

    # Download osmnx network (G, nodes and edges from bounding box of aoi)
    G, nodes, edges = download_osmnx(aoi, save_space = save_space)

    aup.log(f"Finished creating osmnx network.")

    #-----------------------------------------------------------------------------------------------------------------------------------------------------
    # STEP 2: Calculate distance from pois to nearest osmnx node in tidy data (Based on Script 01_denue_to_nodes)
    #-----------------------------------------------------------------------------------------------------------------------------------------------------
    
    # Read points of interest (pois)
    pois = gpd.read_file(pois_dir)
    pois = pois.set_crs("EPSG:4326")

    # Filter pois for aoi
    pois_aoi = gpd.sjoin(pois,aoi,how='inner')
    pois = pois_aoi[['code','geometry']]

    # Prepare to calculate nearest
    nodes_gdf = nodes.set_crs("EPSG:4326")
    edges_gdf = edges.set_crs("EPSG:4326")
    nodes_gdf = nodes_gdf.set_index('osmid')
    edges_gdf = edges_gdf.set_index(["u", "v", "key"])

    # Calculate nearest
    nearest = aup.find_nearest(G, nodes_gdf, pois, return_distance= True)

    aup.log("Calculated distances from pois to nearest node.")

    #-----------------------------------------------------------------------------------------------------------------------------------------------------
    # STEP 3: Calculate distance from each node to nearest source amenity (Based on Script 02_distance_amenities)
    #-----------------------------------------------------------------------------------------------------------------------------------------------------
    
    # Create sources - code dictionary out of main parameters dictionary (previously, dictionary "amenities")
    sources = {}
    for eje in parameters.keys():
        for amenity in parameters[eje]:
            for source in parameters[eje][amenity]:
                sources[source] = parameters[eje][amenity][source]

    # Format
    pois_distance_node = nearest.copy()
    edges_gdf['length'].fillna(edges_gdf['length'].mean(),inplace=True)
    
    # ELEMENTS NEEDED OUTSIDE LOOP
    # nodes_analysis is a nodes_gdf out of loop used in aup.calculate_distance_nearest_poi in each loop
    nodes_analysis = nodes_gdf.reset_index().copy()
    # nodes_sources is a nodes_gdf out of loop used to accumulate a final gdf with the minimal distance from each node to each source
    nodes_sources = gpd.GeoDataFrame()
    # count
    i = 0

    # Loop that calculates distance from each node to each source (iterating by source)
    for s in sources:
        # Locate pois data for current source
        source_gdf = gpd.GeoDataFrame()
        for cod in sources[s]:
            source_tmp = pois_distance_node[pois_distance_node['code']==cod]
            source_gdf = pd.concat([source_gdf,source_tmp])

        aup.log(f"Loaded a total of {len(source_gdf)} points of interest of the source {s} for analisis.")
        
        # Data for current source amenity loop - df_temp: Processing will be divided by batches. Each column will contain a batch of procesed nodes.
        df_temp = nodes_gdf.copy()
        # Data for current source amenity loop - nodes_distance: Minimum time/distance found in all batches will be added from df_min to nodes_distance and finally to nodes_sources (outside loop)
        nodes_distance = nodes_gdf.copy()
        
        # In case there are no amenities of a certain type in the city
        if len(source_gdf) == 0:
            nodes_time = nodes_distance.copy()
            nodes_time['time'] = 0

            aup.log(f"0 points of interest of the source {s} found, time of column set to 0.")      
        
        # Elif, divide in batches processing (200 if the total number of pois is an exact multiple of 250, 250 otherwise)
        elif len(source_gdf) % 250:
            batch_size = len(source_gdf)/200
            for k in range(int(batch_size)+1):

                aup.log(f"Starting range k = {k} of {int(batch_size)} for source {s}.")

                source_process = source_gdf.iloc[int(200*k):int(200*(1+k))].copy()
                nodes_distance_prep = aup.calculate_distance_nearest_poi(source_process, nodes_analysis, edges_gdf, s, 'osmid', wght='length')
                
                #A middle gdf is created whose columns will be the name of the amenity and the batch number it belongs to
                df_int = pd.DataFrame()
                df_int['dist_'+str(k)+s] = nodes_distance_prep['dist_'+s]
                
                #The middle gdf is merged into the previously created temporary gdf to store the data by node, each batch in a column.
                df_temp = df_temp.merge(df_int, left_index=True, right_index=True)
                
            # Once finished, drop the non-distance values from the temporary gdf
            df_temp.drop(['x', 'y', 'street_count','geometry'], inplace = True, axis=1)
            
            # Apply the min function to find the minimum value. This value is sent to a new df_min
            df_min = pd.DataFrame()
            df_min['dist_'+s] = df_temp.min(axis=1)
            
            # Merge df_min which contains the shortest distance to the POI with nodes_distance which will store all final data
            nodes_distance = nodes_distance.merge(df_min, left_index=True, right_index=True)
            
            # Final data gets converted to time, assuming a walking speed of 4km/hr
            nodes_time = nodes_distance.copy()
            nodes_time['time'] = (nodes_time['dist_'+s]*60)/4000
            
            aup.log(f"Calculated time from nodes to pois for a total of {len(nodes_distance)} nodes for source {s}.")

        # Else, divide in batches processing (200 if the total number of pois is an exact multiple of 250, 250 otherwise)   
        else:
            batch_size = len(source_gdf)/250
            for k in range(int(batch_size)+1):

                print(f"Starting range k = {k} of {int(batch_size)} for source {s}.")

                source_process = source_gdf.iloc[int(250*k):int(250*(1+k))].copy()
                nodes_distance_prep = aup.calculate_distance_nearest_poi(source_process, nodes_analysis, edges_gdf, s, 'osmid', wght='length')
                
                #A middle gdf is created whose columns will be the name of the amenity and the batch number it belongs to
                df_int = pd.DataFrame()
                df_int['dist_'+str(k)+s] = nodes_distance_prep['dist_'+s]
                
                #The middle gdf is merged into the previously created temporary gdf to store the data by node, each batch in a column.
                df_temp = df_temp.merge(df_int, left_index=True, right_index=True)
                
            # Once finished, drop the non-distance values from the temporary gdf
            df_temp.drop(['x', 'y', 'street_count','geometry'], inplace = True, axis=1)
            
            # Apply the min function to find the minimum value. This value is sent to a new df_min
            df_min = pd.DataFrame()
            df_min['dist_'+s] = df_temp.min(axis=1)
            
            # Merge df_min which contains the shortest distance to the POI with nodes_distance which will store all final data
            nodes_distance = nodes_distance.merge(df_min, left_index=True, right_index=True)
            
            # Final data gets converted to time, assuming a walking speed of 4km/hr
            nodes_time = nodes_distance.copy()
            nodes_time['time'] = (nodes_time['dist_'+s]*60)/4000
        
            aup.log(f"Calculated time from nodes to pois for a total of {len(nodes_distance)} nodes for source {s}.")    
        
        aup.log("Applying final formating to calculated nodes.")
        
        #Format nodes_distance
        nodes_time['source'] = s
        nodes_time['city'] = city
        nodes_time.reset_index(inplace=True)
        nodes_time = nodes_time.set_crs("EPSG:4326")
        nodes_time = nodes_time[['osmid','time','source','city','x','y','geometry']]
        
        #If it is the first round nodes_sources is created equal to nodes_distance (all nodes, one source)
        #If it is the second or more, the new nodes_distance is merged.
        #This way we obtain the final gdf of interest that will contain the minimum distance to each type of amenity by column.
        if i == 0:
            nodes_sources = nodes_time.copy()
        else:
            nodes_sources = pd.concat([nodes_sources,nodes_time])
            
        aup.log(f"Added time for source {s} to nodes_sources.")     
        
        i += 1

    #-----------------------------------------------------------------------------------------------------------------------------------------------------
    # STEP 4: Group and analyze (from distance data in nodes to proximity in hexagons) (Based on Script 15-15-min-cities)
    #-----------------------------------------------------------------------------------------------------------------------------------------------------
    
    # Prepare data ------------------------------------------------------------------------
    # Prepare data 1/4 - Clean nodes and delete duplicates
    # This step keeps osmid, geometry and metropolis (without duplicates, keeping only one point for each node) to store times to each amenity source by 
    # node in following loop.
    nodes = nodes_sources.copy()
    nodes_geom = nodes.drop_duplicates(subset='osmid', keep="last")[['osmid','geometry','city']].copy()
    nodes_analysis = nodes_geom.copy()

    # Prepare data 2/4 - Reorganize nodes data
    # This step organizes data by nodes by changing (time to source amenities) from rows (1 column with source amenity name + 1 column with time data) 
    # to columns (1 column with time data named after its source amenity)
    for source_amenity in list(nodes.source.unique()):
        nodes_tmp = nodes.loc[nodes.source == source_amenity,['osmid','time']]
        nodes_tmp = nodes_tmp.rename(columns={'time':source_amenity})
        # Search for amenities that aren't present in the city (with all values marked as 0) and change them to NaN
        if nodes_tmp[source_amenity].mean() == 0:
            nodes_tmp[source_amenity] = np.nan
        nodes_analysis = nodes_analysis.merge(nodes_tmp, on='osmid')

    if save_space:
        del nodes_sources
        del nodes_geom
        del nodes_tmp

    aup.log("Transformed nodes data.")

    # Prepare data 3/4 - Create definitions dictionary out of main parameters dictionary (previously, idx_15_min dictionary)
    # This step creates a dictionary containing eje-amenity-sources for the analysis
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
        
        # Each eje gets assigned its own tmp_dicc containing amenities and a [source_list] (Without codes)
        definitions[eje] = tmp_dicc

    # Prepare data 4/4 - Fill for missing amenities
    # This step fills missing columns in case there is a source amenity not available in a city
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
                missing_sourceamenities.append(s)
            
                aup.log(f"{s} source amenity is not present in {city}.")

    aup.log(f"Finished missing source amenities analysis. {len(missing_sourceamenities)} not present source amenities were added as np.nan columns")

    # Process data ------------------------------------------------------------------------

    aup.log("Starting proximity to amenities analysis by node.")

    # Max time calculation
    # This step calculates times by amenity

    column_max_all = [] # list with all max index column names
    column_max_ejes = [] # list with ejes index column names

    # Goes through each eje in dictionary:
    for e in definitions.keys():
        # Appends to 3 lists currently examined eje
        column_max_all.append('max_'+ e.lower())
        column_max_ejes.append('max_'+ e.lower())
        column_max_amenities = [] # list with amenities in current eje

        # Goes through each amenity of current eje:
        for a in definitions[e].keys():
            # Appends to 2 lists currently examined amenity:
            column_max_all.append('max_'+ a.lower())
            column_max_amenities.append('max_'+ a.lower())
            # Calculates time to currently examined amenity according to source_weight dictionary
            if source_weight[e][a] == 'min': 
                nodes_analysis['max_'+ a.lower()] = nodes_analysis[definitions[e][a]].min(axis=1)
            #Else, choose maximum time to sources.
            else:
                nodes_analysis['max_'+ a.lower()] = nodes_analysis[definitions[e][a]].max(axis=1)

        # Calculates time to currently examined eje (max time of its amenities):
        nodes_analysis['max_'+ e.lower()] = nodes_analysis[column_max_amenities].max(axis=1) 

    index_column = 'max_time' # column name for maximum time data
    # Add to column_max_all list the attribute 'max_time'
    column_max_all.append(index_column)
    # Assigns "max_time" the max time for all ejes
    nodes_analysis[index_column] = nodes_analysis[column_max_ejes].max(axis=1)     
    # Add to column_max_all list the attributes 'osmid' and 'geometry' to filter nodes_analysis with the column_max_all list.
    column_max_all.append('osmid')
    column_max_all.append('geometry')
    nodes_analysis_filter = nodes_analysis[column_max_all].copy()

    if save_space:
        del nodes_analysis
        
    aup.log("Calculated proximity to amenities data by node.")

    # Group data by hex ------------------------------------------------------------------------   

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
            del hex_res_idx
            del hex_res_pop #pop_output=True

        else:
            del hexgrid #pop_output=False
            del hex_tmp
            del hex_res_idx

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

    aup.log('Finished final format')

    #-----------------------------------------------------------------------------------------------------------------------------------------------------
    # STEP 5: Saving
    #-----------------------------------------------------------------------------------------------------------------------------------------------------

    if save:
        aup.df_to_db_slow(hex_idx_city, 'proximityanalysis','prox_analysis', if_exists='append')

if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')

    # REQUIRED DATA
    city = 'Aguascalientes'
    res_list = [8,9]

    # Required directories
    aoi_dir = "../../data/external/prox_latam/aoi_ags.gpkg"
    pois_dir = "../../data/external/prox_latam/pois_ags.gpkg"
    # Pop data file directory (required if pop_output = True)
    pop_dir = "../../data/external/prox_latam/pop_gdf_ags.gpkg"
    # Column with pop data (required if pop_output = True)
    pop_column = 'pobtot'
    # Pop gdf index column (required if pop_output = True)
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

    main(save=True,save_space=True)
