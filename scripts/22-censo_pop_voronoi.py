import geopandas as gpd
import pandas as pd
import osmnx as ox
import numpy as np

import shapely

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup

def main(city,save=False):

	##########################################################################################
	# STEP 1: LOAD DATA
    
    # --------------- 1.1 CREATE AREA OF INTEREST FOR CITY
    city_gdf = metro_gdf.loc[metro_gdf.city == city]
    city_gdf = city_gdf.set_crs("EPSG:4326")
    aoi = city_gdf.dissolve()
    
    # --------------- 1.2 LOAD POP DATA    
    print("Loading AGEBs for area of interest.")
    pop_ageb_gdf = aup.gdf_from_polygon(aoi,'censoageb',f'censoageb_{year}')
    pop_ageb_gdf = pop_ageb_gdf.set_crs("EPSG:4326")
    
    print("Loading blocks for area of interest.")
    pop_mza_gdf = aup.gdf_from_polygon(aoi,'censo_mza',f'censo_mza_{year}')
    pop_mza_gdf = pop_mza_gdf.set_crs("EPSG:4326")
    pop_mza_gdf = pop_mza_gdf.loc[pop_mza_gdf.AMBITO == 'Urbana'].copy()
    
    ##########################################################################################
	# STEP 2: CALCULATE NaN VALUES for pop fields (most of them, check function) of gdf containing blocks.
    
    print("--"*30)
    print("CALCULATING NAN VALUES FOR POP FIELDS.")
    
    # --------------- 2.1 CALCULATE_CENSO_NAN_VALUES Function
    pop_mza_gdf_calc = aup.calculate_censo_nan_values_v1(pop_ageb_gdf,pop_mza_gdf,extended_logs=False)

    ##########################################################################################
	# STEP 3: DISTRIBUTE POP BLOCK DATA TO NODES USING VORONOI

    print("--"*30)
    print("DISTRIBUTING POP DATA FROM BLOCKS TO NODES")

    # --------------- 3.0 LOAD NODES
    print("Loading nodes for area of interest.")
    
    if year == '2010':
        _, nodes, _ = aup.graph_from_hippo(aoi, schema='networks', edges_folder='edges_2011', nodes_folder='nodes_2011')
        # FOR NETWORK 2011 ONLY: Drop unncessary columns from nodes column (only present in 20111)
        nodes.drop(['ID', 'TIPOVIA', 'TIPO', 
                    'NUMERO', 'DERE_TRAN', 'ADMINISTRA', 'NUME_CARR', 'CONDICION', 
                    'ORIGEN', 'CALI_REPR', 'CVEGEO', 'NOMVIAL', 'SENTIDO', 'LONGITUD', 'UNIDAD', 
                    'vertex_pos', 'vertex_ind', 'vertex_par', 'vertex_p_1', 
                    'distance', 'angle'], inplace = True, axis=1)
        
    elif year == '2020':
        _, nodes, _ = aup.graph_from_hippo(aoi, schema='osmnx', edges_folder='edges_23_line', nodes_folder='nodes_23_point')
    
    nodes.reset_index(inplace=True)
    nodes = nodes.to_crs("EPSG:4326")

    # --------------- 3.1 CREATE VORONOI POLYGONS USING NODES

    print("Creating voronois with nodes osmid data.")

    # Create voronois
    voronois_gdf = aup.voronoi_points_within_aoi(aoi,nodes,'osmid')
    nodes_voronoi_gdf = voronois_gdf[['osmid','geometry']]

    # --------------- 3.2 SPATIAL INTERSECTION OF POLYGONS WITH BLOCKS

    print("Creating spatial join between voronoi polygons and blocks.")
    
    # Calculate block area
    mza_gdf = pop_mza_gdf_calc.to_crs("EPSG:6372")
    mza_gdf['area_mza'] = mza_gdf.geometry.area
    mza_gdf = mza_gdf.to_crs("EPSG:4326")
    
    # Overlay blocks with voronoi (Spatial intersection)
    mza_voronoi = gpd.overlay(df1=mza_gdf, df2=nodes_voronoi_gdf, how="intersection")
    del mza_gdf

    print("Calculating area_pct that corresponds to each osmid within each block.")

    # Calculate pct of area that corresponds to each osmid within each block
    mza_voronoi = mza_voronoi.to_crs("EPSG:6372")
    mza_voronoi['area_voronoi'] = mza_voronoi.geometry.area
    mza_voronoi = mza_voronoi.to_crs("EPSG:4326")
    mza_voronoi['area_pct'] = mza_voronoi['area_voronoi']/mza_voronoi['area_mza']
    
    # Drop used columns
    mza_voronoi.drop(columns=['area_mza','area_voronoi'],inplace=True)

    # --------------- 3.3 SUM POB DATA THAT CORRESPONDS TO EACH NODE (Groups mza_voronoi data by osmid)

    print("Adding pob data by node.")
    
    columns_of_interest = ['POBTOT','POBFEM','POBMAS',
                    'P_0A2','P_0A2_F','P_0A2_M',
                    'P_3A5','P_3A5_F','P_3A5_M',
                    'P_6A11','P_6A11_F','P_6A11_M',
                    'P_12A14','P_12A14_F','P_12A14_M',
                    'P_15A17','P_15A17_F','P_15A17_M',
                    'P_18A24','P_18A24_F','P_18A24_M',
                    'P_60YMAS','P_60YMAS_F','P_60YMAS_M',
                    'P_3YMAS','P_3YMAS_F','P_3YMAS_M',
                    'P_12YMAS','P_12YMAS_F','P_12YMAS_M',
                    'P_15YMAS','P_15YMAS_F','P_15YMAS_M',
                    'P_18YMAS','P_18YMAS_F','P_18YMAS_M',
                    'POB0_14','POB15_64','POB65_MAS'] # Similar to columns_of_interest inside function calculate_censo_nan_values_v1 but with POBTOT and without REL_H_M.

    # Create pop_nodes_gdf (Will store nodes pop output)
    pop_nodes_gdf = nodes.copy()
    if year == '2010':
        pop_nodes_gdf.drop(columns=['x','y'],inplace=True)
    elif year == '2020':
        pop_nodes_gdf.drop(columns=['x','y','street_count','city'],inplace=True)
    
    for col in columns_of_interest:
        # Turn column to lower and numeric
        col = col.lower()
        mza_voronoi[col] = pd.to_numeric(mza_voronoi[col])
    
        # Calculate pop data proportionaly to pct that voronoi area is of block
        mza_voronoi[f'voronoi_{col}'] = mza_voronoi[col] * mza_voronoi['area_pct']
    
        # Group data by osmid
        #col_data = mza_voronoi[['osmid',f'voronoi_{col}']]
        osmid_grouped_data = mza_voronoi.groupby('osmid').agg({f'voronoi_{col}':np.sum})
        
        # Merge data to nodes_gdf
        osmid_grouped_data.reset_index(inplace=True)
        pop_nodes_gdf = pd.merge(pop_nodes_gdf, osmid_grouped_data, on='osmid')
        pop_nodes_gdf.rename(columns={f'voronoi_{col}':col},inplace=True)

    ##########################################################################################
    # STEP 4: TURN NODES POP DATA TO HEXS
    
    print("--"*30)
    print("DISTRIBUTING POP DATA FROM NODES TO HEXGRID.")
    
    # Create hex_socio_gdf (Will store hexs pop output)
    hex_socio_gdf = gpd.GeoDataFrame()
    
    for res in res_list:
        # --------------- 4.1 LOAD HEXGRID
        # Load hexgrid from db
        print(f"Loading hexgrid res {res} for area of interest.")
        query = f"SELECT * FROM hexgrid.hexgrid_{res}_city_2020 WHERE \"city\" LIKE \'{city}\'"
        hex_res_gdf = aup.gdf_from_query(query, geometry_col='geometry')
        hex_res_gdf = hex_res_gdf.set_crs("EPSG:4326")
    
        # Format - Remove res from index name and add column with res
        hex_res_gdf.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
        hex_res_gdf['res'] = res
        print(f"Created hex_grid with {res} resolution")

        # --------------- 4.2 GROUP POPDATA IN HEXGRID
        # Group pop data
        string_columns = ['osmid'] # Nodes string columns are not used in aup.group_sociodemographic_data. The rest are turned into numeric and processed.
        hex_socio_df = aup.socio_points_to_polygon(hex_res_gdf, pop_nodes_gdf, 'hex_id', string_columns) 
        print(f"Agregated socio data to hex with a total of {hex_socio_df['pobtot'].sum()} population for resolution {res}.")
    
        # Hexagons data to hex_gdf GeoDataFrame
        hex_socio_gdf_tmp = hex_res_gdf.merge(hex_socio_df, on='hex_id')

        # --------------- 4.3 Add additional common fields
        # Calculate population density
        hectares = hex_socio_gdf_tmp.to_crs("EPSG:6372").area / 10000
        hex_socio_gdf_tmp['dens_pob_ha'] = hex_socio_gdf_tmp['pobtot'] / hectares 
        print(f"Calculated an average density of {hex_socio_gdf_tmp['dens_pob_ha'].mean()}")
        
        # Concatenate in hex_socio_gdf (if more resolutions, next resolution will also be stored here)
        hex_socio_gdf = pd.concat([hex_socio_gdf,hex_socio_gdf_tmp])

    # Final format
    pop_nodes_gdf['city'] = city
    hex_socio_gdf.columns = hex_socio_gdf.columns.str.lower()

    ##########################################################################################
    # STEP 5: SAVING
    # Save
    if save:
        print("--"*30)
        print(f"SAVING {city.upper()} POP DATA.")
        
        aup.gdf_to_db_slow(pop_nodes_gdf, nodes_save_table, save_schema, if_exists='append')
        print(f"Uploaded pop nodes for {city}")
        
        aup.gdf_to_db_slow(hex_socio_gdf, save_table, save_schema, if_exists='append')
        print(f"Uploaded pop hexs for {city}")


if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('--- STARTING SCRIPT.')

    # ---------------------------- BASE DATA REQUIRED ----------------------------
    # Cities (If running for 2010, should we use metro_gdf_2015? Note that AGEBs and Blocks would change.)
    metro_schema = 'metropolis'
    metro_table = 'metro_gdf_2020'
    # Year of analysis
    year = '2020' # '2010' or '2020'. ('2010' still WIP, not tested)
    # List of skip cities (If failed / want to skip city)
    skip_city_list = []
    # Hexgrid res of output
    res_list = [8,9] #Only 8,9,10 and 11 available, will run 8 and 9 for prox. analysis v2.
    # Save info
    save = False
    save_schema = 'censo'
    nodes_save_table = f'pobcenso_inegi_{year[:2]}_mzaageb_node'
    save_table = f'pobcenso_inegi_{year[:2]}_mzaageb_hex'
    
    # Test (If testing, runs res 8 for Aguascalientes ONLY and does not save it)
    test = False

    # --------------- SCRIPT
    # Load cities (municipalities)
    query = f"SELECT * FROM {metro_schema}.{metro_table}"
    metro_gdf = aup.gdf_from_query(query, geometry_col='geometry')
    metro_gdf = metro_gdf.set_crs("EPSG:4326")
    # Create a city list
    city_list = list(metro_gdf.city.unique())
    k = len(city_list)
    print(f'Loaded city list with {k} cities.')

    # Prevent cities being analyzed several times in case of a crash
    processed_city_list = []
    try:
        query = f"SELECT city FROM {save_schema}.{save_table}"
        cities_processed = aup.df_from_query(query)
        processed_city_list = list(cities_processed.city.unique())
    except:
        pass

    # LOG - Print progress of script so far
    missing_cities_list = []
    for city in city_list:
        if city not in processed_city_list:
            missing_cities_list.append(city)
    i = len(processed_city_list)
    print(f'Already processed ({i}/{k}) cities.')
    print(f'Missing procesing for cities: {missing_cities_list}')

    # If test, simplifies parameters:
    if test:
        missing_cities_list = ['Aguascalientes']
        skip_city_list = []
        res_list = [8]
        save = False

    # Main function run
    for city in missing_cities_list:
        if city not in skip_city_list:
            print("--"*40)
            i = i + 1
            print(f"Starting city {i}/{k}: {city}")
            main(city,save)
            
            
