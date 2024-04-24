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

""" 
    For each city in Mexico's metropolis list, this script loads each city's pop data (AGEB, block), 
    calculates block's nan values using calculate_censo_nan_values function,
    distributes the resulting blocks pop data to osmnx nodes using voronoi polygons and
    distributes the resulting nodes pop data to hexs, saving both nodes and hexs pop data to db.
"""

def main(city,save=False):

	##########################################################################################
	# STEP 1: LOAD DATA
    aup.log("--"*30)
    aup.log("--- LOADING CITY POP DATA.")
    
    # 1.1 --------------- CREATE AREA OF INTEREST FOR CITY 
    city_gdf = metro_gdf.loc[metro_gdf.city == city]
    city_gdf = city_gdf.set_crs("EPSG:4326")
    aoi = city_gdf.dissolve()
    
    # 1.2 --------------- LOAD POP DATA (AGEBs and Blocks)
    aup.log("--- Loading blocks and AGEBs for area of interest.")
    pop_ageb_gdf = gpd.GeoDataFrame()
    pop_mza_gdf = gpd.GeoDataFrame()

    # Load states for current city (CVE_ENT)
    cve_ent_list = list(city_gdf.CVE_ENT.unique())

    for cve_ent in cve_ent_list:
        #Load muns in each city state
        cve_mun_list = list(city_gdf.loc[city_gdf.CVE_ENT == cve_ent].CVE_MUN.unique())
        # To avoid error that happens when there's only one MUN in State: [SQL: SELECT * FROM censo_mza.censo_mza_2020 WHERE ("CVE_ENT" = '02') AND "CVE_MUN" IN ('001',) ]
        # Duplicate mun inside tupple if there's only one MUN.
        if len(cve_mun_list) >= 2:
            cve_mun_tpl = str(tuple(cve_mun_list))
        else:
            cve_mun_list.append(cve_mun_list[0])
            cve_mun_tpl = str(tuple(cve_mun_list))
        # Load AGEBs and concat
        query = f"SELECT * FROM censoageb.censoageb_{year} WHERE (\"cve_ent\" = \'{cve_ent}\') AND \"cve_mun\" IN {cve_mun_tpl} "
        pop_ageb_gdf = pd.concat([pop_ageb_gdf,aup.gdf_from_query(query, geometry_col='geometry')])
        # Load blocks and concat
        query = f"SELECT * FROM censo_mza.censo_mza_{year} WHERE (\"CVE_ENT\" = \'{cve_ent}\') AND \"CVE_MUN\" IN {cve_mun_tpl} "
        pop_mza_gdf = pd.concat([pop_mza_gdf,aup.gdf_from_query(query, geometry_col='geometry')])

    # Set CRS
    pop_ageb_gdf = pop_ageb_gdf.set_crs("EPSG:4326")
    pop_mza_gdf = pop_mza_gdf.set_crs("EPSG:4326")
    
    ##########################################################################################
	# STEP 2: CALCULATE NaN VALUES for pop fields (most of them, check function) of gdf containing blocks.
    aup.log("--"*30)
    aup.log(f"--- CALCULATING NAN VALUES FOR POP FIELDS IN {city.upper()}.")
    
    # 2.1 --------------- CALCULATE_CENSO_NAN_VALUES FUNCTION
    pop_mza_gdf_calc = aup.calculate_censo_nan_values_v1(pop_ageb_gdf,pop_mza_gdf,extended_logs=False)

    ##########################################################################################
	# STEP 3: DISTRIBUTE POP BLOCK DATA TO NODES USING VORONOI
    aup.log("--"*30)
    aup.log("--- DISTRIBUTING POP DATA FROM BLOCKS TO NODES")

    # 3.0 --------------- LOAD OSMNX NODES
    aup.log("--- Loading nodes for area of interest.")
    
    if year == '2010':
        _, nodes, _ = aup.graph_from_hippo(aoi, schema='networks', edges_folder='edges_2011', nodes_folder='nodes_2011')
        # FOR NETWORK 2011 ONLY: Drop unncessary columns from nodes column (only present in 2011)
        nodes.drop(['ID', 'TIPOVIA', 'TIPO', 
                    'NUMERO', 'DERE_TRAN', 'ADMINISTRA', 'NUME_CARR', 'CONDICION', 
                    'ORIGEN', 'CALI_REPR', 'CVEGEO', 'NOMVIAL', 'SENTIDO', 'LONGITUD', 'UNIDAD', 
                    'vertex_pos', 'vertex_ind', 'vertex_par', 'vertex_p_1', 
                    'distance', 'angle'], inplace = True, axis=1)
    
    elif year == '2020':
        _, nodes, _ = aup.graph_from_hippo(aoi, schema='osmnx', edges_folder='edges_osmnx_23_line', nodes_folder='nodes_osmnx_23_point')
    
    nodes.reset_index(inplace=True)
    nodes = nodes.to_crs("EPSG:4326")

    # 3.1 --------------- CREATE VORONOI POLYGONS USING NODES
    aup.log("--- Creating voronois with nodes osmid data.")

    # Create voronois
    voronois_gdf = aup.voronoi_points_within_aoi(aoi,nodes,'osmid')
    nodes_voronoi_gdf = voronois_gdf[['osmid','geometry']]

    # 3.2 --------------- SPATIAL INTERSECTION OF POLYGONS WITH BLOCKS
    aup.log("--- Creating spatial join between voronoi polygons and blocks.")
    
    # Calculate block area
    mza_gdf = pop_mza_gdf_calc.to_crs("EPSG:6372")
    mza_gdf['area_mza'] = mza_gdf.geometry.area
    mza_gdf = mza_gdf.to_crs("EPSG:4326")
    
    # Overlay blocks with voronoi (Spatial intersection)
    mza_voronoi = gpd.overlay(df1=mza_gdf, df2=nodes_voronoi_gdf, how="intersection")
    del mza_gdf

    aup.log("--- Calculating area_pct that corresponds to each osmid within each block.")

    # Calculate pct of area that corresponds to each osmid within each block
    mza_voronoi = mza_voronoi.to_crs("EPSG:6372")
    mza_voronoi['area_voronoi'] = mza_voronoi.geometry.area
    mza_voronoi = mza_voronoi.to_crs("EPSG:4326")
    mza_voronoi['area_pct'] = mza_voronoi['area_voronoi']/mza_voronoi['area_mza']
    
    # Drop used columns
    mza_voronoi.drop(columns=['area_mza','area_voronoi'],inplace=True)

    # 3.3 --------------- SUM POP DATA THAT CORRESPONDS TO EACH NODE (Groups mza_voronoi data by osmid)
    aup.log("--- Adding pop data by node.")
    
    # List to be similar to columns_of_interest inside function calculate_censo_nan_values_v1,
    # but in .lower(), with pobtot, without rel_h_m 
    columns_of_interest = ['pobtot','pobfem','pobmas',
                           'p_0a2','p_0a2_f','p_0a2_m',
                           'p_3a5','p_3a5_f','p_3a5_m',
                           'p_6a11','p_6a11_f','p_6a11_m',
                           'p_12a14','p_12a14_f','p_12a14_m',
                           'p_15a17','p_15a17_f','p_15a17_m',
                           'p_18a24','p_18a24_f','p_18a24_m',
                           'p_60ymas','p_60ymas_f','p_60ymas_m',
                           'p_3ymas','p_3ymas_f','p_3ymas_m',
                           'p_12ymas','p_12ymas_f','p_12ymas_m',
                           'p_15ymas','p_15ymas_f','p_15ymas_m',
                           'p_18ymas','p_18ymas_f','p_18ymas_m',
                           'pob0_14','pob15_64','pob65_mas',
                           'pcon_disc'] 

    # Create pop_nodes_gdf (Will store nodes pop output by node)
    pop_nodes_gdf = nodes.copy()
    if year == '2010':
        pop_nodes_gdf.drop(columns=['x','y'],inplace=True)
    elif year == '2020':
        pop_nodes_gdf.drop(columns=['x','y','street_count','city'],inplace=True)
    
    # For each column, sum pop data by osmid and assign to node
    for col in columns_of_interest:
        # Turn column to numeric
        mza_voronoi[col] = pd.to_numeric(mza_voronoi[col])
        # Calculate pop data proportionaly to pct that voronoi area is of block
        mza_voronoi[f'voronoi_{col}'] = mza_voronoi[col] * mza_voronoi['area_pct']
        # Group data by osmid
        osmid_grouped_data = mza_voronoi.groupby('osmid').agg({f'voronoi_{col}':np.sum})
        # Merge data to nodes_gdf
        osmid_grouped_data.reset_index(inplace=True)
        pop_nodes_gdf = pd.merge(pop_nodes_gdf, osmid_grouped_data, on='osmid')
        pop_nodes_gdf.rename(columns={f'voronoi_{col}':col},inplace=True)

    ##########################################################################################
    # STEP 4: TURN NODES POP DATA TO HEXS POP DATASET
    aup.log("--"*30)
    aup.log("--- DISTRIBUTING POP DATA FROM NODES TO HEXGRID.")
    
    # Create hex_socio_gdf (Will store hexs pop output)
    hex_socio_gdf = gpd.GeoDataFrame()
    
    for res in res_list:
        # 4.1 --------------- LOAD HEXGRID
        # Load hexgrid from db
        aup.log(f"--- Loading hexgrid res {res} for area of interest.")
        query = f"SELECT * FROM hexgrid.hexgrid_{res}_city_2020 WHERE \"city\" LIKE \'{city}\'"
        hex_res_gdf = aup.gdf_from_query(query, geometry_col='geometry')
        hex_res_gdf = hex_res_gdf.set_crs("EPSG:4326")
        # Format - Remove res from index name and add column with res
        hex_res_gdf.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
        hex_res_gdf['res'] = res
        aup.log(f"--- Created hex_grid with {res} resolution")

        # 4.2 --------------- GROUP POPDATA IN HEXGRID
        # Group pop data
        string_columns = ['osmid'] # Nodes string columns are not used in aup.group_sociodemographic_data. The rest are turned into numeric and processed.
        hex_socio_df = aup.socio_points_to_polygon(hex_res_gdf, pop_nodes_gdf, 'hex_id', string_columns,include_nearest=(True,'osmid')) 
        aup.log(f"--- Agregated socio data to hex with a total of {hex_socio_df['pobtot'].sum()} population for resolution {res}.")
        # Hexagons data to hex_gdf GeoDataFrame
        hex_socio_gdf_tmp = hex_res_gdf.merge(hex_socio_df, on='hex_id')

        # 4.3 --------------- Add additional common fields
        # Calculate population density
        hectares = hex_socio_gdf_tmp.to_crs("EPSG:6372").area / 10000
        hex_socio_gdf_tmp['dens_pob_ha'] = hex_socio_gdf_tmp['pobtot'] / hectares 
        aup.log(f"--- Calculated an average density of {hex_socio_gdf_tmp['dens_pob_ha'].mean()}")
        # Concatenate in hex_socio_gdf (if more resolutions, next resolution will also be stored here)
        hex_socio_gdf = pd.concat([hex_socio_gdf,hex_socio_gdf_tmp])

    # Final format
    pop_nodes_gdf['city'] = city
    hex_socio_gdf.columns = hex_socio_gdf.columns.str.lower()

    ##########################################################################################
    # STEP 5: SAVING
    aup.log("--"*30)
    aup.log(f"--- SAVING POP DATA FOR {city}.")

    # Save
    if save:
        aup.log("--"*30)
        aup.log(f"--- SAVING {city.upper()} POP DATA.")
        
        aup.gdf_to_db_slow(pop_nodes_gdf, nodes_save_table, save_schema, if_exists='append')
        aup.log(f"--- Uploaded pop nodes for {city}")
        
        aup.gdf_to_db_slow(hex_socio_gdf, save_table, save_schema, if_exists='append')
        aup.log(f"--- Uploaded pop hexs for {city}")

    if test:
        pop_nodes_gdf.to_file(nodes_local_save_dir, driver='GPKG')
        hex_socio_gdf.to_file(final_local_save_dir, driver='GPKG')


if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('--- STARTING SCRIPT 22.')

    # ------------------------------ BASE DATA REQUIRED ------------------------------ 
    # Cities (If running for 2010, should we use metro_gdf_2015? Note that AGEBs and Blocks would change.)
    metro_schema = 'metropolis'
    metro_table = 'metro_gdf_2020'
    # Year of analysis
    year = '2020' # '2010' or '2020'. ('2010' still WIP, not tested)
    # List of skip cities (If failed / want to skip city)
    skip_city_list = ['CDMX', 'ZMVM']
    # Hexgrid res of output
    res_list = [8,9] #Only 8,9,10 and 11 available, run 8 and 9 only for prox. analysis v2.
    # Save info
    save = True
    save_schema = 'censo'
    nodes_save_table = f'pobcenso_inegi_{year[:2]}_mzaageb_node'
    save_table = f'pobcenso_inegi_{year[:2]}_mzaageb_hex'
    
    # Test - (If testing, Script runs res 8 for Aguascalientes ONLY and saves it ONLY locally. (Make sure directory exists)
    test = True
    nodes_local_save_dir = f"../data/processed/pop_data/test_ags_script22_nodes.gpkg"
    final_local_save_dir = f"../data/processed/pop_data/test_ags_script22_hex.gpkg"

    # ------------------------------ SCRIPT ------------------------------
    # If test, simplifies script parameters:
    if test:
        missing_cities_list = ['Aguascalientes']
        skip_city_list = []
        res_list = [8,9]
        save = False
        local_save = True
        i = 0
        k = len(missing_cities_list)

        city = 'Aguascalientes'
        query = f"SELECT * FROM {metro_schema}.{metro_table} WHERE \"city\" LIKE \'{city}\'"
        metro_gdf = aup.gdf_from_query(query, geometry_col='geometry')
        metro_gdf = metro_gdf.set_crs("EPSG:4326")

        aup.log(f"Processing test for {missing_cities_list} at res {res_list}.")

    # If not test, runs Mexico's cities
    else:
        # Load cities (municipalities)
        query = f"SELECT * FROM {metro_schema}.{metro_table}"
        metro_gdf = aup.gdf_from_query(query, geometry_col='geometry')
        metro_gdf = metro_gdf.set_crs("EPSG:4326")
        # Create a city list
        city_list = list(metro_gdf.city.unique())
        k = len(city_list)
        aup.log(f'--- Loaded city list with {k} cities.')

        # Prevent cities being analyzed several times in case of a crash
        processed_city_list = []
        try:
            query = f"SELECT city FROM {save_schema}.{save_table}"
            cities_processed = aup.df_from_query(query)
            processed_city_list = list(cities_processed.city.unique())
        except:
            pass

        # LOG CODE - Print progress of script so far
        missing_cities_list = []
        for city in city_list:
            if city not in processed_city_list:
                missing_cities_list.append(city)
        i = len(processed_city_list)
        aup.log(f'--- Already processed ({i}/{k}) cities.')
        aup.log(f'--- Missing procesing for cities: {missing_cities_list}')

    # Main function run
    for city in missing_cities_list:
        if city not in skip_city_list:
            aup.log("--"*40)
            i = i + 1
            aup.log(f"--- Starting city {i}/{k}: {city}")
            main(city,save)
            
            
