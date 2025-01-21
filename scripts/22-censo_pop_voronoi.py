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
else:
    import aup

""" 
    For each city in Mexico's metropolis list, this script loads from the database each city's pop data (AGEB, block), 
    calculates block's nan values using calculate_censo_nan_values() function,
    distributes the resulting blocks pop data to OSMnx nodes using voronoi polygons to assign a pct of the blocks data to each node, and
    distributes the resulting nodes pop data to hexagons of different resolutions, saving blocks, nodes and hexs pop data to the database.
"""

def main(city, save_blocks=False, save_nodes=False, save_hexs=False, local_save=True):

	##########################################################################################
	# STEP 1: LOAD DATA
    aup.log("--"*30)
    aup.log(f"--- {city} - LOADING CITY POP DATA.")
    
    # 1.1 --------------- CREATE AREA OF INTEREST FOR CITY 
    city_gdf = metro_gdf.loc[metro_gdf.city == city]
    city_gdf = city_gdf.set_crs("EPSG:4326")
    aoi = city_gdf.dissolve()
    
    # 1.2 --------------- LOAD POP DATA (AGEBs and Blocks)
    aup.log("--- Loading blocks and AGEBs for area of interest.")
    # Create a tupple from a list with all unique cvegeo_mun ('CVE_ENT'+'CVE_MUN') of current city
    city_gdf['cvegeo_mun'] = city_gdf['CVE_ENT']+city_gdf['CVE_MUN']
    cvegeo_mun_lst = list(city_gdf.cvegeo_mun.unique())
    # To avoid error that happens when there's only one MUN in State: 
    # e.g.: <<< SELECT * FROM censo.censo_inegi_{year[2:]}_mza WHERE ("entidad" = '02') AND "mun" IN ('001',) >>>
    # Duplicate mun inside tupple if there's only one MUN.
    if len(cvegeo_mun_lst) >= 2:
        cvegeo_mun_tpl = str(tuple(cvegeo_mun_lst))
    else:
        cvegeo_mun_lst.append(cvegeo_mun_lst[0])
        cvegeo_mun_tpl = str(tuple(cvegeo_mun_lst))
    aup.log(f"--- Area of interest muns: {cvegeo_mun_tpl}.")
    # Load AGEBs and blocks
    ageb_query = f"SELECT * FROM censo.censo_inegi_{year[2:]}_ageb WHERE \"cvegeo_mun\" IN {cvegeo_mun_tpl}"
    pop_ageb_gdf = aup.gdf_from_query(ageb_query, geometry_col='geometry')
    mza_query = f"SELECT * FROM censo.censo_inegi_{year[2:]}_mza WHERE \"cvegeo_mun\" IN {cvegeo_mun_tpl}"
    pop_mza_gdf = aup.gdf_from_query(mza_query, geometry_col='geometry')
    # Set CRS
    pop_ageb_gdf = pop_ageb_gdf.set_crs("EPSG:4326")
    pop_mza_gdf = pop_mza_gdf.set_crs("EPSG:4326")
    # Logs
    ageb_pobtot = pop_ageb_gdf['pobtot'].sum()
    mza_pobtot = pop_mza_gdf['pobtot'].sum()
    aup.log(f"--- Loaded AGEBs with total population of {ageb_pobtot} for area of interest.")
    aup.log(f"--- Loaded blocks with total population of {mza_pobtot} for area of interest.")
    aup.log(f"--- Blocks - AGEBs popdiff = {mza_pobtot - ageb_pobtot} for area of interest.")
    if year == '2020':
        mza_urbana = pop_mza_gdf.loc[pop_mza_gdf.ambito=='Urbana'].copy()
        mza_urbana_pobtot = mza_urbana['pobtot'].sum()
        aup.log(f"--- Loaded blocks from 2020 have a total URBAN population of {mza_urbana_pobtot} for area of interest.")
        aup.log(f"--- URBAN blocks - AGEBs popdiff = {mza_urbana_pobtot - ageb_pobtot} for area of interest.")
        del mza_urbana
    # Save disk space ---
    del city_gdf
    # -------------------


    ##########################################################################################
	# STEP 2: CALCULATE NaN VALUES FOR POP FIELDS (most of them, explore function for more detail) INSIDE BLOCKS GDF.
    aup.log("--"*30)
    aup.log(f"--- {city} - CALCULATING NAN VALUES FOR POP FIELDS.")
    
    # 2.1 --------------- CALCULATE_CENSO_NAN_VALUES FUNCTION
    pop_mza_gdf_calc = aup.calculate_censo_nan_values_v1(pop_ageb_gdf,pop_mza_gdf,year=year,extended_logs=False)
    # Save disk space ---
    del pop_ageb_gdf
    del pop_mza_gdf
    # -------------------
    
    # 2.2 --------------- SAVE
    # Save format
    pop_mza_gdf_calc_save = pop_mza_gdf_calc.copy()
    pop_mza_gdf_calc_save['city'] = city
    # Save calculated blocks to database
    if save_blocks:
        aup.log(f"--- Saving {city}'s blocks pop data to database.")
        
        # Save blocks
        limit_len = 10000
        if len(pop_mza_gdf_calc_save)>limit_len:
            c_upload = len(pop_mza_gdf_calc_save)/limit_len
            for k in range(int(c_upload)+1):
                aup.log(f"--- Uploading calc pop blocks - Starting range k = {k} of {int(c_upload)}")
                gdf_inter_upload = pop_mza_gdf_calc_save.iloc[int(limit_len*k):int(limit_len*(1+k))].copy()
                aup.gdf_to_db_slow(gdf_inter_upload, blocks_save_table, save_schema, if_exists='append')
            aup.log(f"--- Uploaded calc pop blocks for {city}.")
        else:
            aup.gdf_to_db_slow(pop_mza_gdf_calc_save, blocks_save_table, save_schema, if_exists='append')
            aup.log(f"--- Uploaded calc pop blocks for {city}.")
    # Save calculated blocks locally
    if local_save:
        aup.log(f"--- Saving {city}'s blocks pop data locally.")
        pop_mza_gdf_calc_save.to_file(local_save_dir + f"script22_{year}{city}_pop_mza_gdf_calc.gpkg", driver='GPKG')
    # Save disk space ---
    del pop_mza_gdf_calc_save #Blocks with calculated pop data [If save/local_save, saved to database/locally]
    # -------------------
    

    ##########################################################################################
	# STEP 3: DISTRIBUTE POP BLOCK DATA TO NODES USING VORONOI
    aup.log("--"*30)
    aup.log(f"--- {city} - DISTRIBUTING POP DATA FROM BLOCKS TO NODES.")

    # 3.0 --------------- LOAD OSMNX NODES
    aup.log("--- Loading nodes for area of interest (by aoi).")
    # Load data
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
    # Format data
    nodes.reset_index(inplace=True)
    nodes = nodes.to_crs("EPSG:4326")
    
    # 3.1 --------------- CREATE VORONOI POLYGONS USING NODES
    aup.log("--- Creating voronois with nodes osmid data.")
    # Create voronois
    voronois_gdf = aup.voronoi_points_within_aoi(aoi,nodes,'osmid')
    voronois_gdf = voronois_gdf[['osmid','geometry']]
    # Save disk space ---
    del aoi
    # -------------------

    # 3.2 --------------- SPATIAL INTERSECTION OF VORONOI POLYGONS WITH BLOCKS
    # ------------------- (Finds area_pct, used to distribute pop data)
    aup.log("--- Creating spatial join between voronoi polygons and blocks.")
    # Calculate total block area
    pop_mza_gdf_calc = pop_mza_gdf_calc.to_crs("EPSG:6372")
    pop_mza_gdf_calc['area_mza'] = pop_mza_gdf_calc.geometry.area
    pop_mza_gdf_calc = pop_mza_gdf_calc.to_crs("EPSG:4326")
    # Overlay blocks with voronoi
    # (Spatial intersection, creates split blocks with data from the original block 
    #  and the voronoi poly it falls in)
    mza_voronoi = gpd.overlay(df1=pop_mza_gdf_calc, df2=voronois_gdf, how="intersection")
    aup.log("--- Calculating area_pct that corresponds to each osmid within each block.")
    # Calculate pct of area of each block that falls in any given osmid voronoi polygon
    mza_voronoi = mza_voronoi.to_crs("EPSG:6372")
    mza_voronoi['area_voronoi'] = mza_voronoi.geometry.area
    mza_voronoi = mza_voronoi.to_crs("EPSG:4326")
    mza_voronoi['area_pct'] = mza_voronoi['area_voronoi']/mza_voronoi['area_mza']
    # Drop used columns
    mza_voronoi.drop(columns=['area_mza','area_voronoi'],inplace=True)
    # Save disk space ---
    del pop_mza_gdf_calc
    # -------------------

    # 3.3 --------------- GROUP POP DATA THAT CORRESPONDS TO EACH NODE 
    # ------------------- (Groups mza_voronoi data by osmid)
    aup.log("--- Adding pop data by node.")
    # List is similar to columns_of_interest inside function calculate_censo_nan_values_v1
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
                           'pob0_14','pob15_64','pob65_mas']
    if year == "2010":
        columns_of_interest.append('pcon_lim')
    elif year == "2020":
        columns_of_interest.append('pcon_disc')

    # Create pop_nodes_gdf (Will store nodes pop output by node) from nodes gdf.
    pop_nodes_gdf = nodes.copy()
    if year == '2010':
        pop_nodes_gdf.drop(columns=['x','y'],inplace=True)
    elif year == '2020':
        pop_nodes_gdf.drop(columns=['x','y','street_count','city'],inplace=True)
    # For each column, sum pop data by osmid (Distributing pop data by considering pct of area of original block) and assigning it to its node
    for col in columns_of_interest:
        # Turn column to numeric
        mza_voronoi[col] = pd.to_numeric(mza_voronoi[col])
        # Calculate pop data proportionaly to pct of overlayed voronoi area relative to block area
        mza_voronoi[f'voronoi_{col}'] = mza_voronoi[col] * mza_voronoi['area_pct']
        # Group data by osmid
        osmid_grouped_data = mza_voronoi.groupby('osmid').agg({f'voronoi_{col}':np.sum})
        # Merge data to nodes_gdf
        osmid_grouped_data.reset_index(inplace=True)
        pop_nodes_gdf = pd.merge(pop_nodes_gdf, osmid_grouped_data, on='osmid')
        pop_nodes_gdf.rename(columns={f'voronoi_{col}':col},inplace=True)
    aup.log(f"--- Distributed block data to nodes, total population of {pop_nodes_gdf['pobtot'].sum()} for area of interest.")
    # Save disk space ---
    del nodes
    del mza_voronoi
    del osmid_grouped_data
    # -------------------

    # 3.4 --------------- CALCULATE POP DENSITY IN NODES (USING IT'S VORONOI POLYGON'S AREA)
    # Calculate whole voronoi's area
    voronois_gdf = voronois_gdf.to_crs("EPSG:6372")
    voronois_gdf['area_has'] = voronois_gdf.area/10000
    voronois_gdf = voronois_gdf.to_crs("EPSG:4326")
    # Merge poptot data by node with the whole voronoi polygon using 'osmid'
    dens_voronoi = pd.merge(pop_nodes_gdf[['osmid','pobtot']], voronois_gdf[['osmid','area_has']], on='osmid')
    # Calculate density
    dens_voronoi['dens_pob_ha'] = dens_voronoi['pobtot'] / dens_voronoi['area_has']
    # Merge back that density data to nodes_gdf using 'osmid'
    pop_nodes_gdf = pd.merge(pop_nodes_gdf, dens_voronoi[['osmid','dens_pob_ha']], on='osmid')
    aup.log(f"--- Added density to nodes using each voronoi polygon's area.")
    # Save disk space ---
    del dens_voronoi
    # -------------------

    # 3.5 --------------- SAVE
    # Save format
    pop_nodes_gdf_save = pop_nodes_gdf.copy()
    pop_nodes_gdf_save['city'] = city
    # Save nodes to database
    if save_nodes:
        aup.log(f"--- Saving {city}'s nodes pop data to database.")
        # Saving nodes to database
        limit_len = 10000
        if len(pop_nodes_gdf_save)>limit_len:
            c_upload = len(pop_nodes_gdf_save)/limit_len
            for k in range(int(c_upload)+1):
                aup.log(f"--- Uploading pop nodes - Starting range k = {k} of {int(c_upload)}")
                gdf_inter_upload = pop_nodes_gdf_save.iloc[int(limit_len*k):int(limit_len*(1+k))].copy()
                aup.gdf_to_db_slow(gdf_inter_upload, nodes_save_table, save_schema, if_exists='append')
            aup.log(f"--- Uploaded pop nodes for {city}.")
        else:
            aup.gdf_to_db_slow(pop_nodes_gdf_save, nodes_save_table, save_schema, if_exists='append')
            aup.log(f"--- Uploaded pop nodes for {city}.")
    # Save nodes locally
    if local_save:
        aup.log(f"--- Saving {city}'s nodes pop data locally.")
        voronois_gdf.to_file(local_save_dir + f"script22_{year}{city}_voronoipolys.gpkg", driver='GPKG')
        pop_nodes_gdf_save.to_file(local_save_dir + f"script22_{year}{city}_pop_nodes_gdf.gpkg", driver='GPKG')
    
    aup.log(f"--- Finished Step 03: Pop block data to nodes.")
    # Save disk space ---
    del pop_nodes_gdf_save #Nodes with pop data [If save/local_save, saved to database/locally]
    del voronois_gdf #Voronoi polygons [If local_save, saved locally]
    # -------------------
    
    ##########################################################################################
    # STEP 4: TURN NODES POP DATA TO HEXS POP DATASET

    if process_nodes_to_hexs:
        aup.log("--"*30)
        aup.log(f"--- {city} - DISTRIBUTING POP DATA FROM NODES TO HEXGRID.")
        
        # Create hex_socio_gdf (Will store hexs pop output)
        hex_socio_gdf = gpd.GeoDataFrame()
        # For each res, load hexs and group data
        for res in res_list:
            # 4.1 --------------- LOAD HEXGRID
            # Load hexgrid from db
            aup.log(f"--- Loading hexgrid res {res} for area of interest (by city name).")
            hex_query = f"SELECT * FROM hexgrid.hexgrid_{res}_city_2020 WHERE \"city\" LIKE \'{city}\'" #Always load 2020, just as metro_gdf_2020
            hex_res_gdf = aup.gdf_from_query(hex_query, geometry_col='geometry')
            hex_res_gdf = hex_res_gdf.set_crs("EPSG:4326")
            # Format - Remove res from index name and add column with res
            hex_res_gdf.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
            hex_res_gdf['res'] = res
            aup.log(f"--- Created hex_grid with {res} resolution")

            # 4.2 --------------- GROUP POPDATA IN HEXGRID
            # Group pop data
            # Notes: Nodes's string columns are not used to group data in aup.group_sociodemographic_data() inside socio_points_to_polygon().
            # The rest of the columns are turned into numeric and grouped.
            # dens_pob_ha (Which would need to be in arg. 'avg_column') is not added because it gets overwritten below.
            string_columns = ['osmid'] 
            hex_socio_df = aup.socio_points_to_polygon(hex_res_gdf, pop_nodes_gdf, 'hex_id', string_columns, include_nearest=(True,'osmid')) 
            aup.log(f"--- Agregated socio data to hex with a total of {hex_socio_df['pobtot'].sum()} population for resolution {res}.")
            # Hexagons data to hex_gdf GeoDataFrame
            hex_socio_gdf_tmp = hex_res_gdf.merge(hex_socio_df, on='hex_id')

            # 4.3 --------------- CALCULATE POP DENSITY IN HEXS
            # Calculate population density (Considering tot pop and tot area of hex instead of average of nodes)
            hectares = hex_socio_gdf_tmp.to_crs("EPSG:6372").area / 10000
            hex_socio_gdf_tmp['dens_pob_ha'] = hex_socio_gdf_tmp['pobtot'] / hectares 
            aup.log(f"--- Calculated an average density of {hex_socio_gdf_tmp['dens_pob_ha'].mean()}")
            

            # 4.4 --------------- ADD MISSING HEXS
            # EXPLANATION: 
            # The spatial intersection in step 3.2 creates split blocks containing data from the original BLOCK and each voronoi polygons it falls in.
            # After that, the data is grouped by osmid, and the data is distributed to nodes.
            # That means that not all nodes are added, just those whose voronoi polygon intersected any block. The rest are dropped.
            # Therefore, some urban hexs (which are designed as urban if they touch any urban geoestadistical areas, AGEBs) may be inside urban areas where 
            # there are 0 nodes with data from blocks because there are no blocks nearby. --> These hexs are not added in the previous step (aup.socio_points_to_polygon())
            # This step adds those missing urban hexs with pop data = 0.

            # List all currently added hex_ids
            current_res_hex_ids = list(hex_socio_gdf_tmp.hex_id.unique())
            # Identify all current res's urban hex_ids
            urban_res_hexs = hex_res_gdf.loc[hex_res_gdf['type']=='urban'].copy()
            aup.log(f"--- Found {len(urban_res_hexs)} urban hexs.")
            urban_res_hexs_lst = list(urban_res_hexs.hex_id.unique())
            # Isolate missing urban hex_ids
            missing_hexs = list(set(urban_res_hexs_lst) - set(current_res_hex_ids))
            # Create a GeoDataFrame with missing hexs
            missing_hexs_gdf = hex_res_gdf.loc[hex_res_gdf.hex_id.isin(missing_hexs)].copy()
            # Add missing pop data to missing hexs
            for col in columns_of_interest:
                missing_hexs_gdf[col] = 0
            missing_hexs_gdf['dens_pob_ha'] = 0
            # Concatenate missing hexs to hex_socio_gdf
            hex_socio_gdf_tmp = pd.concat([hex_socio_gdf_tmp,missing_hexs_gdf])
            aup.log(f"--- Added {len(missing_hexs_gdf)} missing urban hexs to hex_socio_gdf.")

            # 4.5 --------------- CONCATENATE ALL RESOLUTIONS
            # Concatenate in hex_socio_gdf (if more resolutions, next resolution will also be stored here)
            hex_socio_gdf = pd.concat([hex_socio_gdf,hex_socio_gdf_tmp])
            
        # 4.4 --------------- SAVE
        # Final format
        hex_socio_gdf.columns = hex_socio_gdf.columns.str.lower()
        # Save to database
        if save_hexs:
            aup.log(f"--- Saving {city}'s hexs pop data to database.")
            # Saving hexs to database
            limit_len = 10000
            if len(hex_socio_gdf)>limit_len:
                c_upload = len(hex_socio_gdf)/limit_len
                for k in range(int(c_upload)+1):
                    aup.log(f"--- Uploading pop hexs - Starting range k = {k} of {int(c_upload)}")
                    gdf_inter_upload = hex_socio_gdf.iloc[int(limit_len*k):int(limit_len*(1+k))].copy()
                    aup.gdf_to_db_slow(gdf_inter_upload, hexs_save_table, save_schema, if_exists='append')
                aup.log(f"--- Uploaded pop hexs for {city}.")
            else:
                aup.gdf_to_db_slow(hex_socio_gdf, hexs_save_table, save_schema, if_exists='append')
                aup.log(f"--- Uploaded pop hexs for {city}.")
        # Saving hexs locally
        if local_save:
            aup.log(f"--- Saving {city}'s hexs pop data locally.")
            hex_socio_gdf.to_file(local_save_dir + f"script22_{year}{city}_hex.gpkg", driver='GPKG')
        # Save disk space
        del pop_nodes_gdf
        del hex_res_gdf
        del hex_socio_df
        del hex_socio_gdf_tmp
        del hex_socio_gdf

    else:
        aup.log(f"--- Skipped nodes to hexs processing and saving for {city}.")

    aup.log(f"--- Finished main function for {city}.")


if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('--- STARTING SCRIPT 22.')

    # ------------------------------ BASE DATA REQUIRED ------------------------------    
    # Year of analysis
    year = '2010' # '2010' or '2020'. ('2010' still WIP, not tested)
    # Hexgrid res of output
    res_list = [8,9,10] #Only 8,9,10 and 11 available, run 8 and 9 only for prox. analysis v2.
    
    # List of skip cities (If failed / want to skip city)
    # NOTE: The following cities's output have population differences between input (Blocks) and output (Nodes, hexs)
    # due to blocks/agebs being outside of the municipality boundaries (attributed to INEGI, 2020)
    #pop_diff_cities = ['ZMVM','Celaya','Acapulco','Pachuca','Oaxaca','Queretaro','Los Mochis','Mazatlan']
    skip_city_list = []

    # ------------------------------ SCRIPT STEPS ------------------------------
    # (Used to divide process during Dev.)
    process_nodes_to_hexs = True

    # ------------------------------ SAVING ------------------------------
    
    # Save output to database?
    save_schema = 'censo'
    save_blocks = False
    blocks_save_table = f'pobcenso_inegi_{year[2:]}_mzaageb_mza'
    save_nodes = False
    nodes_save_table = f'pobcenso_inegi_{year[2:]}_mzaageb_node'
    save_hexs = False
    hexs_save_table = f'pobcenso_inegi_{year[2:]}_mzaageb_hex'

    # Save outputs to local? (Make sure directory exists)
    local_save = False
    local_save_dir = f"../data/scripts_output/script_22/"
    
    # Test - (If testing, Script runs res 8 for one city ONLY and saves it locally ONLY)
    test = True
    city_list = ['Aguascalientes']

    # ------------------------------ SCRIPT ------------------------------
    # Cities (database) [Always 2020]
    metro_schema = 'metropolis'
    metro_table = 'metro_gdf_2020'
    # If test, simplifies script parameters
    if test:
        skip_city_list = []
        processed_city_list = []
        res_list = [8]
        save = False
        local_save = True
        # Only loads cities from the specified city_list (Above)
        k = len(city_list)
        # To avoid error that happens when there's only city in city_list
        # e.g.: <<< "SELECT * FROM {metro_schema}.{metro_table} WHERE \"city\" IN ('Aguascalientes',) >>>
        # Duplicate city inside tupple if there's only one city, and register that it was already run to avoid re-running.
        if len(city_list) >= 2:
            city_tpl = str(tuple(city_list))
        else:
            city_list.append(city_list[0])
            city_tpl = str(tuple(city_list))
        metro_query = f"SELECT * FROM {metro_schema}.{metro_table} WHERE \"city\" IN {city_tpl}"
        metro_gdf = aup.gdf_from_query(metro_query, geometry_col='geometry')
        metro_gdf = metro_gdf.set_crs("EPSG:4326")
        aup.log(f"Processing test for {k} cities at res {res_list}.")

    # If not test, runs Mexico's cities taking into account
    # processed_city_list (Cities alredy processed to hexs) and skip_city_list.
    else:
        # Load all cities' municipalities
        metro_query = f"SELECT * FROM {metro_schema}.{metro_table}"
        metro_gdf = aup.gdf_from_query(metro_query, geometry_col='geometry')
        metro_gdf = metro_gdf.set_crs("EPSG:4326")
        # Create a city list (All cities)
        city_list = list(metro_gdf.city.unique())
        k = len(city_list)
        aup.log(f'--- Loaded city list with {k} cities.')
        # List to discard cities already processed.
        # WARNING: For city to be discarded it must have been processed all until hexs.
        try:
            saved_query = f"SELECT city FROM {save_schema}.{hexs_save_table}"
            cities_processed = aup.df_from_query(saved_query)
            processed_city_list = list(cities_processed.city.unique())
        except:
            processed_city_list = []
            pass

        # LOG CODE - Prints progress of script so far (Has no effect on script)
        missing_cities_list = []
        for city in city_list:
            if city not in processed_city_list:
                missing_cities_list.append(city)
        processed_len = len(processed_city_list)
        aup.log(f'--- Already fully processed {processed_len} cities.')
        aup.log(f'--- Missing procesing for {k-processed_len} cities but process will skip {len(skip_city_list)} cities.')

    # Main function run
    script_run_lst = []
    i = 0
    for city in city_list:
        if city in processed_city_list:
            aup.log("--"*40)
            i+=1
            aup.log(f"--- Already fully processed city {i}/{k}: {city}")
        elif city in skip_city_list:
            aup.log("--"*40)
            i+=1
            aup.log(f"--- Skipping city {i}/{k}: {city}")
        elif city in script_run_lst:
            aup.log("--"*40)
            i+=1
            aup.log(f"--- City {city} already ran during current script run. (Applies to test format).")   
        else:
            aup.log("--"*40)
            i+=1
            aup.log(f"--- Starting city {i}/{k}: {city}")
            main(city, save_blocks, save_nodes, save_hexs, local_save)
            # Register city that was ran
            script_run_lst.append(city)
    
    aup.log(f"--- Finished script 22. Ran {len(script_run_lst)} cities:")
    aup.log(script_run_lst)
            

##########################################################################################
# PREVIOUS CODE
##########################################################################################
# Previous way of loading pop_ageb_gdf and pop_mza_gdf
    a="""

    # Load states for current city (CVE_ENT)
    cve_ent_list = list(city_gdf.CVE_ENT.unique())

    for cve_ent in cve_ent_list:
        #Load muns in each city state
        cve_mun_list = list(city_gdf.loc[city_gdf.CVE_ENT == cve_ent].CVE_MUN.unique())

        # To avoid error that happens when there's only one MUN in State: [SQL: SELECT * FROM censo.censo_inegi_{year[2:]}_mza WHERE ("entidad" = '02') AND "mun" IN ('001',) ]
        # Duplicate mun inside tupple if there's only one MUN.
        if len(cve_mun_list) >= 2:
            cve_mun_tpl = str(tuple(cve_mun_list))
        else:
            cve_mun_list.append(cve_mun_list[0])
            cve_mun_tpl = str(tuple(cve_mun_list))
        # Load AGEBs and concat
        ageb_query = f"SELECT * FROM censoageb.censoageb_{year} WHERE (\"cve_ent\" = \'{cve_ent}\') AND \"cve_mun\" IN {cve_mun_tpl} "
        pop_ageb_gdf = pd.concat([pop_ageb_gdf,aup.gdf_from_query(ageb_query, geometry_col='geometry')])
        # Load blocks and concat
        mza_query = f"SELECT * FROM censo_mza.censo_mza_{year} WHERE (\"CVE_ENT\" = \'{cve_ent}\') AND \"CVE_MUN\" IN {cve_mun_tpl} "
        pop_mza_gdf = pd.concat([pop_mza_gdf,aup.gdf_from_query(mza_query, geometry_col='geometry')])
    
    """
            
