import os
import sys

import pandas as pd
import geopandas as gpd

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


"""
    For each city in Mexico's metropolis list, the script loads the municipalities, AGEBs and hexgrid from the database,
    loads a previously uploaded to database OSMnx network, transfers censo data from AGEBs to nodes (socio_polygon_to_points),
    and then from nodes to hexgrid (socio_points_to_polygon). The result can be saved locally or to the database.

    The Script was updated on 2024 05 21 in order to:
    1. Process all metropolitan areas (INEGI, 2020)
    2. Allow processing and saving different resolutions per city
    3. Rename output tables
    The Script was updated on 2025 02 13 in order to:
    1. Adjust input to new re-updated census data (censo_inegi_{year[2:]}_ageb)
    2. Adjust input to downloaded december 2020 network data (nodes_osmnx_20_point, edges_osmnx_20_line) 
       [Some 2020 cities were missing from osmnx.nodes and osmnx.edges]
    3. 2010 and 2020 were run 

    This updated script produced the following tables:
    censo/censo_inegi_10_ageb_node
    censo/censo_inegi_10_ageb_hex (res 8 and 9)
    censo/censo_inegi_20_ageb_node
    censo/censo_inegi_20_ageb_hex (res 8 and 9)
"""

def main(year, res_list=[8], db_save=False, local_save=False):
    
    ##########################################################################################
	# STEP 1: LOAD DATA
    aup.log("--"*30)
    aup.log(f"--- {city}({year}) - LOADING INPUT DATA.")

    # 1.1 --------------- LOAD POP DATA (AGEBs) FROM THE CURRENT CITY'S (MUN_GDF) MUNICIPALITIES
    aup.log("--- Loading AGEBs for area of interest.")
    ageb_gdf = gpd.GeoDataFrame()
    # Load states (CVE_ENT) for current city
    cve_ent_list = list(mun_gdf.CVE_ENT.unique())
    for cve_ent in cve_ent_list:
        # Load muns (CVE_MUN) for each state
        cve_mun_list = list(mun_gdf.loc[mun_gdf.CVE_ENT == cve_ent].CVE_MUN.unique())
        # To avoid error that happens when there's only one MUN in State: [SQL: SELECT * FROM censo_mza.censo_mza_2020 WHERE ("CVE_ENT" = '02') AND "CVE_MUN" IN ('001',) ]
        # Duplicate mun inside tupple if there's only one MUN.
        if len(cve_mun_list) >= 2:
            cve_mun_tpl = str(tuple(cve_mun_list))
        else:
            cve_mun_list.append(cve_mun_list[0])
            cve_mun_tpl = str(tuple(cve_mun_list))
        # Load AGEBs for each mun of each state and concat
        query = f"SELECT * FROM {ageb_schema}.{ageb_table} WHERE (\"cve_ent\" = \'{cve_ent}\') AND \"cve_mun\" IN {cve_mun_tpl} "
        ageb_gdf = pd.concat([ageb_gdf,aup.gdf_from_query(query, geometry_col='geometry')])
    # Set CRS
    ageb_gdf = ageb_gdf.set_crs("EPSG:4326")
    aup.log(f"--- Loaded a total of {ageb_gdf.pobtot.sum()} people in AGEBs.")

    # 1.2 --------------- LOAD NETWORK (nodes)
    # Load network
    if year == '2010':
        _, nodes, _ = aup.graph_from_hippo(mun_gdf, 'networks', edges_folder='edges_2011', nodes_folder='nodes_2011')
        # Drop unncessary columns from nodes column (only present in 2010, vialidades 2011) [Keeps 'osmid','x','y', and 'geometry']
        nodes.drop(['ID', 'TIPOVIA', 'TIPO', 'NUMERO', 'DERE_TRAN', 'ADMINISTRA', 'NUME_CARR', 'CONDICION', 
        'ORIGEN', 'CALI_REPR', 'CVEGEO', 'NOMVIAL', 'SENTIDO', 'LONGITUD', 'UNIDAD', 'vertex_pos', 
        'vertex_ind', 'vertex_par', 'vertex_p_1','distance', 'angle'], inplace = True, axis=1)
    elif year == '2020': #[Should have 'osmid','x','y','street_count','city', and 'geometry']
        _, nodes, _ = aup.graph_from_hippo(mun_gdf, network_schema, edges_folder=edges_table, nodes_folder=nodes_table)
    aup.log(f"--- Downloaded {len(nodes)} nodes from database for {city}")

    # Network format - Set CRS
    nodes = nodes.to_crs("EPSG:4326")
    # Network format - If using network that has 'city' on it, drop.
    if 'city' in nodes.columns:
        nodes.drop(columns=['city'],inplace=True)
    # Network format - If using a network that has 'street_count' as str, convert to int.
    if 'street_count' in nodes.columns:
        nodes_pop['street_count'] = nodes_pop['street_count'].astype(int)


    ##########################################################################################
	# STEP 2: DISTRIBUTE POP AGEB DATA TO NODES
    aup.log("--"*30)
    aup.log(f"--- {city}({year}) - DISTRIBUTING AGEB DATA TO NODES.")

    # 2.1 --------------- Set average columns and column positions for each year
    # Set columns that won't be divided by nodes
    avg_column = ["prom_hnv", "graproes", "graproes_f", 
                  "graproes_m", "prom_ocup", "pro_ocup_c"]
    # Set column positions where numeric data starts in censoageb_{year} gdf
    if year == '2010':
        column_start = 3 #(16 in deprecated censoageb_2010)
        column_end = -11 #(-1 in deprecated censoageb_2010)
    elif year == '2020':
        column_start = 3 #(14 in deprecated censoageb_2020)
        column_end = -11 #(-2 in deprecated censoageb_2020)
    
    # 2.2 --------------- Run socio_polygon_to_points()
    nodes_pop = aup.socio_polygon_to_points(nodes, ageb_gdf, column_start=column_start, column_end=column_end, 
                                            cve_column='cvegeo_ageb', avg_column=avg_column) #(cve_column='cve_geo' in deprecated censoageb_2010 and censoageb_2020)
    aup.log(f"--- Added a total of {nodes_pop.pobtot.sum()} persons to nodes.")

    # 2.3 --------------- Format and save nodes_pop data
    # Final format
    nodes_pop['city'] = city
    # Local save
    if local_save:
        aup.log(f"--- Saving {city}'s nodes pop data locally.")
        nodes_pop.to_file(local_save_dir + f"script03_{year}{city}_nodes.gpkg", driver='GPKG')
    # Database save
    if db_save:
        # LOG CODE - Progress
        uploaded_nodes = 0
        # Divide nodes by batches of 10000 for upload
        c_nodes = len(nodes_pop) / 10000
        for cont in range(int(c_nodes)+1):
            # Upload node batch
            nodes_pop_upload = nodes_pop.iloc[int(10000*cont):int(10000*(cont+1))].copy()
            aup.gdf_to_db_slow(nodes_pop_upload, nodes_save_table, schema=save_schema, if_exists="append")
            # LOG CODE - Print progress
            uploaded_nodes = uploaded_nodes + len(nodes_pop_upload)
            aup.log(f"--- Uploaded {uploaded_nodes} nodes into DB out of {len(nodes_pop)}.")

    ##########################################################################################
	# STEP 3: DISTRIBUTE NODES POP DATA TO HEXGRID
    aup.log("--"*30)
    aup.log(f"--- {city}({year}) - DISTRIBUTING NODES DATA TO HEXS.")

    # 3.0 --------------- Prepare nodes_pop data for socio_points_to_polygon()
    # Drop "city" column (It is on hexs data)
    if 'city' in nodes_pop.columns:
        nodes_pop.drop(columns=['city'],inplace=True)
    # Define numeric values that are weighted by population
    wgt_dict = {'prom_hnv':'pobtot', 
                'graproes':'pobtot',
                'graproes_f':'pobfem', 
                'graproes_m':'pobmas',
                'prom_ocup':'pobtot',
                'pro_ocup_c':'pobtot'}
    for res in res_list:
        # 3.1 --------------- Load res hexgrid and prepare for socio_points_to_polygon()
        # Load hexgrid 
        hex_table = f'hexgrid_{res}_city_2020'
        query = f"SELECT * FROM {hex_schema}.{hex_table} WHERE \"city\" LIKE \'{city}\'"
        hex_bins = aup.gdf_from_query(query, geometry_col='geometry')
        # Set CRS
        hex_bins = hex_bins.set_crs("EPSG:4326")
        aup.log(f"--- Loaded hexgrid res {res}.")
        # Define string columns unique to either 2020 or 2010
        censo_columns = ['nom_ent','nom_mun','nom_loc',
                         'cve_ent','cve_mun','cve_loc','cve_ageb','cve_mza',
                         'cvegeo_mun','cvegeo_loc','cvegeo_ageb','cvegeo_mza']
        if year == '2020':
            string_columns = censo_columns + [f'hex_id_{res}', 'x', 'y', 'street_count']
            # Deprecated (censoageb_2020)
            #string_columns = ['cve_geo','cve_ent','cve_mun','cve_loc','cve_ageb',
            #'entidad','nom_ent','mun','nom_mun','loc','nom_loc','ageb',
            #'mza','cve_geo_ageb',f'hex_id_{res}', 'x', 'y', 'street_count']
        elif year == '2010':
            string_columns = censo_columns + [f'hex_id_{res}', 'x', 'y']
            # Deprecated (censoageb_2010)
            #string_columns = [
            #'censo', 'cve_ent', 'nom_ent', 'cve_mun', 'nom_mun',
            #'cve_loc', 'cve_ageb', 'cve_cd',
            #f'hex_id_{res}','x', 'y', 'codigo', 'cve_geo', 'geog', 
            #'fecha_act', 'geom', 'institut', 'OID']

        # 3.2 --------------- Run socio_points_to_polygon()
        hex_pop = aup.socio_points_to_polygon(hex_bins, nodes_pop, f'hex_id_{res}', string_columns, wgt_dict=wgt_dict, avg_column=avg_column)
        hex_upload = hex_bins.merge(hex_pop, on= f'hex_id_{res}')
        aup.log(f"--- Added a total of {hex_upload.pobtot.sum()} persons to hexs res {res}.")

        # 3.3 --------------- Format hexs data
        # Format - Remove res from index name and add column with res
        hex_upload.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
        hex_upload['res'] = res
        # Format - Set all columns to .lower
        hex_upload.columns = [col.lower() for col in hex_upload.columns]
        # Format - Reorder columns to place hex_id and res first
        column_list = list(hex_upload.columns)
        column_list.remove('hex_id')
        column_list.remove('res')
        hex_upload = hex_upload[['hex_id','res']+column_list]
        aup.log(f"--- Formated hexgrid res {res} for upload.")

        # 3.4 --------------- Save hexs data
        # Local save
        if local_save:
            aup.log(f"--- Saving {city}'s hexs pop data locally.")
            hex_upload.to_file(local_save_dir + f"script03_{year}{city}_hex{res}.gpkg", driver='GPKG')
        # Database save
        if db_save:
            aup.gdf_to_db_slow(hex_upload, hex_save_table, schema=save_schema, if_exists="append")
            aup.log(f"--- Uploaded pop hexgrid res {res} for city {city}.")


if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('\n Starting script 03.')

    # ------------------------------ SCRIPT CONFIGURATION - YEAR OF CENSUS INPUT DATA ------------------------------
    # Year of data
    year = '2010'
    
    # ------------------------------ SCRIPT CONFIGURATION - DATABASE SCHEMAS AND TABLES ------------------------------
    # City data
    metro_schema = 'metropolis' # Mexico's analysis: 'metropolis'
    metro_table = 'metro_gdf_2020' # Mexico's analysis: 'metro_gdf_2020' (deprecated: 'metro_gdf_2015')
    # AGEB data
    ageb_schema = 'censo' # Mexico's analysis: 'censo' (deprecated: 'censoageb')
    ageb_table = f'censo_inegi_{year[2:]}_ageb' # Mexico's analysis: 'censo_{year[2:]}_ageb' (deprecated: 'censoageb_2010' or 'censoageb_2020')
    # Network data 
    # --> Except 2010, 2010 will use predefined inside Main Function
    network_schema = 'osmnx' # Mexico's analysis: 'osmnx'
    nodes_table = 'nodes_osmnx_20_point' # Mexico's analysis: (deprecated: 'nodes')
    edges_table = 'edges_osmnx_20_line' # Mexico's analysis: (deprecated: 'edges')
    # Hexgrid data
    hex_schema = 'hexgrid' # Mexico's analysis: 'hexgrid'
    # VERIFY INSIDE MAIN FUNCTION hex_table, created for each required hex resolution.
    # Mexico's data depends on res ['hexgrid_{res}_city_2020' (deprecated: 'hexgrid_{res}_city')], 

    # ------------------------------ SCRIPT CONFIGURATION - OUTPUT ------------------------------
    # Resolution of output hexgrid
    res_list = [8,9]
    # Save output locally?
    local_save = False
    local_save_dir = f"../data/scripts_output/script_03/"
    # Save output to db?
    db_save = False
    save_schema = 'censo' # Mexico's analysis:'censo'
    nodes_save_table = f'censo_inegi_{year[2:]}_ageb_node' # Mexico's analysis: f'censo_inegi_{year[2:]}_ageb_node' (deprecated:'nodes_pop_{year}')
    hex_save_table = f'censo_inegi_{year[2:]}_ageb_hex' # Mexico's analysis: f'censo_inegi_{year[2:]}_ageb_hex' (deprecated:'hexbins_pop_{year}')
    # Test (if test, runs a specific city list only and saves locally only, overriding 'save' and 'local_save' vars.)
    test = False
    test_city_list = ['Aguascalientes']

    # ------------------------------ SCRIPT START - NOT CONFIGURATION ------------------------------
    if test:
        missing_cities_list = test_city_list
        i = 0
        k = len(missing_cities_list)
        aup.log(f'--- Test mode, one city. Already processed ({i}/{k}) cities.')
        aup.log(f"--- Processing test for: {missing_cities_list}.")
        db_save = False
        local_save = True
    else:
        # Load all available cities
        aup.log("--- Reading available cities.")
        query = f"SELECT city FROM {metro_schema}.{metro_table}"
        metro_df = aup.df_from_query(query)
        city_list = list(metro_df.city.unique())
        k = len(city_list)
        aup.log(f"--- Loaded city list with {k} cities.")

        # Prevent cities being analyzed several times in case of a crash
        aup.log("--- Checking for previously analysed cities.")
        processed_city_list = []
        try:
            query = f"SELECT city FROM {save_schema}.{hex_save_table}"
            processed_city_list = aup.df_from_query(query)
            processed_city_list = list(processed_city_list.city.unique())
        except:
            pass
        i = len(processed_city_list)

        # Missing cities (to be analysed)
        missing_cities_list = []
        for city in city_list:
            if city not in processed_city_list:
                missing_cities_list.append(city)
        
        aup.log(f'--- Already processed ({i}/{k}) cities.')
        aup.log(f'--- Missing procesing for cities: {missing_cities_list}')

    # Create mun_gdf for each city and run main function
    for city in missing_cities_list:
        i = i + 1
        aup.log("--"*40)
        aup.log(f"--- Running script for city {i}/{k}:{city}.")

        # Loads current city
        query = f"SELECT * FROM {metro_schema}.{metro_table} WHERE \"city\" LIKE \'{city}\'"
        mun_gdf = aup.gdf_from_query(query, geometry_col='geometry')

        #Define projections for municipalities and hexgrids
        mun_gdf = mun_gdf.set_crs("EPSG:4326")
        aup.log(f"--- Loaded municipalities (mun_gdf).")

        main(year, res_list=res_list, db_save=db_save, local_save=local_save)