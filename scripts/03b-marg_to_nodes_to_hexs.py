import os
import sys

import pandas as pd
import geopandas as gpd

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


def main(year, res_list=[8], db_save=False, local_save=False):
    

    ##########################################################################################
	# STEP 1: LOAD AND MERGE AGEBs (with cvecols) WITH AGEBs (with MARGINALISATION) DATA
    aup.log("--"*30)
    aup.log(f"--- {city}({year}) - MERGING AGEBs with cvecols with AGEBs with MARGINALISATION data.")

    # 1.1 --------------- Load pop data (AGEBs) from the current city's (mun_gdf) municipalities
    aup.log("--- Loading AGEBs for area of interest.")
    pop_ageb_gdf = gpd.GeoDataFrame()
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
        pop_ageb_gdf = pd.concat([pop_ageb_gdf,aup.gdf_from_query(query, geometry_col='geometry')])
    # Set CRS
    pop_ageb_gdf = pop_ageb_gdf.set_crs("EPSG:4326")
    # Filter for columns of interest
    pop_ageb_gdf = pop_ageb_gdf[['cvegeo_mun','cvegeo_loc','cvegeo_ageb', #cvegeo cols (IDs)
                                 'cve_ent','nom_ent','cve_mun','nom_mun','cve_loc','nom_loc','cve_ageb', #cve and nom cols (IDs)
                                 'geometry']] #geometry col
    aup.log(f"--- Loaded {len(pop_ageb_gdf)} AGEBs for {city}.")

    # Additional step (Save nom_ent corresponding to each cve_ent available for later use)
    nom_ent_dict = dict(zip(pop_ageb_gdf.cve_ent, pop_ageb_gdf.nom_ent))

    # 1.2 --------------- Load marg. data (AGEBs) from CONAPO
    aup.log("--- Loading marg. data.")
    # List of all unique cvegeo_agebs (unique AGEB identifier) in current city
    cvegeo_ageb_list = list(pop_ageb_gdf.cvegeo_ageb.unique())
    # Load marg. data
    marg_query = f"SELECT * FROM {marg_schema}.{marg_table} WHERE \"cve_geo_ageb\" IN {str(tuple(cvegeo_ageb_list))}"
    marg_gdf = aup.gdf_from_query(marg_query, geometry_col='geometry')
    # Set CRS
    marg_gdf = marg_gdf.set_crs("EPSG:4326")
    # Filter for columns of interest
    if year == '2020':
        marg_df = marg_gdf[['cve_geo_ageb','pobtot','im_2020','gm_2020','imn_2020']].copy()
        del marg_gdf
    aup.log(f"--- Loaded {len(marg_df)} marg. records (AGEBs) for {city}.")

    # 1.3 --------------- Merge pop and marg. data
    aup.log("--- Merging AGEBs with marg. data.")
    # Merge and drop duplicated AGEB ID column
    marg_ageb_gdf = pop_ageb_gdf.merge(marg_df, how='inner', left_on='cvegeo_ageb', right_on='cve_geo_ageb')
    marg_ageb_gdf.drop(columns=['cve_geo_ageb'], inplace=True)
    aup.log(f"--- Merged {len(marg_ageb_gdf)} AGEBs with marg. data for {city}.")


    ##########################################################################################
    # STEP 2: LOAD NETWORK DATA AND DISTRIBUTE MARGINALISATION BY AGEB DATA TO NODES
    aup.log("--"*30)
    aup.log(f"--- {city}({year}) - DISTRIBUTING MARGINALISATION BY AGEB DATA TO NODES.")

    # 2.1 --------------- Load network data
    # Load network
    if year == '2020': #[Should have 'osmid','x','y','street_count','city', and 'geometry']
        _, nodes, _ = aup.graph_from_hippo(mun_gdf, network_schema, edges_folder=edges_table, nodes_folder=nodes_table)
    # Network format - Set CRS
    nodes = nodes.to_crs("EPSG:4326")

    nodes.to_file(local_save_dir + f"nodes_{city}_{year}.gpkg")

    # Network format - If using network that has 'city' on it, drop.
    if 'city' in nodes.columns:
        nodes.drop(columns=['city'],inplace=True)
    # Network format - If using a network that has 'street_count' as str, convert to int.
    if 'street_count' in nodes.columns:
        nodes['street_count'] = nodes['street_count'].fillna(0)
        nodes['street_count'] = nodes['street_count'].astype(int)
    aup.log(f"--- Downloaded {len(nodes)} nodes from database for {city}")

    # 2.2 --------------- Distribute marg. by ageb data to nodes
    aup.log("--- Distributing marg. by ageb data to nodes.")
    # Set columns whose data that won't be distributed by nodes [Data is the same in the polygon and in each of the nodes within it]
    no_distr_cols = ['im_2020','gm_2020','imn_2020'] #Basically, it will just divide "pobtot"
    # Set column positions where numeric data starts in censoageb_{year} gdf
    if year == '2020':
        column_start = len(pop_ageb_gdf.columns) #After merging, the first columns in marg_ageb_gdf are the ones from pop_ageb_gdf.
        column_end = len(marg_ageb_gdf.columns)-1 #After merging, the last columns in marg_ageb_gdf are the ones from marg_df.
    
    # 2.2 --------------- Run socio_polygon_to_points()
    #current_columns = list(marg_ageb_gdf.columns)
    #aup.log(f"--- Performing socio_polygon_to_points with data from col. {current_columns[column_start]} to {current_columns[column_end]}.")
    #aup.log(f"--- Performing socio_polygon_to_points not dividing data from columns: {no_distr_cols}.")
    nodes_marg = aup.socio_polygon_to_points(nodes, 
                                             marg_ageb_gdf, 
                                             column_start=column_start, 
                                             column_end=column_end, 
                                             cve_column='cvegeo_ageb', 
                                             no_distr_cols=no_distr_cols)
    nodes_marg.reset_index(inplace=True) # Restores 'osmid'
    
    # 2.3 --------------- Format and save nodes_pop data
    # Final format
    nodes_marg['city'] = city
    # Local save
    if local_save:
        aup.log(f"--- Saving marginalisation to nodes locally.")
        nodes_marg.to_file(local_save_dir + f"nodes_marg_{city}_{year}.gpkg")
    # Database save
    if db_save:
        aup.log(f"--- Saving marginalisation to nodes in database.")
        # LOG CODE - Progress
        uploaded_nodes = 0
        # Divide nodes by batches of 10000 for upload
        c_nodes = len(nodes_marg) / 10000
        for cont in range(int(c_nodes)+1):
            # Upload node batch
            nodes_marg_upload = nodes_marg.iloc[int(10000*cont):int(10000*(cont+1))].copy()
            aup.gdf_to_db_slow(nodes_marg_upload, nodes_save_table, schema=save_schema, if_exists="append")
            # LOG CODE - Print progress
            uploaded_nodes = uploaded_nodes + len(nodes_marg_upload)
            aup.log(f"--- Uploaded {uploaded_nodes} nodes into DB out of {len(nodes_marg)}.")
    

    ##########################################################################################
	# STEP 3: DISTRIBUTE NODES MARGINALISATION DATA TO HEXGRID
    aup.log("--"*30)
    aup.log(f"--- {city}({year}) - DISTRIBUTING MARGINALISATION IN NODES DATA TO HEXS.")

    # 3.0 --------------- Prepare nodes_marg data for socio_points_to_polygon()
    # Drop columns from nodes_marg that are not needed in hexs data
    nodes_marg.drop(columns=['city'],inplace=True) # "city" column is on hexs data
    nodes_marg.drop(columns=['gm_2020'],inplace=True) # "gm_2020" is a clasification of marginalisation, but must be recalculated after weighting data by population in hexs
    nodes_marg.drop(columns=['osmid'],inplace=True) # "osmid" is not needed in hexs data
    nodes_marg.drop(columns=['x'],inplace=True) # "x" is not needed in hexs data
    nodes_marg.drop(columns=['y'],inplace=True) # "y" is not needed in hexs data
    nodes_marg.drop(columns=['street_count'],inplace=True) # "street_count" is not needed in hexs data
    # socio_points_to_polygon() - Define string columns (Won't be added to hexs data)
    ID_columns = ['nom_ent','nom_mun','nom_loc',
                  'cve_ent','cve_mun','cve_loc','cve_ageb',
                  'cvegeo_mun','cvegeo_loc','cvegeo_ageb']
    # socio_points_to_polygon() - Define numeric values that are weighted by population
    avg_column = ['im_2020','imn_2020']
    wgt_dict = {'im_2020':'pobtot',
                'imn_2020':'pobtot'}
    
    for res in res_list:
        # 3.1 --------------- Load res hexgrid and prepare for socio_points_to_polygon()
        # Load hexgrid 
        hex_table = f'hexgrid_{res}_city_2020'
        query = f"SELECT * FROM {hex_schema}.{hex_table} WHERE \"city\" LIKE \'{city}\'"
        hex_bins = aup.gdf_from_query(query, geometry_col='geometry')
        # Set CRS
        hex_bins = hex_bins.set_crs("EPSG:4326")
        aup.log(f"--- Loaded hexgrid res {res}.")

        # 3.2 --------------- Run socio_points_to_polygon()
        if year == '2020':
            string_columns = ID_columns + [f'hex_id_{res}']
        # Run function
        hex_marg = aup.socio_points_to_polygon(hex_bins, 
                                               nodes_marg, 
                                               f'hex_id_{res}', 
                                               string_columns, 
                                               wgt_dict=wgt_dict, 
                                               avg_column=avg_column)
        print(hex_marg.columns)
        
        # 3.3 --------------- Add data from hex_bins to hex_marg
        hex_upload = hex_bins.merge(hex_marg, on= f'hex_id_{res}')
        print(hex_upload.columns)
        # Rename geographic data 
        hex_upload.rename(columns={'CVEGEO':'cvegeo_mun',
                                 'NOMGEO':'nom_mun'},inplace=True)
        # Add missing geographic data
        hex_upload['cve_ent'] = hex_upload.cvegeo_mun.str[:2]
        hex_upload['cve_mun'] = hex_upload.cvegeo_mun.str[2:5]
        hex_upload['nom_ent'] = hex_upload.cve_ent.map(nom_ent_dict) # Use previously created nom_ent_dict to add nom_ent to hex_bins
        aup.log(f"--- Added a total of {hex_upload.pobtot.sum()} persons to hexs res {res}.")

        # 3.4 --------------- Calculate marginalisation classification for hexs
        hex_upload.loc[hex_upload.imn_2020>=0.966338 , 'grado_marg'] = 'Muy bajo' 
        hex_upload.loc[(hex_upload.imn_2020>=0.946436 )& (hex_upload.imn_2020<0.966338), 'grado_marg'] = 'Bajo' 
        hex_upload.loc[(hex_upload.imn_2020>=0.926536)& (hex_upload.imn_2020<0.946436), 'grado_marg'] = 'Medio' 
        hex_upload.loc[(hex_upload.imn_2020>=0.8999)& (hex_upload.imn_2020<0.926536), 'grado_marg'] = 'Alto' 
        hex_upload.loc[(hex_upload.imn_2020<0.8999), 'grado_marg'] = 'Muy Alto'
        aup.log(f"--- Calculated marginalisation classification for hexs res {res}.")

        # 3.5 --------------- Format hexs data
        # Format - Remove res from index name and add column with res
        hex_upload.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
        hex_upload['res'] = res
        # Format - Set all columns to .lower
        hex_upload.columns = [col.lower() for col in hex_upload.columns]
        # Format - Reorder columns to place hex_id and res first, then ID cols, then data, then geometry.
        column_list = list(hex_upload.columns) # Current columns
        first_list = ['hex_id','res','cvegeo_mun','cve_ent','nom_ent','cve_mun','nom_mun'] # Set first columns
        column_list = [col for col in column_list if col not in first_list] # Remove first_list columns
        column_list.remove('geometry') # Remove geometry
        hex_upload = hex_upload[first_list + column_list + ['geometry']] # Reorder
        aup.log(f"--- Formated hexgrid res {res} for upload.")

        # 3.6 --------------- Save hexs data
        # Local save
        if local_save:
            aup.log(f"--- Saving {city}'s hexs pop data locally.")
            hex_upload.to_file(local_save_dir + f"hexs_marg_{year}_{city}_r{res}.gpkg", driver='GPKG')
        # Database save
        if db_save:
            aup.gdf_to_db_slow(hex_upload, hex_save_table, schema=save_schema, if_exists="append")
            aup.log(f"--- Uploaded pop hexgrid res {res} for city {city}.")



if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('\n Starting script 03b.')

    # ------------------------------ SCRIPT CONFIGURATION - YEAR OF CENSUS INPUT DATA ------------------------------
    # Year of data (Marg. currently available at AGEB level for 2020 only, in preparation for next census).
    year = '2020'

    # ------------------------------ SCRIPT CONFIGURATION - DATABASE SCHEMAS AND TABLES ------------------------------
    # City data (Since there's only network data available for metropolis, not entire country)
    metro_schema = 'metropolis'
    metro_table = 'metro_gdf_2020'
    # Marg data (CONAPO)
    # Link: https://www.gob.mx/conapo/documentos/indices-de-marginacion-2020-284372
    marg_schema = 'censoageb'
    marg_table = 'marginacion_ageb'
    # AGEB data [Using censo_inegi_20_ageb because already has verified cvegeo, cve and nom cols]
    ageb_schema = 'censo'
    ageb_table = f'censo_inegi_{year[2:]}_ageb'
    # Network data
    network_schema = 'osmnx' # Mexico's analysis: 'osmnx'
    nodes_table = f'nodes_osmnx_{year[2:]}_point' # Mexico's analysis: (deprecated: 'nodes')
    edges_table = f'edges_osmnx_{year[2:]}_line' # Mexico's analysis: (deprecated: 'edges')
    # Hexgrid data
    hex_schema = 'hexgrid' # Mexico's analysis: 'hexgrid'
    # VERIFY INSIDE MAIN FUNCTION hex_table, created for each required hex resolution.
    # Mexico's data depends on res ['hexgrid_{res}_city_2020' (deprecated: 'hexgrid_{res}_city')], 
    
    # ------------------------------ SCRIPT CONFIGURATION - OUTPUT ------------------------------
    # Resolution of output hexgrid
    res_list = [8,9]
    # Save output locally?
    local_save = False
    local_save_dir = f"../data/scripts_output/script_03b/"
    # Save output to db?
    db_save = False
    save_schema = 'censo' # Mexico's analysis:'censo'
    nodes_save_table = f'margurb_inegi_{year[2:]}_ageb_node' # (deprecated:'nodes_marg_{year}')
    hex_save_table = f'margurb_inegi_{year[2:]}_ageb_hex' # (deprecated:'hex_bins_marg_{year}')
    # Test (if test, runs a specific city list only and saves locally only, overriding 'save' and 'local_save' vars.)
    test = False
    test_city_list = ['Matamoros']

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