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
    [The script 22 has already been updated with the code of this Script, and it's no longer necessary to run both].
    For each city in Mexico's metropolis list, this Script adds missing hexs to the hexgrid already uploaded in Script 22-censo_pop_voronoi.py.
    (Substituted by Script 22's Step 4.4 --------------- ADD MISSING HEXS).
"""

def main(city,save=False,local_save=True):

    # 1.0 --------------- LOAD CURRENT DATA
    # Load current data for city
    save_schema = 'censo'
    hexs_save_table = f'pobcenso_inegi_20_mzaageb_hex'
    query = f"SELECT * FROM {save_schema}.{hexs_save_table} WHERE \"city\" LIKE \'{city}\'"
    current_data = aup.gdf_from_query(query, geometry_col='geometry')
    aup.log(f"--- Loaded {city}'s current data.")

    # For each res:
    all_missing_hexs = gpd.GeoDataFrame()
    for res in res_list:

        # 1.1 --------------- LOAD CURRENT DATA, CURRENT RES
        current_data_res = current_data.loc[current_data['res']==res].copy()

        # 1.2 --------------- LOAD BASE HEXGRID
        # Load hexgrid from db
        hex_query = f"SELECT * FROM hexgrid.hexgrid_{res}_city_2020 WHERE \"city\" LIKE \'{city}\'"
        hex_res_gdf = aup.gdf_from_query(hex_query, geometry_col='geometry')
        hex_res_gdf = hex_res_gdf.set_crs("EPSG:4326")
        # Format - Remove res from index name and add column with res
        hex_res_gdf.rename(columns={f'hex_id_{res}':'hex_id'},inplace=True)
        hex_res_gdf['res'] = res
        aup.log(f"--- Loaded {city}'s base res {res} hexgrid.")

        # 1.3 --------------- FIND MISSING HEXs AND FILL POP DATA
        # List all currently added hex_ids
        current_res_hex_ids = list(current_data_res.hex_id.unique())
        # Identify all current res's urban hex_ids
        urban_res_hexs = hex_res_gdf.loc[hex_res_gdf['type']=='urban'].copy()
        urban_res_hexs_lst = list(urban_res_hexs.hex_id.unique())
        # Isolate missing urban hex_ids
        missing_hexs = list(set(urban_res_hexs_lst) - set(current_res_hex_ids))
        # Create a GeoDataFrame with missing hexs
        missing_hexs_gdf = hex_res_gdf.loc[hex_res_gdf.hex_id.isin(missing_hexs)].copy()
        # Add missing pop data to missing hexs
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
        for col in columns_of_interest:
            missing_hexs_gdf[col] = 0
        missing_hexs_gdf['dens_pob_ha'] = 0
        all_missing_hexs = pd.concat([all_missing_hexs,missing_hexs_gdf])
        aup.log(f"--- Added {len(missing_hexs_gdf)} urban hexs res {res}.")
    # Final format
    aup.log(f"--- Adding a total of {len(all_missing_hexs)} urban hexs.")
    all_missing_hexs.columns = all_missing_hexs.columns.str.lower()

    # 2.0 --------------- SAVE DATA
    # Saving locally
    if local_save:
        aup.log(f"--- Saving {city}'s missing hexs locally.")
        all_missing_hexs.to_file(local_save_dir + f"script22b_{city}_missing_urbanhexs.gpkg", driver='GPKG')
    # Save to database
    if save and len(all_missing_hexs)>0:
        aup.log(f"--- Saving {city}'s missing hexs to database.")
        # Saving hexs to database
        limit_len = 10000
        if len(all_missing_hexs)>limit_len:
            c_upload = len(all_missing_hexs)/limit_len
            for k in range(int(c_upload)+1):
                aup.log(f"--- Uploading missing hexs - Starting range k = {k} of {int(c_upload)}")
                gdf_inter_upload = all_missing_hexs.iloc[int(limit_len*k):int(limit_len*(1+k))].copy()
                aup.gdf_to_db_slow(gdf_inter_upload, hexs_save_table, save_schema, if_exists='append')
            aup.log(f"--- Uploaded missing hexs for {city}.")
        else:
            aup.gdf_to_db_slow(all_missing_hexs, hexs_save_table, save_schema, if_exists='append')
            aup.log(f"--- Uploaded missing hexs for {city}.")
    
    aup.log(f"--- Finished main function for {city}.")

if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('--- STARTING SCRIPT 22b.')

    test = False #True runs Aguascalientes and saves locally. False runs all cities and saves to database

    # ------------------------------ BASE DATA REQUIRED ------------------------------
    metro_schema = 'metropolis'
    metro_table = 'metro_gdf_2020'
    res_list = [8,9,10]
    local_save_dir = f"../data/processed/pop_data/"

    # ------------------------------ MAIN FUNCTION START ------------------------------
    # Load all cities' municipalities
    metro_query = f"SELECT * FROM {metro_schema}.{metro_table}"
    metro_gdf = aup.gdf_from_query(metro_query, geometry_col='geometry')
    metro_gdf = metro_gdf.set_crs("EPSG:4326")
    # Create a city list (All cities)
    city_list = list(metro_gdf.city.unique())
    k = len(city_list)
    aup.log(f'--- Loaded city list with {k} cities.')

    if test:
        main('Aguascalientes', save=False, local_save=True)
    else:
        i = 0
        for city in city_list:
            aup.log("--"*40)
            i+=1
            aup.log(f"--- Starting city {i}/{k}: {city}")
            main(city, save=True, local_save=False)
