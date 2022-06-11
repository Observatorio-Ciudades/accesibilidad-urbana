import os
import sys

import pandas as pd
import geopandas as gpd

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup

def main(state_id, year, schema, save=False):

    #download block census data from database
    folder_name = 'censo_mza'
    table_name = f'censo_mza_{year}'
    query = f"SELECT * FROM {folder_name}.{table_name} WHERE \"CVEGEO\" LIKE \'{state_id}%%\'"
    block_pop = aup.gdf_from_query(query, geometry_col='geometry')

    aup.log(f"Downloaded block census data for {len(block_pop)} blocks in state {state_id}")

    block_pop = block_pop.to_crs("EPSG:6372") #transform block crs
    block_pop.columns = block_pop.columns.str.lower()

    #extract point from polygon
    block_pop = block_pop.set_index('cvegeo')
    point_within_polygon = gpd.GeoDataFrame(geometry=block_pop.representative_point())
    centroid_block_pop = point_within_polygon.merge(block_pop, 
    right_index=True, left_index=True) #add census data to points
    centroid_block_pop.drop(columns=['geometry_y'], inplace=True)
    centroid_block_pop.rename(columns={'geometry_x':'geometry'}, inplace=True)
    centroid_block_pop = gpd.GeoDataFrame(centroid_block_pop, geometry='geometry')

    aup.log(f"Centroids with {centroid_block_pop.pobtot.sum()} " +
    f"population vs block {block_pop.pobtot.sum()}")

    #download municipalities polygon
    folder_name = 'marco'
    table_name = 'mpos_2020'
    query = f"SELECT * FROM {folder_name}.{table_name} WHERE \"CVEGEO\" LIKE \'{state_id}%%\'"
    mun_gdf = aup.gdf_from_query(query, geometry_col='geometry')

    aup.log("Downloaded municipalities boundaries")

    #delete filler columns
    dlt_col = {2020:['cve_ent','cve_mun','cve_loc','cve_ageb',
        'cve_mza','ambito','tipomza','entidad',
        'nom_ent','mun','nom_mun','loc','ageb','mza'],
        2010:['codigo','geografico',
                'fechaact','geometria','institucio',
                'oid','entidad','nom_ent','mun','nom_mun',
                'loc','nom_loc','ageb','mza']}
    
    centroid_block_pop.drop(columns=dlt_col[year], inplace=True)

    centroid_block_pop = centroid_block_pop.to_crs("EPSG:4326")
    centroid_block_pop = centroid_block_pop.reset_index()

    #generate hexagon gdf

    res_list = [8,9]

    for res in res_list:

        hex_gdf = aup.create_hexgrid(mun_gdf, res)
        hex_gdf = hex_gdf.set_crs("EPSG:4326")

        aup.log(f"Created hex_grid with {res} resolution")

        #group census data

        #column variables for grouping analysis
        string_columns = ['cvegeo']
        avg_column = [
                    "prom_hnv",
                    "graproes",
                    "graproes_f",
                    "graproes_m",
                    "prom_ocup",
                    "pro_ocup_c",
                ]

        wgt_dict = { "prom_hnv":'pobtot',
                    "graproes":'pobtot',
                    "graproes_f":'pobfem',
                    "graproes_m":'pobmas',
                    "prom_ocup":'tvivparhab',
                    "pro_ocup_c":'tvivparhab'
                }

        #group centroids by hexagon
        hex_socio_df = aup.socio_points_to_polygon(
            hex_gdf, centroid_block_pop,f'hex_id_{res}',
            string_columns, wgt_dict=wgt_dict, avg_column=avg_column)

        aup.log("Agregated socio data to hex with a total " +
        f"of {hex_socio_df.pobtot.sum()} population " + 
        f"for resolution {res}")

        #hexagons to GeoDataFrame
        hex_gdf_socio = hex_gdf.merge(hex_socio_df, on=f'hex_id_{res}')

        hectares = hex_gdf_socio.to_crs("EPSG:6372").area / 10000

        hex_gdf_socio['dens_pob_ha'] = hex_gdf_socio['pobtot'] / hectares

        aup.log(f"Calculated an average density of {hex_gdf_socio.dens_pob_ha.mean()}")

        if save:
            aup.gdf_to_db_slow(hex_gdf_socio, f'hex_censo_mza_{year}_res{res}',
                    schema=schema, if_exists='append')

    if save:
        aup.gdf_to_db_slow(centroid_block_pop, f'censo_mza_centroid_{year}',
                    schema=schema, if_exists='append')


if __name__ == "__main__":

    SCHEMA = 'censo'
    years = [2010, 2020]

    for year in years:

        for i in range(1,33):

            state_id = f"{i:02d}"

            aup.log(f"Starting script for {state_id} and {year}")

            main(state_id, year, SCHEMA, save=False)