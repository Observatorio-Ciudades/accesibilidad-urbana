from pystac_client import Client

import geopandas as gpd
import pandas as pd

import rasterio
from rasterio import windows
from rasterio import features
from rasterio import warp
import rasterio.mask
from rasterio.enums import Resampling

import numpy as np
from PIL import Image

import matplotlib.pyplot as plt

from shapely.geometry import Point

import signal
import time

from tqdm import tqdm

import shutil

import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup

np.seterr(divide='ignore', invalid='ignore')

from contextlib import contextmanager

class TimeoutException(Exception): pass

@contextmanager
def time_limit(seconds):
        def signal_handler(signum, frame):
            raise TimeoutException("Timed out!")
        signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            signal.alarm(0)


def main(index_analysis, city, cvegeo_list, band_name_list, time_range, save=False):

    ###############################
    # Download hex polygons with AGEB data
    hex_ageb = gpd.GeoDataFrame()

    schema_hex = 'censo'
    folder_hex = 'hex_bins_pop_2020'

    for m in cvegeo_list:
        query = f"SELECT hex_id_8,geometry FROM {schema_hex}.{folder_hex} WHERE \"CVEGEO\" LIKE \'{m}%%\'"
        hex_ageb = pd.concat([hex_ageb, 
                            aup.gdf_from_query(query, geometry_col='geometry')], 
                            ignore_index = True, axis = 0)

    aup.log(f'Downloaded {len(hex_ageb)} hexagon features')

    ###############################
    # Create at different resolutions
    hex_gdf = hex_ageb.copy()
    hex_gdf.rename(columns={'hex_id_8':'hex_id'}, inplace=True)
    hex_gdf['res'] = 8

    for r in range(9,12):
        
        hex_tmp = aup.create_hexgrid(hex_ageb, r)
        hex_tmp.rename(columns={f'hex_id_{r}':'hex_id'}, inplace=True)
        hex_tmp['res'] = r
        
        hex_gdf = pd.concat([hex_gdf, hex_tmp], 
            ignore_index = True, axis = 0)
        
        del hex_tmp

    aup.log('Created hex data at different resolutions')

    ###############################
    # Gather links for raster data

    # Reads mun_gdf GeoDataFrame as polygon
    poly = hex_gdf.loc[hex_gdf.res==8].geometry
    # Extracts coordinates from polygon as DataFrame
    coord_val = poly.bounds
    # Gets coordinates for bounding box
    n = coord_val.maxy.max()
    s = coord_val.miny.min()
    e = coord_val.maxx.max()
    w = coord_val.minx.min()

    area_of_interest = {
        "type": "Polygon",
        "coordinates": [
            [
                [e, s],
                [w, s],
                [w, n],
                [e, n],
                [e, s],
            ]
        ],
    }

    # time sets to fetch data
    time_of_interest = ["2020-01-01/2020-03-31","2020-04-01/2020-06-30",
                        "2020-07-01/2020-09-30","2020-10-01/2020-12-31",
                        "2021-01-01/2021-03-31","2021-04-01/2021-06-30",
                        "2021-07-01/2021-09-30","2021-10-01/2021-12-31",
                        "2022-01-01/2022-03-31","2022-04-01/2022-06-30",
                        "2022-07-01/2022-09-30","2022-10-01/2022-12-31",
                    ]

    catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

    items = []

    for t in time_of_interest:
        search = catalog.search(
            collections=["sentinel-2-l2a"],
            intersects=area_of_interest,
            datetime=t,
            query={"eo:cloud_cover": {"lt": 10}},
        )

        # Check how many items were returned
        items.extend(list(search.get_items()))

    aup.log(f"Returned {len(items)} Items")

    def gather_links():
            # gather links
        assets_hrefs = aup.link_dict(band_name_list, items)
        # filters for dates with full data
        assets_hrefs, max_links = aup.filter_links(assets_hrefs, band_name_list)
        aup.log(f'{max_links} rasters by time analysis')
        # create complete dates DataFrame
        df_complete_dates, missing_months = aup.df_date_links(assets_hrefs, "2020-01-01", time_range)
        return df_complete_dates,missing_months

    df_complete_dates,missing_months = gather_links()
    aup.log(f'Created DataFrame with {missing_months} missing months')

    ###############################
    # Download raster data

    df_len = df_complete_dates.copy()

    aup.log('\n Starting raster analysis')

    for i in tqdm(range(len(df_len)), position=0, leave=True):
        
        if type(df_len.iloc[i].nir)!=list:
            continueinterations
            
        # gather month and year from df to save ndmi
        month_ = df_complete_dates.iloc[i]['month']
        year_ = df_complete_dates.iloc[i]['year']
        
        if f'{city}_{index_analysis}_{month_}_{year_}.tif' in os.listdir(tmp_dir):
            continue
            
        def mosaic_process(df_complete_dates):
            mosaic_red, _,_ = aup.mosaic_raster(df_complete_dates.iloc[i].red)
            aup.log('Finished processing red')
            mosaic_nir, out_trans_nir, out_meta = aup.mosaic_raster(df_complete_dates.iloc[i].nir)
            aup.log('Finished processing nir')
            return mosaic_red, mosaic_nir, out_trans_nir,out_meta
        
        def second_attempt():
            df_complete_dates,_ = gather_links()
            mosaic_red, mosaic_nir, out_trans_nir,out_meta = mosaic_process(df_complete_dates)
            return df_complete_dates, mosaic_red, mosaic_nir, out_trans_nir,out_meta
            
        # mosaic by raster band
        aup.log(f'\n Starting new analysis for {month_}/{year_}')
        try:
            with time_limit(900):
                mosaic_red, mosaic_nir, out_trans_nir,out_meta = mosaic_process(df_complete_dates)
            
        except Exception as e:
            aup.log(e)
            aup.log(f'Fetching new url for: {month_}/{year_}')
            
            df_complete_dates, mosaic_red, mosaic_nir, out_trans_nir,out_meta = second_attempt()
        
        mosaic_red = mosaic_red.astype('float32')
        mosaic_nir = mosaic_nir.astype('float32')
        aup.log('Transformed red and nir to float')
        aup.log(f'array datatype: {mosaic_red.dtype}')
        
        ndvi = (mosaic_nir-mosaic_red)/(mosaic_nir+mosaic_red)
        aup.log('Calculated ndvi')
        
        out_meta.update({"driver": "GTiff",
                    "dtype": 'float32',
                    "height": ndvi.shape[1],
                    "width": ndvi.shape[2],
                    "transform": out_trans_nir})

        
        
        aup.log('Starting save')

        with rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif", "w", **out_meta) as dest:
            dest.write(ndvi)

            dest.close()
        
        aup.log('Finished saving')
        del mosaic_red
        del mosaic_nir
        del ndvi

    ###############################
    # Raster data to hex

    raster_file = ''
    raster_dir = tmp_dir
    upload_chunk = 150000

    def interpolate_raster_data(data, index_analysis=index_analysis):
        return data[index_analysis].interpolate()

    aup.log('Starting raster data to hex')

    for r in range(8,12):
        # group raster by hex
        hex_raster = aup.raster_to_hex(hex_gdf, df_complete_dates, r, 
        index_analysis, city, raster_dir)
        aup.log('Assigned raster data to hexagons')
        # interpolate data
        hex_raster_inter = hex_raster.groupby('hex_id').apply(interpolate_raster_data)
        # data treatment for interpolation
        hex_raster_inter = hex_raster_inter.reset_index().merge(hex_raster[['month','year','geometry']].reset_index(), 
                                                            left_on='level_1', right_on='index')
        hex_raster_inter.drop(columns=['level_1','index'], inplace=True)

        aup.log('Interpolated missing months')
        
        # summary statistics
        hex_raster_analysis = hex_gdf.loc[hex_gdf['res']==r,['hex_id','geometry','res']].drop_duplicates().copy()
        
        hex_group_data = hex_raster_inter[['hex_id',index_analysis]].groupby('hex_id').agg(['mean','max','min'])
        hex_group_data.columns = ['_'.join(col) for col in hex_group_data.columns]
        
        hex_raster_analysis = hex_raster_analysis.merge(hex_group_data.reset_index(), on='hex_id')
        hex_raster_analysis[index_analysis+'_diff'] = hex_raster_analysis[index_analysis+'_max'] - hex_raster_analysis[index_analysis+'_min']

        aup.log('Created analysis GeoDataFrame')
        
        # remove geometry information
        hex_raster_inter = hex_raster_inter.drop(columns=['geometry'])
        hex_raster_inter['res'] = r
        
        # add city information
        hex_raster_inter['city'] = city
        hex_raster_analysis['city'] = city
        
        # upload to database
        if save==True:

            aup.log('Starting upload')

            if r == 8:
            
                aup.df_to_db_slow(hex_raster_inter, f'{index_analysis}_complete_dataset_hex',
                                'raster_analysis', if_exists='append', chunksize=upload_chunk)
                
                aup.gdf_to_db_slow(hex_raster_analysis, f'{index_analysis}_analysis_hex',
                                'raster_analysis', if_exists='append')

            else:
                aup.df_to_db(hex_raster_inter,f'{index_analysis}_complete_dataset_hex',
                                'raster_analysis', if_exists='append')
                aup.gdf_to_db_slow(hex_raster_analysis, f'{index_analysis}_analysis_hex',
                                'raster_analysis', if_exists='append')
            aup.log(f'Finished uploading data for res{r}')
        
        # delete variables
        
        del hex_raster
        del hex_raster_inter
        del hex_group_data
        del hex_raster_analysis
    
    # delete raster files
    for filename in os.listdir(tmp_dir):
        file_path = os.path.join(tmp_dir, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            aup.log('Failed to delete %s. Reason: %s' % (file_path, e))

if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')
    band_name_list = ['red','nir']
    index_analysis = 'ndvi'
    tmp_dir = f'../data/processed/tmp_{index_analysis}/'
    time_range = 36
    save = True

    gdf_mun = aup.gdf_from_db('metro_gdf', 'metropolis')

    # prevent cities being analyzed to times in case of a crash
    processed_city_list = []
    try:
        processed_city_list = aup.gdf_from_db('raster_analysis', 'ndvi_analysis_hex')
        processed_city_list = list(processed_city_list.city.unique())
    except:
        pass

    for city in gdf_mun.city.unique():

        aup.log(f'\n Starting city {city}')

        if city not in processed_city_list:

            cvegeo_list = list(gdf_mun.loc[gdf_mun.city==city]["CVEGEO"].unique())

            main(index_analysis, city, cvegeo_list, band_name_list, time_range, save)
            # ndvi_analysis(schema, folder_sufix, year, amenities, save = save)