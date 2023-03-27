from pystac_client import Client

import geopandas as gpd
import pandas as pd

import rasterio
from rasterio import windows
from rasterio import features
from rasterio import warp
import rasterio.mask
from rasterio.enums import Resampling

from datetime import datetime
from dateutil.relativedelta import relativedelta

import numpy as np
from PIL import Image

import matplotlib.pyplot as plt

from shapely.geometry import Point

from tqdm import tqdm


import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup

np.seterr(divide='ignore', invalid='ignore')


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

    aup.log(f'Created {len(hex_gdf)} hex data at different resolutions')

    ###############################
    # Determine available links for raster data

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

    def gather_items(time_of_interest):
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
        return items

    items = gather_items(time_of_interest)

    aup.log(f"Returned {len(items)} Items")

    # gather links
    assets_hrefs = aup.link_dict(band_name_list, items)
    # filters for dates with full data
    assets_hrefs, max_links = aup.filter_links(assets_hrefs, band_name_list)
    aup.log(f'{max_links} rasters links by time analysis')
    # create complete dates DataFrame
    df_len, missing_months = aup.df_date_links(assets_hrefs, "2020-01-01", time_range)

    aup.log(f'Created DataFrame with {missing_months} missing months')

    ###############################
    # Download raster data

    aup.log('\n Starting raster analysis')

    for i in tqdm(range(len(df_len)), position=0, leave=True):
        
        checker = 0

        if df_len.iloc[i].data_id==0:
            continue
            
        # gather month and year from df to save ndmi
        month_ = df_len.iloc[i]['month']
        year_ = df_len.iloc[i]['year']
        
        if f'{city}_{index_analysis}_{month_}_{year_}.tif' in os.listdir(tmp_dir):
            continue
        
        aup.log(f'\n Starting new analysis for {month_}/{year_}')
        
        # gather links for raster images
        sample_date = datetime(year_, month_, 1)
        first_day = sample_date + relativedelta(day=1)
        last_day = sample_date + relativedelta(day=31)
        
        time_of_interest = [f"{year_}-{month_:02d}-{first_day.day:02d}/{year_}"+
                            f"-{month_:02d}-{last_day.day:02d}"]
        
        items = gather_items(time_of_interest)
        
        assets_hrefs = aup.link_dict(band_name_list, items)
        
        df_links = pd.DataFrame.from_dict(assets_hrefs, 
                                        orient='Index').reset_index().rename(columns={'index':'date'})
        
        # mosaic raster
        def mosaic_process(red_links, nir_links):
            mosaic_red, _,_ = aup.mosaic_raster(red_links)
            aup.log('Finished processing red')
            mosaic_nir, out_trans_nir, out_meta = aup.mosaic_raster(nir_links)
            aup.log('Finished processing nir')
            return mosaic_red, mosaic_nir, out_trans_nir,out_meta

        for data_link in range(len(df_links)):
            try:
                aup.log(f'Mosaic date {df_links.iloc[data_link].date.day}'+
                        f'/{df_links.iloc[data_link].date.month}'+
                        f'/{df_links.iloc[data_link].date.year}')
                aup.log(df_links.iloc[data_link].red)
                mosaic_red, mosaic_nir, out_trans_nir,out_meta = mosaic_process(
                    df_links.iloc[data_link].red,
                            df_links.iloc[data_link].nir)
                checker = 1
                break
            except:
                continue
                
        if checker==0:
            df_len.iloc[i].data_id=0
            continue

        # modify data types
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

        # save raster to local file
        aup.log('Starting save')

        with rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif", "w", **out_meta) as dest:
            dest.write(ndvi)

            dest.close()

        aup.log('Finished saving')
        del mosaic_red
        del mosaic_nir
        del ndvi

    aup.log(f'Could not process {len(df_len.loc[df_len.data_id==0])-missing_months} months')

    if len(df_len.loc[df_len.data_id==0])/len(df_len) > 0.5:
        aup.log(f'Aborting analysis for amount of missing data for {city}')
        aup.delete_files_from_folder(tmp_dir)

    ###############################
    # Raster data to hex

    raster_file = ''

    def interpolate_raster_data(data, index_analysis=index_analysis):
        return data[index_analysis].interpolate()

    aup.log('Starting raster data to hex')

    for r in range(8,12):
        aup.log(f'Starting analysis for resolution {r}')
        # group raster by hex
        hex_raster = aup.raster_to_hex(hex_gdf, df_len, r, 
        index_analysis, city, tmp_dir)
        aup.log('Assigned raster data to hexagons')
        # interpolate data
        hex_raster_inter = hex_raster.groupby('hex_id',group_keys=True).apply(interpolate_raster_data)
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
        upload_chunk = 150000

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
    aup.delete_files_from_folder(tmp_dir)

if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')
    band_name_list = ['red','nir']
    index_analysis = 'ndvi'
    tmp_dir = f'../data/processed/tmp_{index_analysis}/'
    time_range = 36
    save = True

    gdf_mun = aup.gdf_from_db('metro_gdf', 'metropolis')
    gdf_mun = gdf_mun.sort_values(by='city')

    # prevent cities being analyzed to times in case of a crash
    processed_city_list = []
    try:
        processed_city_list = aup.gdf_from_db('raster_analysis', 'ndvi_analysis_hex')
        processed_city_list = list(processed_city_list.city.unique())
    except:
        pass

    for city in gdf_mun.city.unique():

        if city not in processed_city_list:

            aup.log(f'\n Starting city {city}')

            cvegeo_list = list(gdf_mun.loc[gdf_mun.city==city]["CVEGEO"].unique())

            main(index_analysis, city, cvegeo_list, band_name_list, time_range, save)