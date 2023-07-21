from .utils import *
from .data import *
from tqdm import tqdm
import rasterio
from rasterio import windows
from rasterio import features
from rasterio import warp
import rasterio.mask
from rasterio.enums import Resampling
from rasterio.merge import merge
from rasterio.fill import fillnodata
from pystac.extensions.eo import EOExtension as eo
from datetime import datetime
from dateutil.relativedelta import relativedelta
import planetary_computer as pc
from scipy import stats as st
from rasterio.merge import merge
import pandas as pd
import numpy as np
from func_timeout import func_timeout, FunctionTimedOut
import pymannkendall as mk
import scipy.ndimage as ndimage
from pystac_client import Client
from pandarallel import pandarallel
from multiprocessing import Pool
np.seterr(divide='ignore', invalid='ignore')


class AvailableData(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

class NanValues(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message
    

def available_data_check(df_len, missing_months, pct_limit=50, window_limit=5):
    pct_missing = round(missing_months/len(df_len),2)*100
    log(f'Created DataFrame with {missing_months} ({pct_missing}%) missing months')
    if pct_missing >= pct_limit: 
        raise AvailableData('Missing more than 50 percent of data points')
    df_rol = df_len.rolling(window_limit).sum()
    if len(df_rol.loc[df_rol.data_id==0])>0:
        raise AvailableData('Multiple missing months together')
    del df_rol


def download_raster_from_pc(gdf, index_analysis, city, freq, start_date, end_date,  
                               tmp_dir, band_name_dict, query={},satellite="sentinel-2-l2a"):

    # create area of interest coordinates from hexagons to download raster data    
    log('Extracting bounding coordinates from hexagons')
    # Create buffer around hexagons
    poly = gdf.to_crs("EPSG:6372").buffer(500)
    poly = poly.to_crs("EPSG:4326")
    poly = gpd.GeoDataFrame(geometry=poly).dissolve().geometry
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

    # create time of interest
    log('Defining time of interest')
    time_of_interest = create_time_of_interest(start_date, end_date, freq=freq)

    log('Gathering items for time and area of interest')
    items = gather_items(time_of_interest, area_of_interest, query=query, satellite=satellite)
    log(f'Fetched {len(items)} items')

    date_list = available_datasets(items, satellite)

    # create dictionary from links
    assets_hrefs = link_dict(list(band_name_dict.keys()), items, date_list)
    log('Created dictionary from items')

    # analyze available data according to raster properties
    df_len, missing_months = df_date_links(assets_hrefs, start_date, end_date, list(band_name_dict.keys()), freq)
    available_data_check(df_len, missing_months) # test for missing months

    # create GeoDataFrame for cropping
    bounding_box = gpd.GeoDataFrame(geometry=poly).envelope
    gdf_bb = gpd.GeoDataFrame(gpd.GeoSeries(bounding_box), columns=['geometry'])
    log('Created bounding box for raster cropping')

    # create GeoDataFrame to test nan values in raster
    gdf_raster_test = gdf.to_crs("EPSG:6372").buffer(1)
    gdf_raster_test = gdf_raster_test.to_crs("EPSG:4326")
    gdf_raster_test = gpd.GeoDataFrame(geometry=gdf_raster_test).dissolve()
    log('Starting raster creation for specified time')

    # download raster data by month
    df_len = create_raster_by_month(
        df_len, index_analysis, city, tmp_dir,
        band_name_dict,date_list, gdf_raster_test,
        gdf_bb, area_of_interest, satellite)
    log('Finished raster creation')
    missing_months = len(df_len.loc[df_len.data_id==0])
    log(f'Updated missing months to {missing_months} ({round(missing_months/len(df_len),2)*100}%)')
    
    # interpolate rasters over time for missing months
    log('Starting raster interpolation')
    df_len = raster_interpolation(df_len, city, tmp_dir, index_analysis)
    log('Finished raster interpolation')
    missing_months = len(df_len.loc[df_len.data_id==0])
    log(f'Updated missing months to {missing_months} ({round(missing_months/len(df_len),2)*100}%)')

    return df_len



def create_time_of_interest(start_date, end_date, freq='MS'):

    # create dataframe with dates
    df_tmp_dates = pd.DataFrame() # temporary date dataframe
    df_tmp_dates['date'] = pd.date_range(start = start_date,   
                                end = end_date, 
                                freq = freq)
    # extract month and year from date
    df_tmp_dates['month'] = df_tmp_dates.apply(lambda row: row['date'].month, axis=1)
    df_tmp_dates['year'] = df_tmp_dates.apply(lambda row: row['date'].year, axis=1)

    time_of_interest = []

    # create a time range by month
    for d in range(len(df_tmp_dates)):
        
        month = df_tmp_dates.loc[df_tmp_dates.index==d].month.values[0]
        year = df_tmp_dates.loc[df_tmp_dates.index==d].year.values[0]

        sample_date = datetime(year, month, 1)
        first_day = sample_date + relativedelta(day=1) # first day of the month
        last_day = sample_date + relativedelta(day=31) # last day of the month

        # append time range to time of interest list with planetary computer format
        time_of_interest.append(f"{year}-{month:02d}-{first_day.day:02d}/{year}"+
                                f"-{month:02d}-{last_day.day:02d}")
        
    return time_of_interest


def gather_items(time_of_interest, area_of_interest, query = {}, satellite="sentinel-2-l2a"):

    # gather items from planetary computer by date and area of interest
    catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

    items = []
    for t in time_of_interest:
        try:
            search = catalog.search(
                collections=[satellite],
                intersects=area_of_interest,
                datetime=t,
                query = query
            )

            # Check how many items were returned
            items.extend(list(search.get_items()))
        except:
            log('No items found')
            continue
    return items

def find_asset_by_band_common_name(item, common_name):
    # gather links for each band
    for asset in item.assets.values():
        asset_bands = eo.ext(asset).bands
        if asset_bands and asset_bands[0].common_name == common_name:
            return asset
    raise KeyError(f"{common_name} band not found")

def link_dict(band_name_list, items, date_list):

    # create dictionary from links by date
    
    assets_hrefs = {}

    for i in items:
        # only takes into account dates that are in the date list
        if i.datetime.date() not in date_list:
            continue
        # if date already in dictionary, append link to list
        if i.datetime.date() in list(assets_hrefs.keys()):
            for b in band_name_list:
                assets_hrefs[i.datetime.date()][b].append(pc.sign(find_asset_by_band_common_name(i,b).href))
        # if date not in dictionary, create new dictionary entry
        else:
            assets_hrefs[i.datetime.date()] = {}
            for b in band_name_list:
                assets_hrefs[i.datetime.date()].update({b:[]})
                assets_hrefs[i.datetime.date()][b].append(pc.sign(find_asset_by_band_common_name(i,b).href))
                
    return assets_hrefs


def df_date_links(assets_hrefs, start_date, end_date, band_name_list, freq='MS'):
    # dictionary with dates and links to dataframe
    df_dates = pd.DataFrame.from_dict(assets_hrefs, orient='Index').reset_index().rename(columns={'index':'date'})
    df_dates['date'] = pd.to_datetime(df_dates['date']).dt.date
    df_dates['year'] = df_dates.apply(lambda row: row['date'].year, axis=1)
    df_dates['month'] = df_dates.apply(lambda row: row['date'].month, axis=1)
    
    df_dates_filtered = pd.DataFrame()
    
    # keep only one data point by month
    for y in df_dates['year'].unique():
        for m in df_dates.loc[df_dates['year']==y,'month'].unique():
            df_dates_filtered = pd.concat([df_dates_filtered,
                                         df_dates.loc[(df_dates['year']==y)&
                                                      (df_dates['month']==m)].sample(1)],
                                          ignore_index=True)
    
    # create full range time dataframe
    df_tmp_dates = pd.DataFrame() # temporary date dataframe
    df_tmp_dates['date'] = pd.date_range(start = start_date,   
                               end = end_date, 
                               freq = freq) # create date range
    # extract year and month from date
    df_tmp_dates['year'] = df_tmp_dates.apply(lambda row: row['date'].year, axis=1)
    df_tmp_dates['month'] = df_tmp_dates.apply(lambda row: row['date'].month, axis=1)

    # remove date column for merge
    df_tmp_dates.drop(columns=['date'], inplace=True)

    df_complete_dates = df_tmp_dates.merge(df_dates_filtered, left_on=['year','month'],
                                          right_on=['year','month'], how='left')

    # remove date 
    df_complete_dates.drop(columns='date', inplace=True)
    df_complete_dates.sort_values(by=['year','month'], inplace=True)
    
    # create binary column for available (1) or missing data (0)
    idx = df_complete_dates[band_name_list[0]].isna()
    df_complete_dates['data_id'] = 0
    df_complete_dates.loc[~idx,'data_id'] = 1
    
    df_complete_dates.drop(columns=band_name_list, inplace=True)

    # calculate missing months
    missing_months = len(df_complete_dates.loc[df_complete_dates.data_id==0])
    
    return df_complete_dates, missing_months

def available_datasets(items, satellite="sentinel-2-l2a"):

    # test raster outliers by date

    date_dict = {}
    # iterate over raster tiles by date
    for i in items:
        if satellite == "sentinel-2-l2a":
            # check and add raster properties to dictionary by tile and date
            # if date is within dictionary append properties from item to list
            if i.datetime.date() in list(date_dict.keys()):
                # gather cloud percentage, high_proba_clouds_percentage, no_data values and nodata_pixel_percentage
                # check if properties are within dictionary date keys
                if i.properties['s2:mgrs_tile']+'_cloud' in list(date_dict[i.datetime.date()].keys()):
                    date_dict[i.datetime.date()].update(
                        {i.properties['s2:mgrs_tile']+'_cloud':
                        i.properties['s2:high_proba_clouds_percentage']})
                    date_dict[i.datetime.date()].update(
                        {i.properties['s2:mgrs_tile']+'_nodata':
                        i.properties['s2:nodata_pixel_percentage']})
                
                else:
                    date_dict[i.datetime.date()].update(
                        {i.properties['s2:mgrs_tile']+'_cloud':
                        i.properties['s2:high_proba_clouds_percentage']})
                    date_dict[i.datetime.date()].update(
                        {i.properties['s2:mgrs_tile']+'_nodata':
                        i.properties['s2:nodata_pixel_percentage']})
            # create new date key and add properties to it
            else:
                date_dict[i.datetime.date()] = {}
                date_dict[i.datetime.date()].update(
                    {i.properties['s2:mgrs_tile']+'_cloud':
                    i.properties['s2:high_proba_clouds_percentage']})
                date_dict[i.datetime.date()].update(
                    {i.properties['s2:mgrs_tile']+'_nodata':
                    i.properties['s2:nodata_pixel_percentage']})
        elif satellite == "landsat-c2-l2":
            # check and add raster properties to dictionary by tile and date
            # if date is within dictionary append properties from item to list
            if i.datetime.date() in list(date_dict.keys()):
                # gather cloud percentage, high_proba_clouds_percentage, no_data values and nodata_pixel_percentage
                # check if properties are within dictionary date keys
                if i.properties['landsat:wrs_row']+'_cloud' in list(date_dict[i.datetime.date()].keys()):
                    date_dict[i.datetime.date()].update(
                        {i.properties['llandsat:wrs_row']+'_cloud':
                        i.properties['landsat:cloud_cover_land']})
                
                else:
                    date_dict[i.datetime.date()].update(
                        {i.properties['landsat:wrs_row']+'_cloud':
                        i.properties['landsat:cloud_cover_land']})
            # create new date key and add properties to it
            else:
                date_dict[i.datetime.date()] = {}
                date_dict[i.datetime.date()].update(
                    {i.properties['landsat:wrs_row']+'_cloud':
                    i.properties['landsat:cloud_cover_land']})
    
    # determine third quartile for each tile
    df_tile = pd.DataFrame.from_dict(date_dict, orient='index')
    q3 = [np.percentile(df_tile[c].dropna(), 
                        [75]) for c in df_tile.columns.to_list()]
    q3 = [v[0] for v in q3]

    log(f'Quantile filter dictionary by column: {dict(zip(df_tile.columns, q3))}')

    column_list = df_tile.columns.to_list()

    # filter dates by missing values or outliers according to cloud and no_data values
    for c in range(len(column_list)):
        df_tile.loc[df_tile[column_list[c]]>q3[c],column_list[c]] = np.nan
    
    # create list of dates within normal distribution and without missing values
    date_list = df_tile.dropna().index.to_list()

    log(f'Available dates: {len(date_list)}')
    log(f'Raster tiles per date: {len(df_tile.columns.to_list())/2}')

    return date_list




def mosaic_raster(raster_asset_list, tmp_dir='tmp/', upscale=False):
    src_files_to_mosaic = []

    for assets in raster_asset_list:
        src = rasterio.open(assets)
        src_files_to_mosaic.append(src)
        
    mosaic, out_trans = merge(src_files_to_mosaic) # mosaic raster
    
    meta = src.meta
    
    if upscale:
        # save raster
        out_meta = src.meta

        out_meta.update({"driver": "GTiff",
                         "dtype": 'float32',
                         "height": mosaic.shape[1],
                         "width": mosaic.shape[2],
                         "transform": out_trans})
        # write raster
        with rasterio.open(tmp_dir+"mosaic_upscale.tif", "w", **out_meta) as dest:
            dest.write(mosaic)

            dest.close()
        # read and upscale
        with rasterio.open(tmp_dir+"mosaic_upscale.tif", "r") as ds:

            upscale_factor = 1/2

            mosaic = ds.read(
                        out_shape=(
                            ds.count,
                            int(ds.height * upscale_factor),
                            int(ds.width * upscale_factor)
                        ),
                        resampling=Resampling.bilinear
                    )
            out_trans = ds.transform * ds.transform.scale(
                                (ds.width / mosaic.shape[-1]),
                                (ds.height / mosaic.shape[-2])
                            )
            meta = ds.meta

            meta.update({"driver": "GTiff",
                            "dtype": 'float32',
                            "height": mosaic.shape[1],
                            "width": mosaic.shape[2],
                            "transform": out_trans})

        ds.close()
    src.close()
    
    return mosaic, out_trans, meta


def mosaic_process(links_band_1, links_band_2, band_name_dict, gdf_bb, tmp_dir=''):
    log(f'Starting mosaic for {list(band_name_dict.keys())[0]}')
    mosaic_band_1, out_trans_band_1, out_meta_1= mosaic_raster(links_band_1, tmp_dir, 
                                                               upscale=band_name_dict[list(band_name_dict.keys())[0]][0])
    mosaic_band_1 = mosaic_band_1.astype('float16')

    out_meta_1.update({"driver": "GTiff",
                    "dtype": 'float32',
                    "height": mosaic_band_1.shape[1],
                    "width": mosaic_band_1.shape[2],
                    "transform": out_trans_band_1})

    log(f'Starting save: {list(band_name_dict.keys())[0]}')

    with rasterio.open(f"{tmp_dir}{list(band_name_dict.keys())[0]}.tif", "w", **out_meta_1) as dest:
        dest.write(mosaic_band_1)

        dest.close()
        
    del mosaic_band_1
    log('Finished saving complete dataset')
    
    log('Starting crop')
    
    with rasterio.open(f"{tmp_dir}{list(band_name_dict.keys())[0]}.tif") as src:
        gdf_bb = gdf_bb.to_crs(src.crs)
        shapes = [gdf_bb.iloc[feature].geometry for feature in range(len(gdf_bb))]
        mosaic_band_1, out_transform = rasterio.mask.mask(src, shapes, crop=True)
        out_meta = src.meta
        out_meta.update({"driver": "GTiff",
                            "dtype": 'float32',
                            "height": mosaic_band_1.shape[1],
                            "width": mosaic_band_1.shape[2],
                            "transform": out_transform})
        src.close()


    with rasterio.open(f"{tmp_dir}{list(band_name_dict.keys())[0]}.tif", "w", **out_meta) as dest:
        dest.write(mosaic_band_1)

        dest.close()

    mosaic_band_1 = mosaic_band_1.astype('float16')

    log(f'Finished croping: {list(band_name_dict.keys())[0]}')

    log(f'Finished processing {list(band_name_dict.keys())[0]}')

    log(f'Starting mosaic for {list(band_name_dict.keys())[1]}')
    mosaic_band_2, out_trans_band_2, out_meta_2 = mosaic_raster(links_band_2, tmp_dir, 
                                                               upscale=band_name_dict[list(band_name_dict.keys())[1]][0])
    log(f'Finished processing {list(band_name_dict.keys())[1]}')
    mosaic_band_2 = mosaic_band_2.astype('float16')
    log('Transformed band arrays to float16')

    out_meta_2.update({"driver": "GTiff",
                    "dtype": 'float32',
                    "height": mosaic_band_2.shape[1],
                    "width": mosaic_band_2.shape[2],
                    "transform": out_trans_band_2})

    log(f'Starting save: {list(band_name_dict.keys())[1]}')

    with rasterio.open(f"{tmp_dir}{list(band_name_dict.keys())[1]}.tif", "w", **out_meta_2) as dest:
        dest.write(mosaic_band_2)

        dest.close()
        
    del mosaic_band_2
    log('Finished saving complete dataset')
    
    log('Starting crop')
    
    with rasterio.open(f"{tmp_dir}{list(band_name_dict.keys())[1]}.tif") as src:
        gdf_bb = gdf_bb.to_crs(src.crs)
        shapes = [gdf_bb.iloc[feature].geometry for feature in range(len(gdf_bb))]
        mosaic_band_2, out_transform = rasterio.mask.mask(src, shapes, crop=True)
        out_meta = src.meta
        out_meta.update({"driver": "GTiff",
                            "dtype": 'float32',
                            "height": mosaic_band_2.shape[1],
                            "width": mosaic_band_2.shape[2],
                            "transform": out_transform})
        src.close()

    with rasterio.open(f"{tmp_dir}{list(band_name_dict.keys())[1]}.tif", "w", **out_meta) as dest:
        dest.write(mosaic_band_2)

        dest.close()

    mosaic_band_2 = mosaic_band_2.astype('float16')

    log(f'Finished croping: {list(band_name_dict.keys())[1]}')

    log(f'Finished processing {list(band_name_dict.keys())[1]}')
    

    return mosaic_band_1, mosaic_band_2, out_transform, out_meta



def raster_nan_test(gdf, raster_file):
    
    gdf['test'] = gdf.geometry.apply(lambda geom: clean_mask(geom, raster_file)).apply(np.ma.mean)
    
    log(f'There are {gdf.test.isna().sum()} null data values')

    if gdf['test'].isna().sum() > 0:
        raise NanValues('NaN values are still present after processing')

def mosaic_process_v2(raster_bands, band_name_dict, gdf_bb, tmp_dir):
    
    raster_array = {}
    
    for b in band_name_dict.keys():

        log(f'Starting mosaic for {b}')
        raster_array[b]= [mosaic_raster(raster_bands[b], tmp_dir, 
                                       upscale=band_name_dict[b][0])]
        raster_array[b][0] = raster_array[b][0].astype('float16')
        # return mosaic, out_trans, meta

        raster_array[b][2].update({"driver": "GTiff",
                        "dtype": 'float32',
                        "height": raster_array[b][0].shape[1],
                        "width": raster_array[b][0].shape[2],
                        "transform": raster_array[b][1]})

        log(f'Starting save: {b}')

        with rasterio.open(f"{tmp_dir}{list(band_name_dict.keys())[0]}.tif", "w", **raster_array[b][2]) as dest:
            dest.write(raster_array[b][0])

            dest.close()
            
        raster_array[b][0] = [np.nan]
        log('Finished saving complete dataset')
        
        log('Starting crop')
        
        with rasterio.open(f"{tmp_dir}{list(band_name_dict.keys())[0]}.tif") as src:
            gdf_bb = gdf_bb.to_crs(src.crs)
            shapes = [gdf_bb.iloc[feature].geometry for feature in range(len(gdf_bb))]
            raster_array[b][0], raster_array[b][1] = rasterio.mask.mask(src, shapes, crop=True)
            raster_array[b][2] = src.meta
            raster_array[b][2].update({"driver": "GTiff",
                                "dtype": 'float32',
                                "height": raster_array[b][0].shape[1],
                                "width": raster_array[b][0].shape[2],
                                "transform": raster_array[b][1]})
            src.close()


        with rasterio.open(f"{tmp_dir}{list(band_name_dict.keys())[0]}.tif", "w", **raster_array[b][2]) as dest:
            dest.write(raster_array[b][0])

            dest.close()

        raster_array[b][0] = raster_array[b][0].astype('float16')

        log(f'Finished croping: {list(band_name_dict.keys())[0]}')

        log(f'Finished processing {list(band_name_dict.keys())[0]}')

    return raster_array

def create_raster_by_month(df_len, index_analysis, city, tmp_dir, 
                           band_name_dict, date_list, gdf_raster_test, gdf_bb, 
                           aoi, sat, time_exc_limit=900):

    df_len['able_to_download'] = np.nan

    log('\n Starting raster analysis')

    # check if file exists, for example in case of code crash
    df_file_dir = tmp_dir+index_analysis+f'_{city}_dataframe.csv'
    if os.path.exists(df_file_dir) == False: # Or folder, will return true or false
        df_len.to_csv(df_file_dir)
    # create folder to store temporary raster files by iteration
    tmp_raster_dir = tmp_dir+'temporary_files/'
    if os.path.exists(tmp_raster_dir) == False: # Or folder, will return true or false
        os.mkdir(tmp_raster_dir)

    for i in tqdm(range(len(df_len)), position=0, leave=True):
        
        df_raster = pd.read_csv(df_file_dir, index_col=False)

        # binary id - checks if month could be processed
        checker = 0

        if df_raster.iloc[i].data_id==0:
            continue
            
        # gather month and year from df to save ndmi
        month_ = df_raster.loc[df_raster.index==i].month.values[0]
        year_ = df_raster.loc[df_raster.index==i].year.values[0]
        
        if f'{city}_{index_analysis}_{month_}_{year_}.tif' in os.listdir(tmp_dir):
            continue
        
        log(f'\n Starting new analysis for {month_}/{year_}')
        
        # gather links for raster images
        sample_date = datetime(year_, month_, 1)
        first_day = sample_date + relativedelta(day=1)
        last_day = sample_date + relativedelta(day=31)

        # creates time range for a specific month
        time_of_interest = [f"{year_}-{month_:02d}-{first_day.day:02d}/{year_}"+
                            f"-{month_:02d}-{last_day.day:02d}"]
        # gather links for the date range
        items = gather_items(time_of_interest, aoi, sat)
        # gather links from dates that are within date_list
        assets_hrefs = link_dict(list(band_name_dict.keys()), items, date_list)
        # create dataframe
        #df_links = pd.DataFrame.from_dict(assets_hrefs, 
        #                                orient='Index').reset_index().rename(columns={'index':'date'})
        
        # mosaic raster
        
        iter_count = 1
        
        while iter_count <= 5:

            # create skip date list used to analyze null values in raster
            skip_date_list = []

            #for data_link in range(len(df_links)):
            for data_link in range(len(assets_hrefs.keys())):
                log(f'Mosaic date {list(assets_hrefs.keys())[data_link].day}'+
                            f'/{list(assets_hrefs.keys())[data_link].month}'+
                            f'/{list(assets_hrefs.keys())[data_link].year} - iteration:{iter_count}')
                
                # check if date contains null values within study area
                #if df_links.iloc[data_link]['date'] in skip_date_list:
                if list(assets_hrefs.keys())[data_link] in skip_date_list:
                    continue

                try:
                    #links_band_1 = df_links.iloc[data_link][list(band_name_dict.keys())[0]]
                    #links_band_2 = df_links.iloc[data_link][list(band_name_dict.keys())[1]]
                    bands_links = assets_hrefs[list(assets_hrefs.keys())[data_link]]

                    rasters_arrays, _,out_meta = func_timeout(time_exc_limit, mosaic_process,
                                                                                args=(bands_links,
                                                                                      band_name_dict,gdf_bb, tmp_raster_dir))

                    # calculate raster index
                    raster_index = (mosaic_band_1-mosaic_band_2)/(mosaic_band_1+mosaic_band_2)
                    log(f'Calculated {index_analysis}')
                    del mosaic_band_1
                    del mosaic_band_2

                    log(f'Starting interpolation')

                    raster_index[raster_index == 0 ] = np.nan # change zero values to nan
                    raster_index = raster_index.astype('float32') # change data type to float32 to avoid fillnodata error

                    log(f'Interpolating {np.isnan(raster_index).sum()} nan values')
                    raster_fill = fillnodata(raster_index, mask=~np.isnan(raster_index),
                                        max_search_distance=50, smoothing_iterations=0)
                    log(f'Finished interpolation to fill na - {np.isnan(raster_fill).sum()} nan')

                    with rasterio.open(f"{tmp_raster_dir}{index_analysis}.tif",'w', **out_meta) as dest:
                            dest.write(raster_fill)

                            dest.close()

                    log('Starting null test')

                    raster_file = rasterio.open(f"{tmp_raster_dir}{index_analysis}.tif")

                    gdf_raster_test = gdf_raster_test.to_crs(raster_file.crs)

                    try:
                        # test for nan values within study area
                        raster_nan_test(gdf_raster_test,raster_file)

                        log('Passed null test')
                        
                        # save raster to processing database
                        with rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif",'w', **out_meta) as dest:
                            dest.write(raster_fill)

                            dest.close()
                        log(f'Finished saving {index_analysis} raster')

                        checker = 1
                        iter_count = 6
                        delete_files_from_folder(tmp_raster_dir)
                        break
                    except:
                        log('Failed null test')
                        skip_date_list.append(df_links.iloc[data_link]['date'])
                        delete_files_from_folder(tmp_raster_dir)

                except:
                    log(f'Error in iteration {iter_count}')
                    delete_files_from_folder(tmp_raster_dir)
                    continue
            iter_count = iter_count + 1
                
        if checker==0:
            df_raster.loc[df_raster.index==i,'data_id']=0
            df_raster.loc[df_raster.index==i,'able_to_download']=0
            df_raster.to_csv(df_file_dir, index=False)
            continue

        df_raster.loc[((df_len['year']==year_)&
                (df_raster['month']==month_)),'able_to_download'] = 1
        
        df_raster.loc[((df_len['year']==year_)&
                            (df_raster['month']==month_)),'no_data_values'] = np.isnan(raster_fill).sum()
        
        df_raster.to_csv(df_file_dir, index=False)
        
        del raster_fill

        available_data_check(df_raster, len(df_raster.loc[df_raster.data_id==0]))

    df_len = pd.read_csv(df_file_dir)[['year','month','data_id','able_to_download']]

    return df_len


def raster_interpolation(df_len, city, tmp_dir, index_analysis):

    log(f'Interpolating {len(df_len.loc[df_len.data_id==0])}')

    available_data_check(df_len, len(df_len.loc[df_len.data_id==0]))

    df_len['interpolate'] = 0
    
    for row in range(len(df_len)):
    
        if df_len.iloc[row].data_id == 0:
            start = row - 1
            if start == -1:
                start = 0
            
            try:
                finish = df_len.iloc[row:,:].data_id.ne(0).idxmax()
            except:
                finish = len(df_len)
                
            log(f'Row start:{start} - row finish:{finish}')
                    
            df_subset = df_len.iloc[start:finish+1]
            if df_subset.loc[df_subset.index==start].data_id.values[0] == 0:
                
                log('Enterning missing data case 1 - first value missing')
                
                month_ = df_subset.loc[df_subset.index==finish].month.values[0]
                year_ = df_subset.loc[df_subset.index==finish].year.values[0]
                raster_file = rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif")
                meta = raster_file.meta
                raster_last = raster_file.read()
                
                log('Read last raster data')
                
                cont = 0
                while df_subset.iloc[cont].data_id == 0:
                    month_ = int(df_subset.iloc[cont].month)
                    year_ = int(df_subset.iloc[cont].year)
                    with rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif",'w', **meta) as dest:
                        dest.write(raster_last)

                        dest.close()
                        
                        log('Finished creating raster')
                        df_len.loc[df_len.index==start+cont,'data_id'] = 1
                        df_len.loc[df_len.index==start+cont,'interpolate'] = 1
                    
                    cont += 1
                
            elif df_subset.loc[df_subset.index==finish].data_id.values[0] == 0:
                
                log('Enterning missing data case 2 - last value missing')
                
                month_ = df_subset.loc[df_subset.index==start].month.values[0]
                year_ = df_subset.loc[df_subset.index==start].year.values[0]
                raster_file = rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif")
                meta = raster_file.meta
                raster_first = raster_file.read()
                
                log('Read first raster data')
                
                cont = 1
                while cont < len(df_subset):
                    month_ = int(df_subset.iloc[cont].month)
                    year_ = int(df_subset.iloc[cont].year)
                    with rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif",'w', **meta) as dest:
                        dest.write(raster_first)

                        dest.close()
                        
                        log('Finished creating raster')
                        df_len.loc[df_len.index==start+cont,'data_id'] = 1
                        df_len.loc[df_len.index==start+cont,'interpolate'] = 1
                    
                    cont += 1
                
            else:
                
                log('Enterning missing data case 3  - mid point missing')
                
                month_ = df_subset.loc[df_subset.index==start].month.values[0]
                year_ = df_subset.loc[df_subset.index==start].year.values[0]
                raster_file = rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif")
                raster_first = raster_file.read()
                
                month_ = df_subset.loc[df_subset.index==finish].month.values[0]
                year_ = df_subset.loc[df_subset.index==finish].year.values[0]
                raster_file = rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif")
                meta = raster_file.meta
                raster_last = raster_file.read()
                
                missing_len = len(df_subset.loc[df_subset.data_id==0])
                slope = 1 / (missing_len + 1)
                slope_increment = slope
                
                cont = 1
                
                log(f'Preparing data for interpolation with slope {slope}')
                
                # rejoin arr1, arr2 into a single array of shape (2, 10, 10)
                arr = np.r_['0,3', raster_first, raster_last]
                # define the grid coordinates where you want to interpolate
                dim_row = raster_first.shape[1]
                dim_col = raster_first.shape[2]

                X, Y = np.meshgrid(np.arange(dim_row), np.arange(dim_col))
                
                while round(slope,4) < 1:
                    
                    log(f'Starting interpolation for iteration {cont} with position {slope}')
                    # switch order of col and row
                    coordinates = np.ones((dim_col, dim_row))*slope, X, Y
                    
                    inter_raster = ndimage.map_coordinates(arr, coordinates, order=1).T
                    inter_raster = inter_raster.reshape((1,inter_raster.shape[0],inter_raster.shape[1]))
                    
                    month_ = int(df_subset.iloc[cont].month)
                    year_ = int(df_subset.iloc[cont].year)
                    with rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif",'w', **meta) as dest:
                        dest.write(inter_raster)

                        dest.close()
                        
                        log(f'Finished creating raster')
                        
                        df_len.loc[df_len.index==start+cont,'data_id'] = 1
                        df_len.loc[df_len.index==start+cont,'interpolate'] = 1
                    
                    cont += 1
                    slope = slope + slope_increment

    df_file_dir = tmp_dir+index_analysis+f'_{city}_dataframe.csv'
    df_len.to_csv(df_file_dir)

    return df_len


def clean_mask(geom, dataset='', **mask_kw):
    mask_kw.setdefault('crop', True)
    mask_kw.setdefault('all_touched', True)
    mask_kw.setdefault('filled', False)
    masked, _ = rasterio.mask.mask(dataset=dataset, shapes=(geom,),
                                  **mask_kw)
    return masked


def mask_by_hexagon(hex_gdf,year,month,city,index_analysis,tmp_dir):
    hex_raster = hex_gdf.copy()
    # read ndmi file
    raster_file = rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month}_{year}.tif")

    hex_raster = hex_raster.to_crs(raster_file.crs)
    try:

        hex_raster[index_analysis] = hex_raster.geometry.apply(lambda geom: clean_mask(geom, raster_file)).apply(np.ma.mean)
    except:
        hex_raster[index_analysis] = np.nan

    hex_raster['month'] = month
    hex_raster['year'] = year

    hex_raster = hex_raster.to_crs("EPSG:4326")

    return hex_raster


def raster_to_hex_multi(hex_gdf, df_len, index_analysis, city, raster_dir):
    # create empty geodataframe to save ndmi by date
    hex_raster = gpd.GeoDataFrame()

    years_list = list(df_len.year.unique())

    for i in tqdm(range(len(years_list)),position=0,leave=True):
        y = years_list[i]
        input_list = [[hex_gdf,y,month,city,index_analysis,raster_dir] for month in list(df_len.month.unique())]
        pbar = tqdm(total=len(input_list))
        pool = Pool()
        hex_res = pd.concat(pool.starmap(mask_by_hexagon,input_list))
        pool.close()
        hex_raster = pd.concat([hex_raster, hex_res], 
            ignore_index = True, axis = 0)
        del hex_res
        
    return hex_raster

def raster_to_hex(hex_gdf, df_len, r, index_analysis, city, raster_dir):
    # create empty geodataframe to save index_analysis by date
    hex_raster = gpd.GeoDataFrame()

    for d in tqdm(range(len(df_len)),position=0,leave=True):

        month_ = df_len.loc[df_len.index==d].month.values[0]
        year_ = df_len.loc[df_len.index==d].year.values[0]

        hex_tmp = hex_gdf.copy()

        if df_len.iloc[d].data_id==1:

            # read index_analysis file
            raster_file = rasterio.open(f"{raster_dir}{city}_{index_analysis}_{month_}_{year_}.tif")

            hex_tmp = hex_tmp.to_crs(raster_file.crs)

            try:
                hex_tmp[index_analysis] = hex_tmp.geometry.apply(lambda geom: clean_mask(geom, raster_file)).apply(np.ma.mean)
            except:
                hex_tmp[index_analysis] = np.nan

        else:
            hex_tmp[index_analysis] = np.nan

        hex_tmp['month'] = month_
        hex_tmp['year'] = year_

        hex_tmp = hex_tmp.to_crs("EPSG:4326")

        # concatenate into single geodataframe
        hex_raster = pd.concat([hex_raster, hex_tmp], 
            ignore_index = True, axis = 0)

        del hex_tmp
        
    return hex_raster


def raster_to_hex_analysis(hex_gdf, df_len, index_analysis, tmp_dir, city, res):
    # group raster by hex
    log('Starting raster to hexagons')
    hex_gdf = hex_gdf.copy()
    hex_raster = raster_to_hex_multi(hex_gdf, df_len, index_analysis, city, tmp_dir)
    log('Assigned raster data to hexagons')
    
    # summary statistics
    hex_raster_analysis = hex_gdf[['hex_id','geometry','res']].drop_duplicates().copy()
    
    hex_raster_minmax = hex_raster[['hex_id',index_analysis,'year']].groupby(['hex_id','year']).agg(['max','min'])
    hex_raster_minmax.columns = ['_'.join(col) for col in hex_raster_minmax.columns]
    hex_raster_minmax = hex_raster_minmax.reset_index()
    hex_raster_minmax = hex_raster_minmax[['hex_id',f'{index_analysis}_max',f'{index_analysis}_min']].groupby(['hex_id']).mean()
    hex_raster_minmax = hex_raster_minmax.reset_index()

    hex_group_data = hex_raster[['hex_id',index_analysis]].groupby('hex_id').agg(['mean','std',
                                                                                'median',mk.sens_slope])
    hex_group_data.columns = ['_'.join(col) for col in hex_group_data.columns]
    hex_group_data = hex_group_data.reset_index().merge(hex_raster_minmax, on='hex_id')
    
    hex_raster_analysis = hex_raster_analysis.merge(hex_group_data, on='hex_id')
    hex_raster_analysis[index_analysis+'_diff'] = hex_raster_analysis[index_analysis+'_max'] - hex_raster_analysis[index_analysis+'_min']
    hex_raster_analysis[index_analysis+'_tend'] = hex_raster_analysis[f'{index_analysis}_sens_slope'].apply(lambda x: x[0])
    hex_raster_analysis = hex_raster_analysis.drop(columns=[f'{index_analysis}_sens_slope'])
    
    # remove geometry information
    hex_raster_df = hex_raster.drop(columns=['geometry'])
    
    # add city information
    hex_raster_df['city'] = city
    hex_raster_analysis['city'] = city

    log(f'df nan values: {hex_raster_df[index_analysis].isna().sum()}')

    return hex_raster_analysis, hex_raster_df