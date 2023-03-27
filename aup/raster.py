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


def index_analysis_to_hexagons(gdf, index_analysis, city, res, freq, start_date, end_date, 
                               satellite, tmp_dir, band_name_list, hex_check=True):
    # if GeoDataFrame is not h3 hexagons it creates them
    if hex_check == False:
        gdf = create_hexgrid(gdf, res[0])
        log('Created hexagon gdf')

    # create hexagons at different resolutions
    res_min = res[0]
    res_max = res[-1]

    hex_gdf = gdf.copy()
    hex_gdf.rename(columns={f'hex_id_{res_min}':'hex_id'}, inplace=True)
    hex_gdf['res'] = res_min

    log(f'Starting creation of hexagons from res:{res_min} to res:{res_max}')

    for r in range(res_min+1, res_max+1):
        hex_tmp = create_hexgrid(hex_gdf.loc[hex_gdf.res==res_min], r)
        hex_tmp.rename(columns={f'hex_id_{r}':'hex_id'}, inplace=True)
        hex_tmp['res'] = r
        
        hex_gdf = pd.concat([hex_gdf, hex_tmp], 
            ignore_index = True, axis = 0)
        
        del hex_tmp
        log(f'Created hexagons at res {r}')
    
    log('Finished created hexagons by resolution')

    log('Extracting bounding coordinates from hexagons')
    # Create buffer around hexagons
    poly = hex_gdf.loc[hex_gdf.res==res_min].to_crs("EPSG:6372").buffer(500)
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
    items = gather_items(time_of_interest, area_of_interest, satellite=satellite)
    log(f'Fetched {len(items)} items')

    # create dictionary from links
    assets_hrefs = link_dict(band_name_list, items)
    log('Created dictionary from items')

    # filter for dates with requiered links for area_of_interest
    # assets_hrefs, median_links = filter_links(assets_hrefs, band_name_list)
    # log(f'{median_links} rasters links by time analysis')

    # analyze available data
    df_len, missing_months = df_date_links(assets_hrefs, start_date, end_date, freq)
    log(f'Created DataFrame with {missing_months} ({round(missing_months/len(df_len),2)*100}%) missing months')

    # raster cropping
    bounding_box = gpd.GeoDataFrame(geometry=poly).envelope
    gdf_bb = gpd.GeoDataFrame(gpd.GeoSeries(bounding_box), columns=['geometry'])
    log('Created bounding box for raster cropping')

    log('Starting raster creation for specified time')
    df_len = create_raster_by_month(df_len, index_analysis, city, tmp_dir, 
                                    band_name_list, gdf_bb, area_of_interest, satellite)
    log('Finished raster creation')
    missing_months = len(df_len.loc[df_len.data_id==0])
    log(f'Updated missing months to {missing_months} ({round(missing_months/len(df_len),2)*100}%)')

    row_mode = df_len.raster_row.mode().values[0]
    col_mode = df_len.raster_col.mode().values[0]
    df_len.loc[((df_len.raster_row < row_mode)|
            (df_len.raster_col < col_mode)|
            (df_len.raster_col.isna())),'data_id'] = 0
    
    log('Starting raster interpolation')
    df_len = raster_interpolation(df_len, city, tmp_dir, index_analysis)
    log('Finished raster interpolation')
    missing_months = len(df_len.loc[df_len.data_id==0])
    log(f'Updated missing months to {missing_months} ({round(missing_months/len(df_len),2)*100}%)')

    return df_len

def download_raster_from_pc(gdf, index_analysis, city, freq, start_date, end_date, 
                               tmp_dir, band_name_list, satellite="sentinel-2-l2a"):

    # if GeoDataFrame is not h3 hexagons it creates them

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
    items = gather_items(time_of_interest, area_of_interest, satellite=satellite)
    log(f'Fetched {len(items)} items')

    # create dictionary from links
    assets_hrefs = link_dict(band_name_list, items)
    log('Created dictionary from items')

    # filter for dates with requiered links for area_of_interest
    assets_hrefs, median_links = filter_links(assets_hrefs, band_name_list)
    log(f'{median_links} rasters links by time analysis')

    # analyze available data
    df_len, missing_months = df_date_links(assets_hrefs, start_date, end_date, band_name_list, freq)
    log(f'Created DataFrame with {missing_months} ({round(missing_months/len(df_len),2)*100}%) missing months')

    # raster cropping
    bounding_box = gpd.GeoDataFrame(geometry=poly).envelope
    gdf_bb = gpd.GeoDataFrame(gpd.GeoSeries(bounding_box), columns=['geometry'])
    log('Created bounding box for raster cropping')

    log('Starting raster creation for specified time')
    df_len = create_raster_by_month(df_len, index_analysis, city, tmp_dir, 
                                    band_name_list, gdf_bb, area_of_interest, satellite)
    log('Finished raster creation')
    missing_months = len(df_len.loc[df_len.data_id==0])
    log(f'Updated missing months to {missing_months} ({round(missing_months/len(df_len),2)*100}%)')

    row_mode = df_len.raster_row.mode().values[0]
    col_mode = df_len.raster_col.mode().values[0]
    df_len.loc[((df_len.raster_row < row_mode)|
            (df_len.raster_col < col_mode)|
            (df_len.raster_col.isna())),'data_id'] = 0
    
    log('Starting raster interpolation')
    df_len = raster_interpolation(df_len, city, tmp_dir, index_analysis)
    log('Finished raster interpolation')
    missing_months = len(df_len.loc[df_len.data_id==0])
    log(f'Updated missing months to {missing_months} ({round(missing_months/len(df_len),2)*100}%)')

    return df_len



def create_time_of_interest(start_date, end_date, freq='MS'):
    df_tmp_dates = pd.DataFrame() # temporary date dataframe
    df_tmp_dates['date'] = pd.date_range(start = start_date,   
                                end = end_date,   # there are 30 periods because range from satelite img goes from 01-01-2020 - 30-06-2022
                                freq = freq)
    df_tmp_dates['month'] = df_tmp_dates.apply(lambda row: row['date'].month, axis=1)
    df_tmp_dates['year'] = df_tmp_dates.apply(lambda row: row['date'].year, axis=1)

    time_of_interest = []

    for d in range(len(df_tmp_dates)):
        
        month = df_tmp_dates.loc[df_tmp_dates.index==d].month.values[0]
        year = df_tmp_dates.loc[df_tmp_dates.index==d].year.values[0]

        sample_date = datetime(year, month, 1)
        first_day = sample_date + relativedelta(day=1)
        last_day = sample_date + relativedelta(day=31)

        time_of_interest.append(f"{year}-{month:02d}-{first_day.day:02d}/{year}"+
                                f"-{month:02d}-{last_day.day:02d}")
        
    return time_of_interest


def gather_items(time_of_interest, area_of_interest, satellite="sentinel-2-l2a"):
    catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

    items = []

    for t in time_of_interest:
        search = catalog.search(
            collections=[satellite],
            intersects=area_of_interest,
            datetime=t,
            query={"eo:cloud_cover": {"lt": 10}},
        )

        # Check how many items were returned
        items.extend(list(search.get_items()))
    return items

def find_asset_by_band_common_name(item, common_name):
    for asset in item.assets.values():
        asset_bands = eo.ext(asset).bands
        if asset_bands and asset_bands[0].common_name == common_name:
            return asset
    raise KeyError(f"{common_name} band not found")

def link_dict(band_name_list, items):
    
    assets_hrefs = {}

    for i in items:
        if i.datetime.date() in list(assets_hrefs.keys()):
            for b in band_name_list:
                assets_hrefs[i.datetime.date()][b].append(pc.sign(find_asset_by_band_common_name(i,b).href))
        else:
            assets_hrefs[i.datetime.date()] = {}
            for b in band_name_list:
                assets_hrefs[i.datetime.date()].update({b:[]})
                assets_hrefs[i.datetime.date()][b].append(pc.sign(find_asset_by_band_common_name(i,b).href))
                
    return assets_hrefs

def filter_links(assets_hrefs, band_name_list):
    max_links_len = st.mode(np.array([len(x[band_name_list[0]]) for x in list(assets_hrefs.values())]))[0][0]
    
    # iterate and remove dates without sufficient data
    for k_date in list(assets_hrefs.keys()):
        # gather data from first band in dictionary - the max value should be the same in all bands
        k_band = list(assets_hrefs[k_date].keys())[0]
        # compare len of that band to max
        if len(assets_hrefs[k_date][k_band]) != max_links_len:
            # if len is less it indicates that is missing data
            # remove date with missing data
            assets_hrefs.pop(k_date)
    
    return assets_hrefs, max_links_len


def df_date_links(assets_hrefs, start_date, end_date, band_name_list, freq='MS'):
    # dictionary to dataframe
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
                               end = end_date,   # there are 30 periods because range from satelite img goes from 01-01-2020 - 30-06-2022
                               freq = freq) # create date range
    # extract year and month
    df_tmp_dates['year'] = df_tmp_dates.apply(lambda row: row['date'].year, axis=1)
    df_tmp_dates['month'] = df_tmp_dates.apply(lambda row: row['date'].month, axis=1)

    # remove date column for merge
    df_tmp_dates.drop(columns=['date'], inplace=True)

    df_complete_dates = df_tmp_dates.merge(df_dates_filtered, left_on=['year','month'],
                                          right_on=['year','month'], how='left')

    # remove date 
    df_complete_dates.drop(columns='date', inplace=True)
    df_complete_dates.sort_values(by=['year','month'], inplace=True)
    
    idx = df_complete_dates[band_name_list[0]].isna()
    df_complete_dates['data_id'] = 0
    df_complete_dates.loc[~idx,'data_id'] = 1
    
    df_complete_dates.drop(columns=band_name_list, inplace=True)
    
    missing_months = len(df_complete_dates.loc[df_complete_dates.data_id==0])
    
    return df_complete_dates, missing_months


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

        ds.close()
    src.close()
    
    return mosaic, out_trans, meta

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
    # create empty geodataframe to save ndmi by date
    hex_raster = gpd.GeoDataFrame()

    for d in tqdm(range(len(df_len)),position=0,leave=True):

        month_ = df_len.loc[df_len.index==d].month.values[0]
        year_ = df_len.loc[df_len.index==d].year.values[0]

        hex_tmp = hex_gdf.loc[hex_gdf.res==r].copy()

        if df_len.iloc[d].data_id==1:

            # read ndmi file
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
    hex_gdf = hex_gdf.loc[hex_gdf.res==res].copy()
    hex_raster = raster_to_hex_multi(hex_gdf, df_len, index_analysis, city, tmp_dir)
    log('Assigned raster data to hexagons')
    
    # summary statistics
    hex_raster_analysis = hex_gdf[['hex_id','geometry','res']].drop_duplicates().copy()
    
    hex_group_data = hex_raster[['hex_id',index_analysis]].groupby('hex_id').agg(['mean','max','min','std',
                                                                                'median',mk.sens_slope])
    hex_group_data.columns = ['_'.join(col) for col in hex_group_data.columns]
    
    hex_raster_analysis = hex_raster_analysis.merge(hex_group_data.reset_index(), on='hex_id')
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

def mosaic_process(links_band_1, links_band_2, band_name_list, tmp_dir=''):
    mosaic_band_1, _,_ = mosaic_raster(links_band_1, tmp_dir, upscale=False)
    log(f'Finished processing {band_name_list[0]}')
    mosaic_band_2, out_trans_band_2, out_meta = mosaic_raster(links_band_2)
    log(f'Finished processing {band_name_list[1]}')
    return mosaic_band_1, mosaic_band_2, out_trans_band_2,out_meta


def create_raster_by_month(df_len, index_analysis, city, tmp_dir, 
                           band_name_list, gdf_bb, 
                           aoi, sat, time_exc_limit=600):

    df_len['raster_row'] = np.nan
    df_len['raster_col'] = np.nan
    df_len['no_data_values'] = np.nan

    log('\n Starting raster analysis')

    # check if file exists, for example in case of code crash
    df_file_dir = tmp_dir+index_analysis+f'_{city}_dataframe.csv'
    if os.path.exists(df_file_dir) == False: # Or folder, will return true or false
        df_len.to_csv(df_file_dir)

    for i in tqdm(range(len(df_len)), position=0, leave=True):
        
        df_raster = pd.read_csv(df_file_dir, index_col=False)

        
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
        
        time_of_interest = [f"{year_}-{month_:02d}-{first_day.day:02d}/{year_}"+
                            f"-{month_:02d}-{last_day.day:02d}"]
        
        items = gather_items(time_of_interest, aoi, sat)
        
        assets_hrefs = link_dict(band_name_list,items)
        
        df_links = pd.DataFrame.from_dict(assets_hrefs, 
                                        orient='Index').reset_index().rename(columns={'index':'date'})
        
        # mosaic raster
        
        iter_count = 1
        
        while iter_count <= 5:

            for data_link in range(len(df_links)):
                log(f'Mosaic date {df_links.iloc[data_link].date.day}'+
                            f'/{df_links.iloc[data_link].date.month}'+
                            f'/{df_links.iloc[data_link].date.year} - iteration:{iter_count}')
                try:
                    links_band_1 = df_links.iloc[data_link][band_name_list[0]]
                    links_band_2 = df_links.iloc[data_link][band_name_list[1]]
                    # band_links = [df_links.iloc[data_link][band_name_list[band]] for band in band_name_list]

                    mosaic_band_1, mosaic_band_2, out_trans_band_2,out_meta = func_timeout(time_exc_limit, mosaic_process,
                                                                                args=(links_band_1,links_band_2,band_name_list,tmp_dir))          
                    checker = 1
                    iter_count = 6
                    break
                except:
                    log(f'Error in iteration {iter_count}')
                    continue
            iter_count = iter_count + 1
                
        if checker==0:
            df_raster.loc[df_raster.index==i,'data_id']=0
            continue

        mosaic_band_1 = mosaic_band_1.astype('float32')
        mosaic_band_2 = mosaic_band_2.astype('float32')
        log('Transformed band arrays to float32')
        log(f'array datatype: {mosaic_band_1.dtype}')

        raster_index = (mosaic_band_1-mosaic_band_2)/(mosaic_band_1+mosaic_band_2)
        log(f'Calculated {index_analysis}')

        out_meta.update({"driver": "GTiff",
                    "dtype": 'float32',
                    "height": raster_index.shape[1],
                    "width": raster_index.shape[2],
                    "transform": out_trans_band_2})

        log('Starting save')

        with rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif", "w", **out_meta) as dest:
            dest.write(raster_index)

            dest.close()

        log('Finished saving complete dataset')
        
        log('Starting crop')
        
        with rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif") as src:
            gdf_bb = gdf_bb.to_crs(src.crs)
            shapes = [gdf_bb.iloc[feature].geometry for feature in range(len(gdf_bb))]
            out_image, out_transform = rasterio.mask.mask(src, shapes, crop=True)
            out_meta = src.meta
            out_meta.update({"driver": "GTiff",
                                "dtype": 'float32',
                                "height": out_image.shape[1],
                                "width": out_image.shape[2],
                                "transform": out_transform})
            src.close()
        log('Finished croping')
        
        df_raster.loc[((df_raster['year']==year_)&
                (df_raster['month']==month_)),'raster_row'] = out_image.shape[1]
        df_raster.loc[((df_len['year']==year_)&
                (df_raster['month']==month_)),'raster_col'] = out_image.shape[2]
        
        log(f'Starting interpolation')

        out_image[out_image == 0 ] = np.nan # change zero values to nan
        
        df_raster.loc[((df_len['year']==year_)&
                (df_raster['month']==month_)),'no_data_values'] = np.isnan(out_image).sum()

        log(f'Interpolating {np.isnan(out_image).sum()} nan values')
        raster_fill = fillnodata(out_image, mask=~np.isnan(out_image),
                            max_search_distance=50, smoothing_iterations=0)
        log(f'Finished interpolation to fill na - {np.isnan(raster_fill).sum()} nan')
        
        log('Starting to save croped raster')
            
        with rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif",'w', **out_meta) as dest:
            dest.write(raster_fill)

            dest.close()
        log('Finished saving croped raster')
        
        df_raster.to_csv(df_file_dir, index=False)
        
        del out_image
        del mosaic_band_1
        del mosaic_band_2
        del raster_index

    df_len = pd.read_csv(df_file_dir)[['year','month','data_id','raster_row','raster_col']]

    return df_len


def raster_interpolation(df_len, city, tmp_dir, index_analysis):
    row_mode = df_len.raster_row.mode().values[0]
    col_mode = df_len.raster_col.mode().values[0]
    df_len.loc[(((df_len.raster_row < row_mode)|
            (df_len.raster_col < col_mode))&
            (df_len.raster_col.notna())),'data_id'] = 0
    mean_no_data = df_len.no_data_values.mean()
    desvest_no_data = df_len.no_data_values.std()
    df_len.loc[df_len.no_data_values > mean_no_data+desvest_no_data,'data_id'] = 0

    log(f'Interpolating {len(df_len.loc[df_len.data_id==0])}')

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
                        df_len.loc[df_len.index==start+cont,'raster_row'] = row_mode
                        df_len.loc[df_len.index==start+cont,'raster_col'] = col_mode
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
                        df_len.loc[df_len.index==start+cont,'raster_row'] = row_mode
                        df_len.loc[df_len.index==start+cont,'raster_col'] = col_mode
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
                        df_len.loc[df_len.index==start+cont,'raster_row'] = dim_row
                        df_len.loc[df_len.index==start+cont,'raster_col'] = dim_col
                        df_len.loc[df_len.index==start+cont,'interpolate'] = 1
                    
                    cont += 1
                    slope = slope + slope_increment

    df_file_dir = tmp_dir+index_analysis+f'_{city}_dataframe.csv'
    df_len.to_csv(df_file_dir)

    return df_len