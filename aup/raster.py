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

# Flags to ignore division by zero and invalid floating point operations
np.seterr(divide='ignore', invalid='ignore')

# A class is created it receives a message and has a function that returns it as a string
class AvailableData(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message



def download_raster_from_pc(gdf, index_analysis, city, freq, start_date, end_date, 
                               tmp_dir, band_name_list, satellite="sentinel-2-l2a"):
    """
    Function that returns a raster with the data provided

    Arguments:
        gdf (GeoDataFrame): Area of interest
        index_analysis (str): Index of analysis
        city (str): City name
        freq (str): Frequency
        start_date (date): First date of data
        end_date (date): First date of data
        tmp_dir (str): address of temporary directory
        band_name_list (list): List with data
        satellite (str): Defaults to "sentinel-2-l2a".

    Raises:
        AvailableData: Object with a message

    Returns:
        df_len (dataframe): Final raster
    """
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

    # Sets the coordinates for the area of interest
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
    # gathers items for time and area of interest
    log('Gathering items for time and area of interest')
    items = gather_items(time_of_interest, area_of_interest, satellite=satellite)
    log(f'Fetched {len(items)} items')

    date_list = available_datasets(items)

    # create dictionary from links
    assets_hrefs = link_dict(band_name_list, items, date_list)
    log('Created dictionary from items')

    # filter for dates with requiered links for area_of_interest
    # assets_hrefs, median_links = filter_links(assets_hrefs, band_name_list)
    # log(f'{median_links} rasters links by time analysis')

    # creates raster and analyzes percentage of missing data points
    df_len, missing_months = df_date_links(assets_hrefs, start_date, end_date, band_name_list, freq)
    pct_missing = round(missing_months/len(df_len),2)*100
    log(f'Created DataFrame with {missing_months} ({pct_missing}%) missing months')
    # if more than 50% of data is missing, raise error and print message
    if pct_missing >= 50:
        
        raise AvailableData('Missing more than 50 percent of data points')

    # raster cropping with bounding box from earlier 
    bounding_box = gpd.GeoDataFrame(geometry=poly).envelope
    gdf_bb = gpd.GeoDataFrame(gpd.GeoSeries(bounding_box), columns=['geometry'])
    log('Created bounding box for raster cropping')
    # raster creation
    log('Starting raster creation for specified time')
    df_len = create_raster_by_month(df_len, index_analysis, city, tmp_dir, 
                                    band_name_list,date_list, gdf_bb, area_of_interest, satellite)
    log('Finished raster creation')
    # calculates percentage of missing months
    missing_months = len(df_len.loc[df_len.data_id==0])
    log(f'Updated missing months to {missing_months} ({round(missing_months/len(df_len),2)*100}%)')
    # assures that all the values missing are filled with 0
    row_mode = df_len.raster_row.mode().values[0]
    col_mode = df_len.raster_col.mode().values[0]
    df_len.loc[((df_len.raster_row < row_mode)|
            (df_len.raster_col < col_mode)|
            (df_len.raster_col.isna())),'data_id'] = 0
    # starts raster interpolation by predicting points from existing values and updates missing months percentage
    log('Starting raster interpolation')
    df_len = raster_interpolation(df_len, city, tmp_dir, index_analysis)
    log('Finished raster interpolation')
    missing_months = len(df_len.loc[df_len.data_id==0])
    log(f'Updated missing months to {missing_months} ({round(missing_months/len(df_len),2)*100}%)')
    # returns final raster
    return df_len




def create_time_of_interest(start_date, end_date, freq='MS'):
    """
    Creates the time of interest for the raster
    Arguments:
        start_date (date): First date in data
        end_date (date): Last date in data
        freq (str):Defaults to 'MS'.

    Returns:
        time_of_interest (array): Dates of interest
    """
    df_tmp_dates = pd.DataFrame() # temporary date dataframe
    df_tmp_dates['date'] = pd.date_range(start = start_date,   
                                end = end_date,   # there are 30 periods because range from satelite img goes from 01-01-2020 - 30-06-2022
                                freq = freq)
    df_tmp_dates['month'] = df_tmp_dates.apply(lambda row: row['date'].month, axis=1)
    df_tmp_dates['year'] = df_tmp_dates.apply(lambda row: row['date'].year, axis=1)

    time_of_interest = []
    
    # Fills array with days
    for d in range(len(df_tmp_dates)):
        
        month = df_tmp_dates.loc[df_tmp_dates.index==d].month.values[0]
        year = df_tmp_dates.loc[df_tmp_dates.index==d].year.values[0]

        sample_date = datetime(year, month, 1)
        first_day = sample_date + relativedelta(day=1)
        last_day = sample_date + relativedelta(day=31)

        time_of_interest.append(f"{year}-{month:02d}-{first_day.day:02d}/{year}"+
                                f"-{month:02d}-{last_day.day:02d}")
        
    # Returns array with time of interest
    return time_of_interest


def gather_items(time_of_interest, area_of_interest, satellite="sentinel-2-l2a"):
    """ 
    Items gathered in time and area of interest from planetary computer

    Arguments:
        time_of_interest (array): days of interest
        area_of_interest (dict): Polygon, area of interest
        satellite (str): Defaults to "sentinel-2-l2a".

    Returns:
        items (array): items intersecting time and area of interest
    """
    catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

    items = []

    for t in time_of_interest:
        search = catalog.search(
            collections=[satellite],
            intersects=area_of_interest,
            datetime=t,
        )

        # Check how many items were returned
        items.extend(list(search.get_items()))
    return items

def find_asset_by_band_common_name(item, common_name):
    """
    Filter that receives an item from a list and searches for a band with a common name

    Arguments:
        item (object): Belongs to the gathered items
        common_name (str): Common name of the band  to be searched

    Raises:
        KeyError: If common_name is not found

    Returns:
        asset (str) : Asset with the common name
    """
    for asset in item.assets.values():
        asset_bands = eo.ext(asset).bands
        if asset_bands and asset_bands[0].common_name == common_name:
            return asset
    raise KeyError(f"{common_name} band not found")

def link_dict(band_name_list, items, date_list):
    """
    The function creates a dictionary with the links to the assets

    Arguments:
        band_name_list (list): List with data
        items (list): items intersecting time and area of interest
        date_list (list): dates of interest

    Returns:
        assets_hrefs (dict): Dictionary with the links to the assets
    """
    assets_hrefs = {}

    for i in items:
        if i.datetime.date() not in date_list:
            continue
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
    """
    The function filters links to assets, removing those without sufficient data.

    Arguments:
        assets_hrefs (dict): links to assets
        band_name_list (list): List with data

    Returns:
        assets_hrefs (dict): Updated dictionary
        max_links_len (int): Max number of links
    """
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
    """Function converts the dictionary of assets into a dataframe that contains. date, month and year. It merges the month and year, to then remove the date and extract the data_idand remove columns containing band names.
    Aditionally it uses a function to count the missing months 
    Arguments:
        assets_hrefs (dict): Dictionary with the links to the assets
        start_date (str): First date in data
        end_date (str): Last date in data
        band_name_list (list): List with data
        freq (str): Defaults to 'MS'.

    Returns:
        df_complete_dates (dataframe): Dataframe with filtered dates
        missing_months (int): Number of missing months
    """
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

def available_datasets(items):
    """
    Filters dates per quantile and finds available ones

    Arguments:
        items (array): items intersecting time and area of interest

    Returns:
        date_list (list): List with available dates with filter
    """
    date_dict = {}

    for i in items:
        if i.datetime.date() in list(date_dict.keys()):
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
        else:
            date_dict[i.datetime.date()] = {}
            date_dict[i.datetime.date()].update(
                {i.properties['s2:mgrs_tile']+'_cloud':
                 i.properties['s2:high_proba_clouds_percentage']})
            date_dict[i.datetime.date()].update(
                {i.properties['s2:mgrs_tile']+'_nodata':
                 i.properties['s2:nodata_pixel_percentage']})
            
    df_tile = pd.DataFrame.from_dict(date_dict, orient='index')
    q3 = [np.percentile(df_tile[c].dropna(), 
                        [75]) for c in df_tile.columns.to_list()]
    q3 = [v[0] for v in q3]

    log(f'Quantile filter dictionary by column: {dict(zip(df_tile.columns, q3))}')

    column_list = df_tile.columns.to_list()

    for c in range(len(column_list)):
        df_tile.loc[df_tile[column_list[c]]>q3[c],column_list[c]] = np.nan
    date_list = df_tile.dropna().index.to_list()

    log(f'Available dates: {len(date_list)}')
    log(f'Raster tiles per date: {len(df_tile.columns.to_list())/2}')

    return date_list




def mosaic_raster(raster_asset_list, tmp_dir='tmp/', upscale=False):
    """
    The mosaic_raster function takes a list of raster assets and merges them together.
        Arguments:
            raster_asset_list (list): A list of raster asset paths to be appended together.
            tmp_dir (str): The directory where temporary files will be stored during processing. Defaults to 'tmp/'.
            upscale (bool): Whether or not the mosaic is upscaled by 2x  using the formats of the conditional statement
        Returns:
            mosaic (list): Transforms the data into a raster object
            out_trans (list): Mosaic that is upscaled 
            meta (dictionary): Metadata of the raster object

       
    """

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
    """
    The mask in this function is used to extract the values from a raster dataset that fall 
    within a given geometry of interest.
    Arguments:
        geom (geometry): Geometric figure that will be used to mask the raster dataset.
        dataset (rasterio DatasetReader): The raster dataset that will be masked by the 
        inputted geometry. If no value is provided, then it defaults to an empty string 
        and returns only the masked array of values from within the inputted geometry 
        without any metadata.
        mask_kw (dict): A dictionary of arguments passed to create the mask.
    Returns:
        masked (array): Returns values from within the inputted geometry.
    """
    
    mask_kw.setdefault('crop', True)
    mask_kw.setdefault('all_touched', True)
    mask_kw.setdefault('filled', False)
    masked, _ = rasterio.mask.mask(dataset=dataset, shapes=(geom,),
                                  **mask_kw)
    return masked


def mask_by_hexagon(hex_gdf,year,month,city,index_analysis,tmp_dir):
    """"
    The function takes a hexagon GeoDataFrame, year, month, city name and index analysis as input.
    It then opens the raster file for that specific month and year in the tmp_dir directory. 
    It applies a mask to the raster file
    
    Arguments:
        hex_gdf (geodataframe): Creates a copy of the hexagon geodataframe
        year (int): Specify the year that will be used
        month (int): Month index that will be used
        city (str): Specify the city for which we want to calculate the ndmi
        index_analysis (str): Specify which index analysis to use
        tmp_dir (str): Specify the directory where the raster files are stored
    Returns:
        hex_raster (matrix): A hexagon raster with the index analysis added as a column
    """
    hex_raster = hex_gdf.copy()
    # read ndmi file
    raster_file = rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month}_{year}.tif")

    hex_raster = hex_raster.to_crs(raster_file.crs) 
    # Using the apply function to apply the clean_mask function to the geometry column of the hex_gdf geodataframe
    try:

        hex_raster[index_analysis] = hex_raster.geometry.apply(lambda geom: clean_mask(geom, raster_file)).apply(np.ma.mean)
    except:
        hex_raster[index_analysis] = np.nan
    #adds month and year columns to the hex_raster geodataframe
    hex_raster['month'] = month
    hex_raster['year'] = year

    hex_raster = hex_raster.to_crs("EPSG:4326")

    return hex_raster


def raster_to_hex_multi(hex_gdf, df_len, index_analysis, city, raster_dir):
    """
    The function takes a hexagon geodataframe, the length of the dataframe,
    the index analysis Normalized Difference Moisture Index, and the city name as inputs. 
    It then creates an empty geodataframe to save ndmi by date. The function loops through 
    each year in df_len and for each month in that year, to mask each and every raster 
    to its corresponding hexagon.
    Arguments:
        hex_gdf (geodata frame): Pass the hexagon geodataframe to the function
        df_len (int): Determine the number of years and months that are in the data
        index_analysis (str): Specify which index analysis to use
        city(str): Specify the city of interest
        raster_dir (str): Specify the directory where the raster files are stored
    Returns: 
    hex_raster (matrix): A geodataframe with the hexagon id
    """
    
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
    """
    The raster_to_hex function takes a hexagonal grid, a dataframe of dates, and the ndmi index
    to then return it into a geodataframe a  mean value for each hexagon in a grid for each date
    to offer a better classification in a csv file.
    Arguments:
        hex_gdf (geodataframe): Pass the hexagonal grid to the function
        df_len (int): Iterate through the dataframe containing the dates of each image
        r (int): Specify the resolution of the hexagons
        index_analysis (str): Select the index to be analyzed
        city (str): Specify the city to be analyzed
        raster_dir (str): Specify the directory where the raster files are stored
    Returns:
        hextmp (geodataframe): A geodataframe with the mean value of each index by hexagon and date

    """
    
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
    """
    The raster_to_hex_analysis function groups the raster by hexagons and calculates 
    summary statistics for each one of them.
    The function returns two dataframes: one with summary statistics for each of the hexagons
    (hexagon id, mean value of index analysis per year) and another with all values 
    from the raster assigned to their respective hexagon.
    Arguments:
        hex_gdf (geodataframe): Gets the hexagons
        df_len (dataframe): Divides the dataframe into chunks to be processed in parallel
        index_analysis (str): Specify the column name of the index we want to analyze
        tmp_dir (str): Store the raster files in a temporary directory
        city (str): city information from the dataframes
        res (int): Filters the hexagons dataframe by resolution
    Returns:
        hex_raster_analysis (matrix): Has the summary statistics for each of the hexagons
        hex_raster_df (dataframe): Has all values from the raster assigned to their respective hexagon
    """
    
    # group raster by hex

    log('Starting raster to hexagons')
    hex_gdf = hex_gdf.loc[hex_gdf.res==res].copy()
    hex_raster = raster_to_hex_multi(hex_gdf, df_len, index_analysis, city, tmp_dir)
    log('Assigned raster data to hexagons')
    
    # summary statistics
    hex_raster_analysis = hex_gdf[['hex_id','geometry','res']].drop_duplicates().copy()
    
    hex_raster_minmax = hex_raster[['hex_id',index_analysis,'year']].groupby(['hex_id','year']).agg(['max','min'])
    hex_raster_minmax.columns = ['_'.join(col) for col in hex_raster_minmax.columns]
    hex_raster_minmax = hex_raster_minmax.reset_index()
    hex_raster_minmax = hex_raster_minmax[['hex_id','ndvi_max','ndvi_min']].groupby(['hex_id']).mean()
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

def mosaic_process(links_band_1, links_band_2, band_name_list, tmp_dir=''):
    """
    The function takes in two lists of links to raster files, and a list of band names.
    It then mosaics the first list of links into one large array, and does the same for the second list.
    The function returns four objects: 
        1) The mosaic_band_array for band 1 (mosaic_band_2), 
        2) The mosaic_band array for band 2 (mosaic_band2), 
        3) A transformation matrix that can be used to transform coordinates from
        pixel space to map space (outtrans)  
        4) An object containing the metadata for the output file (out_meta)
    Arguments: 
        links_band_1 (list): Pass in the links for band 1
        links_band_2 (list): Get the output_transform and output_meta
        band_name_list (list): Name the output files
        tmp_dir (str): Specify a temporary directory to store the intermediate files
    Returns: 
        mosaic_band_1 (array): The mosaic array for band 1
        mosaic_band_2 (array): The mosaic array for band 2
        out_trans_band_2 (array): The transformation matrix for band 2
        out_meta (object): The metadata for the output file
        
    """
    mosaic_band_1, _,_ = mosaic_raster(links_band_1, tmp_dir, upscale=False)
    mosaic_band_1 = mosaic_band_1.astype('float32')
    log(f'Finished processing {band_name_list[0]}')
    mosaic_band_2, out_trans_band_2, out_meta = mosaic_raster(links_band_2)
    log(f'Finished processing {band_name_list[1]}')
    mosaic_band_2 = mosaic_band_2.astype('float32')
    log('Transformed band arrays to float32')
    log(f'array datatype: {mosaic_band_1.dtype}')
    return mosaic_band_1, mosaic_band_2, out_trans_band_2,out_meta


def create_raster_by_month(df_len, index_analysis, city, tmp_dir, 
                           band_name_list, date_list, gdf_bb, 
                           aoi, sat, time_exc_limit=600):
    """
    The create_raster_by_month function is used to create a raster for each month of the year.
    The function takes in a dataframe with the length of years and months, an index analysis, city name, 
    temporary directory path (tmp_dir), band name list (band_name_list), date list (date_list), 
    geodataframe bounding box(gdf_bb) and area of interest(aoi). 
    the function also performs raster analysis for each row of the DataFrame, downloads and processes 
    the raster data, calculates an index, crops the raster, performs interpolation, and 
    saves the processed rasters and corresponding metadata in the specified directory.
    Arguments:
        df_len (dataframe): Store the results of the analysis
        index_analysis (str): Define the index analysis
        city (str): Save the raster files based on the city name
        tmp_dir (str): Save the raster files in a temporary directory
        band_name_list (list): Define the bands that will be used in the analysis
        date_list (list): Define the dates that are used to download the images
        gdf_bb (geodataframe): Crop the raster to a specific area of interest
        aoi (str): Define the area of interest
        sat (str): Define the satellite used to gather data
        time_exc_limit (int): Set the time limit for downloading a raster
    Returns:
        df_len (dataframe): Store the results of the analysis
    
    """
    
    df_len['raster_row'] = np.nan
    df_len['raster_col'] = np.nan
    df_len['no_data_values'] = np.nan
    df_len['able_to_download'] = np.nan

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
        
        assets_hrefs = link_dict(band_name_list,items,date_list)
        
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
            df_raster.loc[df_raster.index==i,'able_to_download']=0
            df_raster.to_csv(df_file_dir, index=False)
            continue

        raster_index = (mosaic_band_1-mosaic_band_2)/(mosaic_band_1+mosaic_band_2)
        log(f'Calculated {index_analysis}')
        del mosaic_band_1
        del mosaic_band_2

        out_meta.update({"driver": "GTiff",
                    "dtype": 'float32',
                    "height": raster_index.shape[1],
                    "width": raster_index.shape[2],
                    "transform": out_trans_band_2})

        log('Starting save')

        with rasterio.open(f"{tmp_dir}{city}_{index_analysis}_{month_}_{year_}.tif", "w", **out_meta) as dest:
            dest.write(raster_index)

            dest.close()
            
        del raster_index
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
        df_raster.loc[((df_len['year']==year_)&
                (df_raster['month']==month_)),'able_to_download'] = 1
        
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

    df_len = pd.read_csv(df_file_dir)[['year','month','data_id','raster_row','raster_col','no_data_values','able_to_download']]

    return df_len


def raster_interpolation(df_len, city, tmp_dir, index_analysis): 
    """
    This function analyzes and interpolates the raster data that was downloaded.
    The function takes a pandas dataframe with the following columns:
            - month (int)
            - year (int)
            - raster_row (int)  # number of rows in raster file for that month/year combination, if available. If not available, NaN value will be present. 
            - raster_col (int)  # number of columns in raster file for that month/year combination, if available. If not available, NaN value will be present.  
    Arguments:
        df_len (dataframe): Pass the dataframe containing the information of each raster file
        city (str): Name the raster files
        tmp_dir (str): Specify the directory where the raster files are stored
        index_analysis (str) : Select the index to be analyzed
    Returns:
        df_len (dataframe): Returns the updated dataframe from the cvs document that arranged
        all the information of the raster files
    """
       
    row_mode = df_len.raster_row.mode().values[0]
    col_mode = df_len.raster_col.mode().values[0]
    mean_no_data = df_len[((df_len.raster_row == row_mode)|
            (df_len.raster_col == col_mode))].no_data_values.mean()
    stddev_no_data = df_len[((df_len.raster_row == row_mode)|
            (df_len.raster_col == col_mode))].no_data_values.std()
    
    log(f'Mean no-data:{mean_no_data}, Std no-data{stddev_no_data}, Rows: {row_mode}, Columns: {col_mode}')

    df_len.loc[(((df_len.raster_row < row_mode)|
            (df_len.raster_col < col_mode)|
            (df_len.no_data_values > mean_no_data+stddev_no_data))&
            (df_len.raster_col.notna())),'data_id'] = 0
 

    log(f'Interpolating {len(df_len.loc[df_len.data_id==0])}')

    pct_missing = len(df_len.loc[df_len.data_id==0]) / len(df_len)
    pct_missing = round(pct_missing,2)*100
    
    if pct_missing > 50:
        
        raise AvailableData('Missing more than 50 percent of data points')

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