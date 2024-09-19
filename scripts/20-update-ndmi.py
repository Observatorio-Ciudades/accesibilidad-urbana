import geopandas as gpd
import pandas as pd
import pymannkendall as mk

import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


class NanValues(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

def main(index_analysis, city, band_name_dict, start_date, end_date, freq, satellite, query_sat={}, save=False, del_data=False):

    ###############################
    ### Create city area of interest with biggest hexs
    big_res = min(res)
    schema_hex = 'hexgrid'
    table_hex = f'hexgrid_{big_res}_city_2020'

    # Download hexagons with type=urban
    type = 'urban'
    query = f"SELECT hex_id_{big_res},geometry FROM {schema_hex}.{table_hex} WHERE \"city\" = '{city}\' AND \"type\" = '{type}\'"
    hex_urban = aup.gdf_from_query(query, geometry_col='geometry')
    
    # Download hexagons with type=rural within 500m buffer
    poly = hex_urban.to_crs("EPSG:6372").buffer(500).reset_index()
    poly = poly.to_crs("EPSG:4326")
    poly_wkt = poly.dissolve().geometry.to_wkt()[0]
    type = 'rural'
    query = f"SELECT hex_id_{big_res},geometry FROM {schema_hex}.{table_hex} WHERE \"city\" = '{city}\' AND \"type\" = '{type}\' AND (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\'))"
    hex_rural = aup.gdf_from_query(query, geometry_col='geometry')
    
    # Concatenate urban and rural hex
    hex_city = pd.concat([hex_urban, hex_rural])

    aup.log(f'Downloaded {len(hex_city)} hexagon features')
    
    ### Download and process rasters
    df_len = aup.download_raster_from_pc(hex_city, index_analysis, city, freq,
                                        start_date, end_date, tmp_dir, band_name_dict, 
                                        query=query_sat, satellite = satellite)

    aup.log(f'Finished downloading and processing rasters for {city}')

    ### raster to hex
    ### hex preprocessing
    aup.log('Started loading hexagons at different resolutions')
    
    # Create res_list
    res_list=[]
    for r in range(res[0],res[-1]+1):
        res_list.append(r)

    # Load hexgrids
    hex_gdf = hex_city.copy()
    hex_gdf.rename(columns={f'hex_id_{big_res}':'hex_id'}, inplace=True)
    hex_gdf['res'] = big_res
    
    for r in res_list:
        # biggest resolution already loaded
        # if r == big_res:
        #    continue
        
        # Load hexgrid
        table_hex = f'hexgrid_{r}_city_2020'
        query = f"SELECT hex_id_{r},geometry FROM {schema_hex}.{table_hex} WHERE \"city\" = '{city}\' AND (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\'))"
        hex_tmp = aup.gdf_from_query(query, geometry_col='geometry')
        # Format hexgrid
        hex_tmp.rename(columns={f'hex_id_{r}':'hex_id'}, inplace=True)
        hex_tmp['res'] = r
        # Concatenate to hex_gdf
        hex_gdf = pd.concat([hex_gdf, hex_tmp])

        del hex_tmp

    aup.log('Finished creating hexagons at different resolutions')

    for r in list(hex_gdf.res.unique()):

        # biggest resolution already loaded
        # if r != 11:
        #     aup.log(f'Skipping res: {r}')
        #     continue

        processing_chunk = 100000

        # filters hexagons at specified resolution
        hex_gdf_res = hex_gdf.loc[hex_gdf.res==r].copy()
        hex_gdf_res = hex_gdf_res.reset_index(drop=True)

        if len(hex_gdf_res)>processing_chunk:
            aup.log(f'hex_gdf_res len: {len(hex_gdf_res)} is bigger than processing chunk: {processing_chunk}')
            c_processing = len(hex_gdf_res)/processing_chunk
            aup.log(f'There are {round(c_processing)+1} processes')
            for i in range(int(c_processing)+1):
                aup.log(f'Processing from {i*processing_chunk} to {(i+1)*processing_chunk}')
                hex_gdf_i = hex_gdf_res.iloc[int(processing_chunk*i):int(processing_chunk*(1+i))].copy()
                raster_to_hex_save(hex_gdf_i, df_len, index_analysis, tmp_dir, city, r, save, i)

        else:
            aup.log('hex_gdf len smaller than processing chunk')
            hex_gdf_i = hex_gdf_res.copy()
            raster_to_hex_save(hex_gdf_i, df_len, index_analysis, tmp_dir, city, r, save)

    aup.log(f'Finished processing city -- {city}')
    del hex_gdf

    if del_data:
        # delete raster files
        aup.delete_files_from_folder(tmp_dir)


def raster_to_hex_analysis_update(hex_gdf, df_len, index_analysis, tmp_dir, city, res):
    """
    The function groups the raster by hexagons and calculates 
    summary statistics for each one of them.
    The function returns a dataframe and a geodataframe: one with summary statistics for each of the hexagons
    (hexagon id, mean value of index analysis per year) and another with all values 
    from the raster assigned to their respective hexagon.

    Arguments:
        hex_gdf (geopandas.GeoDataFrame): Gets the hexagons
        df_len (pandas.DataFrame): Divides the dataframe into chunks to be processed in parallel
        index_analysis (str): Specify the column name of the index we want to analyze
        tmp_dir (str): Store the raster files in a temporary directory
        city (str): city information from the dataframes
        res (int): hexagon resolution used to filter the dataframe by resolution

    Returns:
        hex_raster_analysis (geopandas.GeoDataFrame): Has the summary statistics for each of the hexagons
        hex_raster_df (pandas.DataFrame): Has all values from the raster assigned to their respective hexagon
    """
    
    # group raster by hex

    aup.log('Starting raster to hexagons')
    hex_gdf = hex_gdf.copy()
    hex_raster = aup.raster_to_hex_multi(hex_gdf, df_len, index_analysis, city, tmp_dir)
    aup.log('Assigned raster data to hexagons')

    # download raster data for previous years
    aup.log("Downloading raster data for previous years")
    processed_hex_id_list = hex_raster.hex_id.unique()
    query = f"SELECT * FROM raster_analysis.{index_analysis}_complete_dataset_hex WHERE \"hex_id\" IN {str(tuple(processed_hex_id_list))} AND \"city\" = '\{city}\' AND \"res\" = \'{res}\'"
    hex_raster_previous = aup.df_from_query(query)
    hex_raster_previous = hex_raster_previous.drop(columns=['city','res'])

    # merge raster data
    aup.log("Merging raster data")
    hex_raster = pd.concat([hex_raster, hex_raster_previous])
    
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

    aup.log(f'df nan values: {hex_raster_df[index_analysis].isna().sum()}')

    return hex_raster_analysis, hex_raster_df

def raster_to_hex_save(hex_gdf_i, df_len, index_analysis, tmp_dir, city, r, save, i=0):
    aup.log(f'Translating raster to hexagon for res: {r}')

    hex_raster_analysis, df_raster_analysis = raster_to_hex_analysis_update(hex_gdf_i, df_len, index_analysis,
                                                                tmp_dir, city, r)
    # transform res to int
    hex_raster_analysis['res'] = hex_raster_analysis['res'].astype(int)
    df_raster_analysis['res'] = df_raster_analysis['res'].astype(int)
    
    aup.log('Finished assigning raster data to hexagons')
    aup.log(f'df nan values: {df_raster_analysis[index_analysis].isna().sum()}')
    if df_raster_analysis[index_analysis].isna().sum() > 0:
        raise NanValues('NaN values are still present after processing')
    
    # local save (test)
    if local_save:
        # Create folder to store local save
        localsave_dir = tmp_dir+'local_save/'
        if os.path.exists(localsave_dir) == False:
            os.mkdir(localsave_dir)

        # Local save
        hex_raster_analysis.to_file(tmp_dir+'local_save/'+f'{city}_{index_analysis}_HexRes{r}_v{i}.geojson')
        df_raster_analysis.to_csv(tmp_dir+'local_save/'+f'{city}_{index_analysis}_HexRes{r}_v{i}.csv')

    # Save - upload to database
    if save:
        upload_chunk = 150000
        aup.log('Starting upload')

        if r == 8:

            aup.df_to_db_slow(df_raster_analysis, f'{index_analysis}_complete_dataset_hex',
                            'raster_analysis', if_exists='append', chunksize=upload_chunk)

            aup.gdf_to_db_slow(hex_raster_analysis, f'{index_analysis}_analysis_hex',
                            'raster_analysis', if_exists='append')

        else:
            limit_len = 5000000
            if len(df_raster_analysis)>limit_len:
                c_upload = len(df_raster_analysis)/limit_len
                for k in range(int(c_upload)+1):
                    aup.log(f"Starting range k = {k} of {int(c_upload)}")
                    df_inter_upload = df_raster_analysis.iloc[int(limit_len*k):int(limit_len*(1+k))].copy()
                    aup.df_to_db(df_inter_upload,f'{index_analysis}_complete_dataset_hex',
                                    'raster_analysis', if_exists='append')
            else:
                aup.df_to_db(df_raster_analysis,f'{index_analysis}_complete_dataset_hex',
                                    'raster_analysis', if_exists='append')
            aup.gdf_to_db_slow(hex_raster_analysis, f'{index_analysis}_analysis_hex',
                            'raster_analysis', if_exists='append')
            
        aup.log(f'Finished uploading data for res{r}')
        
    # delete variables
    del df_raster_analysis
    del hex_raster_analysis

if __name__ == "__main__":
    aup.log('--'*20)
    aup.log('Starting script')

    band_name_dict = {'nir':[True], #If GSD(resolution) of band is different, set True.
                      'swir16':[False], #If GSD(resolution) of band is different, set True.
                      'eq':['(nir-swir16)/(nir+swir16)']} 
    index_analysis = 'ndmi'
    tmp_dir = f'../data/processed/tmp_{index_analysis}/'
    res = [8,11] # 8, 11
    freq = 'MS'
    start_date = '2023-01-01'
    end_date = '2023-12-31'
    satellite = "sentinel-2-l2a"
    del_data = True
    query_sat = {"eo:cloud_cover": {"lt": 10}}

    local_save = False #------ Set True if test
    save = True #------ Set True if full analysis

    ###############################
    # Create folder to store city skip_list
    folder_dir = f'../data/processed/{index_analysis}_skip_city/'
    if os.path.exists(folder_dir) == False:
        os.mkdir(folder_dir)

    df_skip_dir = f'../data/processed/{index_analysis}_skip_city/skip_list.csv'
    if os.path.exists(df_skip_dir) == False: # Or folder, will return true or false
        df_skip = pd.DataFrame(columns=['city','missing_months','unable_to_download'])
        df_skip.to_csv(df_skip_dir)
    else:
        df_skip = pd.read_csv(df_skip_dir)

    skip_list = list(df_skip.city.unique())

    # Create folder to store raster analysis
    if os.path.exists(tmp_dir) == False:
        os.mkdir(tmp_dir)

    gdf_mun = aup.gdf_from_db('metro_gdf_2020', 'metropolis')
    gdf_mun = gdf_mun.sort_values(by='city')

    # prevent cities being analyzed several times in case of a crash
    aup.log('Downloading preprocessed data')
    processed_city_list = []
    try:
        query = f"SELECT city FROM raster_analysis.{index_analysis}_complete_dataset_hex GROUP BY city"
        processed_city_list = aup.df_from_query(query)
        processed_city_list = list(processed_city_list.city.unique())
    except:
        pass

    #------ Set following if test
    # city_list = ['ZMVM']
    # for city in city_list:
    finish_list = ['CDMX','Aguascalientes','Chihuahua','Chilpancingo','Ciudad Obregon',
                   'Colima','Culiacan','Delicias','Durango','Ensenada','Guadalajara']

    #------ Set following if full analysis
    for city in gdf_mun.city.unique():

        if city in finish_list:
            continue

        # if city not in processed_city_list and city not in skip_list:
        if city in processed_city_list and city not in skip_list:

            aup.log(f'\n Starting city {city}')

            try:
                main(index_analysis, city, band_name_dict, start_date,
                    end_date, freq, satellite,
                    query_sat=query_sat, save=save, del_data=del_data)
            except Exception as e:
                aup.log(e)
                aup.log(f'Error with city {city}')
                df_skip.loc[len(df_skip)+1,'city'] = city
                df_file_dir = tmp_dir+index_analysis+f'_{city}_dataframe.csv'
                if os.path.exists(df_file_dir) == False: # Or folder, will return true or false
                    df_skip.loc[len(df_skip),'missing_months'] = -1
                    df_skip.loc[len(df_skip),'unable_to_download'] = -1
                else:
                    df_raster = pd.read_csv(df_file_dir)
                    missing_months = len(df_raster.loc[df_raster.data_id==0])
                    # not_donwloadable = len(df_raster.loc[df_raster.able_to_download==0])
                    not_donwloadable = -1
                    df_skip.loc[len(df_skip),'missing_months'] = missing_months
                    df_skip.loc[len(df_skip),'unable_to_download'] = not_donwloadable
                df_skip.to_csv(df_skip_dir, index=False)
                if del_data:
                    # delete raster files
                    aup.delete_files_from_folder(tmp_dir)
                pass