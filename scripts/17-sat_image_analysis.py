import geopandas as gpd
import pandas as pd

import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup
else :
    import aup


class NanValues(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

def main(index_analysis, city, band_name_dict, start_date, end_date, freq,
satellite, query_sat={}, save_db=False, local_save=False, del_data=False,
download_raster=True, data_to_hex=False, processed_data={}):

    # ------------------------------ CREATION OF AREA OF INTEREST ------------------------------
    # Create city area of interest with biggest hexs
    big_res = min(res)
    schema_hex = 'hexgrid'
    table_hex = f'hexgrid_{big_res}_city_2020'

    # Download hexagons with type=urban
    type = 'urban'
    query = f"SELECT hex_id_{big_res},geometry FROM {schema_hex}.{table_hex} WHERE \"city\" = \'{city}\' AND \"type\" = \'{type}\'"
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

    if download_raster:

        # ------------------------------ DOWNLOAD AND PROCESS RASTERS ------------------------------
        df_len = aup.download_raster_from_pc(hex_city, index_analysis, city, freq,
                                            start_date, end_date, tmp_dir, band_name_dict,
                                            query=query_sat, satellite = satellite,
                                            compute_unavailable_dates=False) # Determine whether to interpolate dates

        aup.log(f'Finished downloading and processing rasters for {city}')
    else:
        df_len = pd.read_csv(tmp_dir+index_analysis+f'_{city}_dataframe.csv')

    if data_to_hex:

        # ------------------------------ RASTERS TO HEX ------------------------------
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

        aup.log(f'Loaded hexgrid res {big_res}')

        for r in res_list:
            # biggest resolution already loaded
            if r == big_res:
                continue

            # Load hexgrid
            table_hex = f'hexgrid_{r}_city_2020'
            query = f"SELECT hex_id_{r},geometry FROM {schema_hex}.{table_hex} WHERE \"city\"=\'{city}\' AND  (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\'))"
            hex_tmp = aup.gdf_from_query(query, geometry_col='geometry')
            # Format hexgrid
            hex_tmp.rename(columns={f'hex_id_{r}':'hex_id'}, inplace=True)
            hex_tmp['res'] = r
            # Concatenate to hex_gdf
            hex_gdf = pd.concat([hex_gdf, hex_tmp])

            aup.log(f'Loaded hexgrid res {r}')

            del hex_tmp

        aup.log('Finished creating hexagons at different resolutions')

        # remove preprocessed hexagons
        if city in processed_data.keys():
            hex_gdf = hex_gdf.loc[~hex_gdf.res.isin(processed_data[city])].copy()
            aup.log(f'Processing only hexagons for resolutions {hex_gdf.res.unique()}')

        # Raster to hex function for each resolution (saves output)
        for r in list(hex_gdf.res.unique()):

            aup.log(f'---------------------------------------')
            aup.log(f'STARTING processing for resolution {r}.')

            processing_chunk = 100000

            # filters hexagons at specified resolution
            hex_gdf_res = hex_gdf.loc[hex_gdf.res==r].copy()
            hex_gdf_res = hex_gdf_res.reset_index(drop=True)

            if len(hex_gdf_res)>processing_chunk:
                aup.log(f'hex_gdf_res len: {len(hex_gdf_res)} is bigger than processing chunk: {processing_chunk}')
                c_processing = len(hex_gdf_res)/processing_chunk
                aup.log(f'There are {round(c_processing)} processes')
                for i in range(int(c_processing)+1):
                    aup.log(f'Processing from {i*processing_chunk} to {(i+1)*processing_chunk}')
                    hex_gdf_i = hex_gdf_res.iloc[int(processing_chunk*i):int(processing_chunk*(1+i))].copy()
                    raster_to_hex_save(hex_gdf_i, df_len, index_analysis, tmp_dir, city, r, save_db, local_save, i)

            else:
                aup.log('hex_gdf len smaller than processing chunk')
                hex_gdf_i = hex_gdf_res.copy()
                raster_to_hex_save(hex_gdf_i, df_len, index_analysis, tmp_dir, city, r, save_db, local_save)

        aup.log(f'Finished processing city -- {city}')
        del hex_gdf

        # Delete city's raster files
        if del_data:
            aup.delete_files_from_folder(tmp_dir)

def raster_to_hex_save(hex_gdf_i, df_len, index_analysis, tmp_dir, city, r, save_db, local_save, i=0):
    aup.log(f'Translating raster to hexagon for res: {r}')

    hex_raster_analysis, df_raster_analysis = aup.raster_to_hex_analysis(hex_gdf_i, df_len, index_analysis,
                                                                tmp_dir, city, r)
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
    if save_db:
        upload_chunk = 150000
        aup.log(f'Starting upload for res: {r}')
        aup.gdf_to_db_slow(hex_raster_analysis, f'{index_analysis}_analysis_hex',
                            'raster_analysis', if_exists='append')

        # Removed process since dissagregated hexagon data isn't uploaded anymore

        # if r == 8:
            # df upload
            # aup.df_to_db_slow(df_raster_analysis, f'{index_analysis}_complete_dataset_hex',
            #                'raster_analysis', if_exists='append', chunksize=upload_chunk)
            # gdf upload
            # aup.gdf_to_db_slow(hex_raster_analysis, f'{index_analysis}_analysis_hex',
            #                'raster_analysis', if_exists='append')

        # else:
            # df upload
            # limit_len = 5000000
            # if len(df_raster_analysis)>limit_len:
            #     c_upload = len(df_raster_analysis)/limit_len
            #     for k in range(int(c_upload)+1):
            #         aup.log(f"Starting range k = {k} of {int(c_upload)}")
                    # df_inter_upload = df_raster_analysis.iloc[int(limit_len*k):int(limit_len*(1+k))].copy()
                    # aup.df_to_db(df_inter_upload,f'{index_analysis}_complete_dataset_hex',
                    #                'raster_analysis', if_exists='append')
            # else:
                # aup.df_to_db(df_raster_analysis,f'{index_analysis}_complete_dataset_hex',
                #                    'raster_analysis', if_exists='append')
            # gdf upload
            # aup.gdf_to_db_slow(hex_raster_analysis, f'{index_analysis}_analysis_hex',
            #                 'raster_analysis', if_exists='append')
        aup.log(f'Finished uploading data for res{r}')

    # delete variables
    del df_raster_analysis
    del hex_raster_analysis

if __name__ == "__main__":
    aup.log('--'*20)
    aup.log('Starting script')

    # band_name_dict = {'nir':[True], #If GSD(resolution) of band is different, set True.
    #                   'swir16':[False], #If GSD(resolution) of band is different, set True.
    #                  'eq':['(nir-swir16)/(nir+swir16)']}
    band_name_dict = {'nir':[False], #If GSD(resolution) of band is different, set True.
                        'red':[False], #If GSD(resolution) of band is different, set True.
                       'eq':['(nir-red)/(nir+red)']}
    # band_name_dict = {'lwir11':[False],
    #                  'eq':["((lwir11*0.00341802) + 149.0)-273.15"]}

    index_analysis = 'ndvi'
    # tmp_dir = f'../data/processed/tmp_{index_analysis}/'
    tmp_dir = f'/mnt/ext_ssd/Repos/observatorio-ciudades/accesibilidad-urbana/data/tmp_{index_analysis}/'
    res = [8,11] # 8, 11
    freq = 'MS'
    start_date = '2018-01-01'
    end_date = '2023-12-31'
    # satellite = "landsat-c2-l2"
    satellite = "sentinel-2-l2a"
    del_data = False

    # sat_query = {"eo:cloud_cover": {"lt": 15},
    #               "platform": {"in": ["landsat-8", "landsat-9"]}}
    sat_query = {"eo:cloud_cover": {"lt": 15}}

    download_raster = True
    data_to_hex = True

    local_save = False #------ Set True if test
    save_db = True #------ Set True if full analysis

    ###############################
    # Create folder to store city skip_list csv
    folder_dir = f'../data/processed/{index_analysis}_skip_city/'

    if os.path.exists(folder_dir) == False:
        os.mkdir(folder_dir)

    # Create city skip_list csv
    df_skip_dir = f'../data/processed/{index_analysis}_skip_city/skip_list.csv'
    if os.path.exists(df_skip_dir) == False:
        df_skip = pd.DataFrame(columns=['city','missing_months','unable_to_download'])
        df_skip.to_csv(df_skip_dir, index=False)
    else:
        df_skip = pd.read_csv(df_skip_dir)

    # Read current cities to skip
    skip_list = list(df_skip.city.unique())

    # Create folder to store raster analysis
    if os.path.exists(tmp_dir) == False:
        os.mkdir(tmp_dir)

    # Read all cities
    gdf_mun = aup.gdf_from_db('metro_gdf_2020', 'metropolis')
    gdf_mun = gdf_mun.sort_values(by='city')

    # Prevent cities being analyzed several times in case of a crash
    aup.log('Downloading preprocessed data')
    processed_city_dict = {}
    try:
        # remove query to process every city
        query = f"SELECT city,res FROM raster_analysis.{index_analysis}_analysis_hex"
        processed_city_dict = aup.df_from_query(query)
        processed_city_dict = processed_city_dict.groupby('city')['res'].apply(set).to_dict()
        aup.log('Currently processed data:')
        aup.log(processed_city_dict)
        # temporary code to finish uploading Toluca
        # processed_city_dict['Toluca'].discard(11)
        # aup.log(processed_city_dict)
        pass
    except:
        aup.log('Could not download preprocessed data')
        pass

    #---------------------------------------
    #------ Set following if test, else comment
    # NDMI processed cities
    '''processed_city_list = ['Aguascalientes','CDMX','Chihuahua','Chilpancingo','Ciudad Obregon',
                 'Colima','Culiacan','Delicias','Durango','Ensenada','Guadalajara','Guanajuato',
                 'Hermosillo','La Paz','Laguna','Los Cabos','Matamoros','Mexicali','Mazatlan',
                 'Monclova','Monterrey','Nogales','Nuevo Laredo','Oaxaca','Pachuca','Piedad',
                 'Piedras Negras','Poza Rica','Puebla','Reynosa','Queretaro','San Martin','Tapachula',
                 'Tehuacan','Tepic','Tijuana','Tlaxcala','Toluca','Tulancingo','Tuxtla',
                 'Uruapan','Vallarta','Victoria','Villahermosa','Xalapa','Zacatecas','Zamora']'''
    # NDVI Processed cities
    '''processed_city_list = ['Acapulco','Aguascalientes','CDMX','Chihuahua','Chilpancingo','Ciudad Obregon',
                 'Colima','Cuautla','Cuernavaca','Delicias','Durango','Ensenada','Guadalajara',
                 'Hermosillo','Juarez','La Paz','Laguna','Los Cabos','Los Mochis','Matamoros','Mexicali','Mazatlan',
                 'Monclova','Monterrey','Nogales','Nuevo Laredo','Oaxaca','Pachuca','Poza Rica',
                 'Piedras Negras','Puebla','Quereatro','Reynosa','San Martin','Tapachula',
                 'Tehuacan','Tepic','Tijuana','Tlaxcala','Toluca','Tulancingo','Tuxtla',
                 'Uruapan','Vallarta','Victoria','Villahermosa','Xalapa','Zacatecas','Zamora',
                 'ZMVM']'''
    # city_list = ['CDMX']
    # city_list = ['La Paz','Laguna','Leon','Los Cabos','Matamoros','Mazatlan','Merida','Monclova','Monterrey','Nogales','Nuevo Laredo']
    #for city in city_list:

    #------ Set following if all-cities analysis, else comment
    for city in gdf_mun.city.unique():
        # Process each available city
        # if (city not in processed_city_list) and (city not in skip_list):
        # if (city in processed_city_list) and ((city not in processed_city_dict.keys()) or
        #     (len(processed_city_dict[city])<4)) and (city not in skip_list):
        if ((city not in processed_city_dict.keys()) or
            (len(processed_city_dict[city])<4)) and (city not in skip_list):
    #---------------------------------------
            aup.log(f'\n Starting city {city}')

            # Try process and save (Successful city)
            try:
                main(index_analysis, city, band_name_dict, start_date,
                    end_date, freq, satellite,
                    query_sat=sat_query, save_db=save_db, local_save=local_save,
                    del_data=del_data, download_raster=download_raster,
                    data_to_hex=data_to_hex, processed_data=processed_city_dict)

            # Except, register failure (Unsuccessful city)
            except Exception as e:
                aup.log(e)
                aup.log(f'Error with city {city}')

                df_skip.loc[len(df_skip)+1,'city'] = city
                df_file_dir = tmp_dir+index_analysis+f'_{city}_dataframe.csv'
                # If didn't create df_file_dir, register missing months unable to download as -1
                if os.path.exists(df_file_dir) == False:
                    df_skip.loc[len(df_skip),'missing_months'] = -1
                    # df_skip.loc[len(df_skip),'unable_to_download'] = -1
                # Else, register values
                else:
                    df_raster = pd.read_csv(df_file_dir)
                    missing_months = len(df_raster.loc[df_raster.data_id==0])
                    # not_donwloadable = len(df_raster.loc[df_raster.able_to_download==0])
                    not_downloadable = -1
                    # df_skip.loc[len(df_skip),'missing_months'] = missing_months
                    # df_skip.loc[len(df_skip),'unable_to_download'] = not_donwloadable
                # Save city to skip_list csv
                df_skip.to_csv(df_skip_dir, index=False)
                # Delete city's raster files
                if del_data:
                    aup.delete_files_from_folder(tmp_dir)
                pass
