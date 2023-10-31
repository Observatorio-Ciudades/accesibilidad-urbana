import geopandas as gpd
import pandas as pd

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

def main(index_analysis, city, cvegeo_list, band_name_dict, start_date, end_date, freq, satellite, query_sat, save=False, del_data=False):

    ###############################
    # Download hex polygons with AGEB data
    schema_hex = 'censo'
    folder_hex = 'hex_bins_pop_2020'

    hex_ageb = gpd.GeoDataFrame()

    # Donwload hex bins with population data
    for m in cvegeo_list:
        query = f"SELECT hex_id_8,geometry FROM {schema_hex}.{folder_hex} WHERE \"CVEGEO\" LIKE \'{m}%%\'"
        hex_ageb = pd.concat([hex_ageb, 
                            aup.gdf_from_query(query, geometry_col='geometry')], 
                            ignore_index = True, axis = 0)

    aup.log(f'Downloaded {len(hex_ageb)} hexagon features')
    

    df_len = aup.download_raster_from_pc(hex_ageb, index_analysis, city, freq,
                                        start_date, end_date, tmp_dir, band_name_dict, 
                                        query=query_sat, satellite=satellite)

    aup.log(f'Finished downloading and processing rasters for {city}')

    ### raster to hex
    ### hex preprocessing
    aup.log('Started creating hexagons at different resolutions')
    hex_gdf = hex_ageb.copy()
    hex_gdf.rename(columns={'hex_id_8':'hex_id'}, inplace=True)
    hex_gdf['res'] = res[0]

    if len(res)>1:
        for r in range(res[0]+1,res[-1]+1):
            
            hex_tmp = aup.create_hexgrid(hex_ageb, r)
            hex_tmp.rename(columns={f'hex_id_{r}':'hex_id'}, inplace=True)
            hex_tmp['res'] = r
            
            hex_gdf = pd.concat([hex_gdf, hex_tmp], 
                ignore_index = True, axis = 0)
            
            del hex_tmp
    else:
        hex_gdf = aup.create_hexgrid(hex_gdf, res[0])
        hex_gdf.rename(columns={f'hex_id_{res[0]}':'hex_id'}, inplace=True)
        hex_gdf['res'] = res[0]

    aup.log('Finished creating hexagons at different resolutions')

    for r in list(hex_gdf.res.unique()):

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

def raster_to_hex_save(hex_gdf_i, df_len, index_analysis, tmp_dir, city, r, save, i=0):
    aup.log(f'Translating raster to hexagon for res: {r}')

    hex_raster_analysis, df_raster_analysis = aup.raster_to_hex_analysis(hex_gdf_i, df_len, index_analysis,
                                                                tmp_dir, city, r)
    hex_raster_analysis['temp_diff_mean'] = hex_raster_analysis[f'{index_analysis}_mean'] - hex_raster_analysis[f'{index_analysis}_mean'].mean()

    aup.log('Finished assigning raster data to hexagons')
    aup.log(f'df nan values: {df_raster_analysis[index_analysis].isna().sum()}')
    if df_raster_analysis[index_analysis].isna().sum() > 0:
        raise NanValues('NaN values are still present after processing')

    if save:
        # local save
        # hex_raster_analysis.to_file(tmp_dir+'local_save/'+f'{city}_{index_analysis}_HexRes{r}_v{i}.geojson')
        # df_raster_analysis.to_csv(tmp_dir+'local_save/'+f'{city}_{index_analysis}_HexRes{r}_v{i}.csv')
        
        # upload to database
        
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

    band_name_dict = {'lwir11':[False],
                 'eq':["((lwir11*0.00341802) + 149.0)-273.15"]}
    query_sat = {"eo:cloud_cover": {"lt": 20},
              "platform": {"in": ["landsat-8", "landsat-9"]}}
    index_analysis = 'temperature'
    tmp_dir = f'../data/processed/tmp_{index_analysis}/'
    res = [8,11] # 8, 11
    freq = 'MS'
    start_date = '2018-01-01'
    end_date = '2022-12-31'
    satellite = 'landsat-c2-l2'
    save = False # True
    del_data = False # True

    # check if a skip city csv exists
    df_skip_dir = f'../data/processed/{index_analysis}_skip_city/skip_list.csv'
    if os.path.exists(df_skip_dir) == False: # Or folder, will return true or false
        df_skip = pd.DataFrame(columns=['city','missing_months','unable_to_download'])
        df_skip.to_csv(df_skip_dir)
    else:
        df_skip = pd.read_csv(df_skip_dir)
    # gather the city names from the skip city csv
    skip_list = list(df_skip.city.unique())

    # download the cities GeoDataFrames from the database
    gdf_mun = aup.gdf_from_db('metro_gdf', 'metropolis')
    gdf_mun = gdf_mun.sort_values(by='city')

    # prevent cities being analyzed several times in case of a crash
    aup.log('Downloading preprocessed data')
    processed_city_list = []
    try:
        query = f"SELECT city FROM raster_analysis.{index_analysis}_analysis_hex"
        processed_city_list = aup.df_from_query(query)
        processed_city_list = list(processed_city_list.city.unique())
    except:
        pass

    city_analysis = ['Monterrey'] # Guaymas

    for city in gdf_mun.city.unique():

        aup.log(f'\n Iterating {city}')

        # if city not in processed_city_list and city not in skip_list:
        if city in city_analysis and city not in processed_city_list and city not in skip_list:

            aup.log(f'\n Starting city {city}')

            cvegeo_list = list(gdf_mun.loc[gdf_mun.city==city]["CVEGEO"].unique())
            if city == 'ZMVM':
                cvegeo_list = ["09002", "09003", "09004", "09005", "09006", 
                            "09007", "09008", "09009", "09010", "09011", 
                            "09012", "09013", "09014", "09015", "09016", "09017"]

            try:
                main(index_analysis, city, cvegeo_list, band_name_dict, start_date,
                    end_date, freq, satellite, query_sat, save, del_data)
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
                    not_donwloadable = len(df_raster.loc[df_raster.able_to_download==0])
                    df_skip.loc[len(df_skip),'missing_months'] = missing_months
                    df_skip.loc[len(df_skip),'unable_to_download'] = not_donwloadable
                df_skip.to_csv(df_skip_dir, index=False)
                if del_data:
                    # delete raster files
                    aup.delete_files_from_folder(tmp_dir)
                pass