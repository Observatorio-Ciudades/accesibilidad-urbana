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

def main(index_analysis, city, cvegeo_list, band_name_list, start_date, end_date, freq, satellite, save=False, del_data=False):

    ###############################
    # Download hex polygons with AGEB data
    schema_hex = 'censo'
    folder_hex = 'hex_bins_pop_2020'

    hex_ageb = gpd.GeoDataFrame()

    cvegeo_list = list(gdf_mun.loc[gdf_mun.city==city]["CVEGEO"].unique())

    for m in cvegeo_list:
        query = f"SELECT hex_id_8,geometry FROM {schema_hex}.{folder_hex} WHERE \"CVEGEO\" LIKE \'{m}%%\'"
        hex_ageb = pd.concat([hex_ageb, 
                            aup.gdf_from_query(query, geometry_col='geometry')], 
                            ignore_index = True, axis = 0)

    aup.log(f'Downloaded {len(hex_ageb)} hexagon features')
    

    df_len = aup.download_raster_from_pc(hex_ageb, index_analysis, city, freq,
                                        start_date, end_date, tmp_dir, band_name_list, satellite)

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

        aup.log(f'Translating raster to hexagon for res: {r}')

        hex_raster_analysis, df_raster_analysis = aup.raster_to_hex_analysis(hex_gdf, df_len, index_analysis,
                                                                    tmp_dir, city, r)
        aup.log('Finished assigning raster data to hexagons')
        aup.log(f'df nan values: {df_raster_analysis[index_analysis].isna().sum()}')
        if df_raster_analysis[index_analysis].isna().sum() > 0:
            raise NanValues('NaN values are still present after processing')

        if save:
            # hex_raster_analysis.to_file(tmp_dir+f'{city}_{index_analysis}_HexRes{r}.geojson')
            # df_raster_analysis.to_csv(tmp_dir+f'{city}_{index_analysis}_HexRes{r}.csv')
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

    aup.log(f'Finished processing city -- {city}')
    del hex_gdf

    if del_data:

        # delete raster files
        aup.delete_files_from_folder(tmp_dir)


if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')

    band_name_list = ['nir','red']
    index_analysis = 'ndvi'
    tmp_dir = f'../data/processed/tmp_{index_analysis}/'
    res = [8,11]
    freq = 'MS'
    start_date = '2018-01-01'
    end_date = '2022-12-31'
    satellite = "sentinel-2-l2a"
    save = True
    del_data = True

    df_skip_dir = f'../data/processed/{index_analysis}_skip_city/skip_list.csv'
    if os.path.exists(df_skip_dir) == False: # Or folder, will return true or false
        df_skip = pd.DataFrame(columns=['city','missing_months','unable_to_download'])
        df_skip.to_csv(df_skip_dir)
    else:
        df_skip = pd.read_csv(df_skip_dir)

    skip_list = list(df_skip.city.unique())

    gdf_mun = aup.gdf_from_db('metro_gdf', 'metropolis')
    gdf_mun = gdf_mun.sort_values(by='city')

    # prevent cities being analyzed to times in case of a crash
    aup.log('Downloading preprocessed data')
    processed_city_list = []
    try:
        query = f"SELECT city FROM raster_analysis.{index_analysis}_analysis_hex"
        processed_city_list = aup.df_from_query(query)
        processed_city_list = list(processed_city_list.city.unique())
    except:
        pass

    city_analysis = ['Guadalajara'] # Guaymas
    for city in gdf_mun.city.unique():

        if city not in processed_city_list and city not in skip_list:
        # if city in city_analysis:

            aup.log(f'\n Starting city {city}')

            cvegeo_list = list(gdf_mun.loc[gdf_mun.city==city]["CVEGEO"].unique())

            try:
                main(index_analysis, city, cvegeo_list, band_name_list, start_date,
                    end_date, freq, satellite, save, del_data)
            except:
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