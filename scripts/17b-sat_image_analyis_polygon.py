import geopandas as gpd
import pandas as pd

import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup
else:
    import aup


class NanValues(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

def main(aoi_gdf, index_analysis, city, band_name_dict, start_date, end_date, freq, satellite, sat_query, save=False, del_data=False):

    # ------------------------------ CREATION OF AREA OF INTEREST ------------------------------
    # Create area of interest with biggest hexs
    big_res = min(res)
    poly = aoi_gdf.to_crs(projection_crs).buffer(500).reset_index()
    poly = poly.rename(columns={0:'geometry'})
    poly = gpd.GeoDataFrame(poly, geometry='geometry')
    poly = poly.to_crs("EPSG:4326")
    hex_aoi = aup.create_hexgrid(poly, big_res)

    aup.log(f'Created {len(hex_aoi)} hexagon features')
    
    # ------------------------------ DOWNLOAD AND PROCESS RASTERS ------------------------------
    df_len = aup.download_raster_from_pc(hex_aoi, index_analysis, city, freq,
                                         start_date, end_date, tmp_dir, band_name_dict,
                                         satellite=satellite, query=sat_query, projection_crs=projection_crs,
                                         compute_unavailable_dates=True)
    aup.log(f'Finished downloading and processing rasters for {city}')

    # ------------------------------ RASTERS TO HEX ------------------------------
    if raster_to_hex:
        ### hex preprocessing
        aup.log('Started loading hexagons at different resolutions')
        
        # Create res_list
        res_list=[]
        for r in range(res[0],res[-1]+1):
            res_list.append(r)
        
        # Load hexgrids
        hex_gdf = hex_aoi.copy()
        hex_gdf.rename(columns={f'hex_id_{big_res}':'hex_id'}, inplace=True)
        hex_gdf['res'] = big_res

        aup.log(f'Loaded hexgrid res {big_res}')
        hex_diss = hex_aoi.dissolve()
        
        for r in res_list:
            # biggest resolution already loaded
            if r == big_res:
                continue
            
            # Load hexgrid
            hex_tmp = aup.create_hexgrid(hex_diss, r)
            # Format hexgrid
            hex_tmp.rename(columns={f'hex_id_{r}':'hex_id'}, inplace=True)
            hex_tmp['res'] = r
            # Concatenate to hex_gdf
            hex_gdf = pd.concat([hex_gdf, hex_tmp])

            aup.log(f'Loaded hexgrid res {r}')

            del hex_tmp

        aup.log('Finished creating hexagons at different resolutions')

        # Raster to hex function for each resolution (saves output)
        for r in list(hex_gdf.res.unique()):

            aup.log(f'---------------------------------------')
            aup.log(f'STARTING processing for resolution {r}.')

            # 20,000 max. on DELL laptop, crashed with 50,000
            # 100,000 works on Alienware laptop.
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
                    raster_to_hex_save(hex_gdf_i, df_len, index_analysis, tmp_dir, city, r, save, local_save, i)

            else:
                aup.log('hex_gdf len smaller than processing chunk')
                hex_gdf_i = hex_gdf_res.copy()
                raster_to_hex_save(hex_gdf_i, df_len, index_analysis, tmp_dir, city, r, save, local_save)

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

        if r == 8:
            # df upload
            aup.df_to_db_slow(df_raster_analysis, f'{index_analysis}_complete_dataset_hex',
                            'raster_analysis', if_exists='append', chunksize=upload_chunk)
            # gdf upload
            aup.gdf_to_db_slow(hex_raster_analysis, f'{index_analysis}_analysis_hex',
                            'raster_analysis', if_exists='append')

        else:
            # df upload
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
            # gdf upload
            aup.gdf_to_db_slow(hex_raster_analysis, f'{index_analysis}_analysis_hex',
                            'raster_analysis', if_exists='append')
        aup.log(f'Finished uploading data for res{r}')
        
    # delete variables
    del df_raster_analysis
    del hex_raster_analysis

if __name__ == "__main__":
    aup.log('--'*50)
    aup.log('--- STARTING SCRIPT 17b.')

    # ------------------------------ SCRIPT CONFIGURATION - ANALYSIS ------------------------------
    band_name_dict = {'nir':[True], #If GSD(resolution) of band is different, set True.
                      'swir16':[False], #If GSD(resolution) of band is different, set True.
                      'eq':['(nir-swir16)/(nir+swir16)']
                      } 
    index_analysis = 'ndmi'
    tmp_dir = f'../data/processed/tmp_{index_analysis}/'
    res = [8,11]                                                    # Commonly used: [8, 11]
    freq = 'MS'
    start_date = '2016-05-01'
    end_date = '2024-12-31'
    satellite = "sentinel-2-l2a"                                    # Commonly used: "sentinel-2-l2a","landsat-c2-l2"
    sat_query = {"eo:cloud_cover": {"lt": 10}}                      # Commonly used: {"eo:cloud_cover": {"lt": 10}}, {'plataform':{'in':['landsat-8','landsat-9']}}
    del_data = False # Del rasters after processing

    # ------------------------------ SCRIPT CONFIGURATION - AREA OF INTEREST ------------------------------
    city = 'Caracterizacion_aoi' #city in this case is area of interest name
    aoi_gdf = gpd.read_file('../data/external/temporal_todocker/2025_caracterizacion_forestal/AreaEstudio_CaracterizacionForestal_UTM_v1.gpkg')
    projection_crs = "EPSG:32614"

    # ------------------------------ SCRIPT CONFIGURATION - SAVING ------------------------------
    raster_to_hex = True #------ Can set False if testing/visualizing downloaded/interpolated rasters. Set True if transfering data to hexs and saving. 
    local_save = True #------ Set True if saving locally
    save = False #------ Set True if saving to database

    # ------------------------------ SCRIPT START ------------------------------
    # Create folder to store city skip_list csv
    folder_dir = f'../data/processed/{index_analysis}_skip_city/'
    if os.path.exists(folder_dir) == False:
        os.mkdir(folder_dir)
    
    # Create city skip_list csv
    df_skip_dir = f'../data/processed/{index_analysis}_skip_city/skip_list.csv'
    if os.path.exists(df_skip_dir) == False: # Or folder, will return true or false
        df_skip = pd.DataFrame(columns=['city','missing_months','unable_to_download'])
        df_skip.to_csv(df_skip_dir, index=False)
    else:
        df_skip = pd.read_csv(df_skip_dir)
    
    # Read current cities to skip
    skip_list = list(df_skip.city.unique())

    # Create folder to store raster analysis
    if os.path.exists(tmp_dir) == False:
        os.mkdir(tmp_dir)

    # Run script
    main(aoi_gdf, index_analysis, city, band_name_dict, start_date,
         end_date, freq, satellite, sat_query, save, del_data)
    

    ##### ##### ##### ##### ##### ##### ##### ##### ##### #####
    # RASTER ANALYSIS EXAMPLES - PREVIOUSLY USED SCRIPT CONFIGURATIONS

    ##### Temperature analysis in Santiago, Chile
    # ------------------------------ SCRIPT CONFIGURATION - ANALYSIS ------------------------------
    # band_name_dict = {'lwir11':[False],
    #              'eq':["((lwir11*0.00341802) + 149.0)-273.15"]}
    # index_analysis = 'temperature'
    # res = [8,11]
    # freq = 'MS'
    # start_date = '2019-01-01'
    # end_date = '2023-12-31'
    # satellite = "landsat-c2-l2"
    # sat_query = {"eo:cloud_cover": {"lt": 20},
    #           "platform": {"in": ["landsat-8", "landsat-9"]}}
    # ------------------------------ SCRIPT CONFIGURATION - AREA OF INTEREST ------------------------------
    # city = 'Santiago'
    # aoi_gdf = gpd.read_file('../data/external/municipio_santiago/PoligonoSantiago.shp')
    # aoi_gdf = aup.gdf_from_db('santiago_aoi','projects_research')
    # projection_crs = "EPSG:32719"


    ##### NDVI analysis in Medellín, Colombia 
    # ------------------------------ SCRIPT CONFIGURATION - ANALYSIS ------------------------------
    # band_name_dict = {'nir08':[False], #If GSD(resolution) of band is different, set True.
    #                   'red':[False], #If GSD(resolution) of band is different, set True.
    #                   'eq':['(nir08-red)/(nir08+red)']}
    # index_analysis = 'ndvi'
    # res = [8,11]
    # freq = 'MS'
    # start_date = '2019-01-01'
    # end_date = '2023-12-31'
    # satellite = "landsat-c2-l2"  #NOTE: USED DIFFERENT SATELLITE THAN USUAL. band_name_dict, sat_query and satellite CHANGED.
    # sat_query = {'plataform':{'in':['landsat-8','landsat-9']}}
    # ------------------------------ SCRIPT CONFIGURATION - AREA OF INTEREST ------------------------------
    # city = 'Medellin'
    # aoi_gdf = gpd.read_file('../data/external/municipio_medellin/medellin_urban_gcs.geojson')
    # projection_crs = "EPSG:32618"


    ##### NDWI analysis in Chapala, Mexico
    # ------------------------------ SCRIPT CONFIGURATION - ANALYSIS ------------------------------
    # band_name_dict = {'green':[False], #If GSD(resolution) of band is different, set True.
    #                  'nir':[False], #If GSD(resolution) of band is different, set True.
    #                  'eq':['(green-nir)/(green+nir)']} 
    # index_analysis = 'ndwi'
    # res = [8,10]
    # freq = 'MS'
    # start_date = '2019-01-01'
    # end_date = '2023-12-31'
    # satellite = "sentinel-2-l2a"
    # sat_query = {"eo:cloud_cover": {"lt": 40}}
    # ------------------------------ SCRIPT CONFIGURATION - AREA OF INTEREST ------------------------------
    # city = 'Chapala'
    # aoi_gdf = gpd.read_file('../data/external/temporal_todocker/chapala/chapala_polygon.gpkg')
    # projection_crs = "EPSG:6372"


    ##### EVI analysis in Sierra Gorda, Querétaro, Mexico
    # ------------------------------ SCRIPT CONFIGURATION - ANALYSIS ------------------------------
    #band_name_dict = {'blue': [False], #If GSD(resolution) of band is different, set True.
    #                  'red': [False], #If GSD(resolution) of band is different, set True.
    #                  'nir': [False], #If GSD(resolution) of band is different, set True.
    #                  'eq': ["2.5 * ((nir - red) / (nir + 6 * red - 7.5 * blue + 1))"]
    #                  }
    #index_analysis = 'evi'
    #res = [8,11]
    #freq = 'MS'
    #start_date = '2016-05-01'
    #end_date = '2024-12-31'
    #satellite = "sentinel-2-l2a"
    #sat_query = {"eo:cloud_cover": {"lt": 10}}
    # ------------------------------ SCRIPT CONFIGURATION - AREA OF INTEREST ------------------------------
    #city = 'Caracterizacion_aoi' #city in this case is area of interest name
    #aoi_gdf = gpd.read_file('../data/external/temporal_todocker/2025_caracterizacion_forestal/AreaEstudio_CaracterizacionForestal_UTM_v1.gpkg')
    #projection_crs = "EPSG:32614"

    ##### NDMI analysis in Sierra Gorda, Querétaro, Mexico
    # ------------------------------ SCRIPT CONFIGURATION - ANALYSIS ------------------------------
    #band_name_dict = {'nir':[True], #If GSD(resolution) of band is different, set True.
    #                  'swir16':[False], #If GSD(resolution) of band is different, set True.
    #                  'eq':['(nir-swir16)/(nir+swir16)']
    #                  } 
    #index_analysis = 'ndmi'
    #res = [8,11]
    #freq = 'MS'
    #start_date = '2016-05-01'
    #end_date = '2024-12-31'
    #satellite = "sentinel-2-l2a"
    #sat_query = {"eo:cloud_cover": {"lt": 10}}
    # ------------------------------ SCRIPT CONFIGURATION - AREA OF INTEREST ------------------------------
    #city = 'Caracterizacion_aoi' #city in this case is area of interest name
    #aoi_gdf = gpd.read_file('../data/external/temporal_todocker/2025_caracterizacion_forestal/AreaEstudio_CaracterizacionForestal_UTM_v1.gpkg')
    #projection_crs = "EPSG:32614"


           