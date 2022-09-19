import os
import sys

import pandas as pd
import geopandas as gpd
import numpy as np

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup
mpos = ["01001","01005","01011","02001","02002","02003", "02004", "02005","03003","04002","05009", "05017", "05035", "10007", "10012",
"05006", "05010", "05018", "05031", "05022", "05025","05004", "05027", "05030","06002", "06003", "06004", "06005", "06010","06001", "06009",
"07089", "07012", "07027", "07079", "07086", "07101", "08002", "08004", "08019", "08021", "08045", "08032", "08059", 
"08037", "09002", "09003", "09004", "09005", "09006", "09007", "09008", "09009", "09010", "09011", "09012", "09013", "09014", "09015", "09016", "09017", "13069", "15002", "15009", "15010", "15011", "15013", "15015", "15016", "15017", "15020", "15022", "15023", "15024", "15025", "15028", "15029", "15030", "15031", "15033", "15034", "15035", "15036", "15037", "15038", "15039", "15044", "15046", "15050", "15053", "15057", "15058", "15059", "15060", "15061", "15065", "15068", "15069", "15070", "15075", "15081", "15083", "15084", "15089", "15091", "15092", "15093", "15094", "15096", "15099", "15100", "15103", "15104", "15108", "15109", "15112", "15120", "15121", "15122", "15125" ,
"10005",  "11007", "11009", "11011", "11044", "11015", "11020", "11037", "11021", "11041", "11025", "11031", "12001", "12021", "12029", "12075", 
"13022", "13039", "13048", "13051", "13052", "13082", "13083", "13010", "13013", "13070", "13074", "13076", 
"13016", "13056", "13077", "14002", "14039", "14044", "14051", "14070", "14097", "14098", "14101", "14120", "14124", 
"14047", "14063", "14066", "14067", "18020", "15006", "15012", "15019", "15043", "15098", "15101", "15005", "15018", "15027", "15051", "15054", "15055", "15062", "15067", "15072", "15073", "15076", "15087", "15090", "15106", "15115", "15118",
"11023", "16069", "16022", "16053", "16088", "16043", "16108", "17002", "17004", "17006", "17026", "17029", "17030",
"17007", "17008", "17009", "17011", "17018", "17020", "17024", "17028", "18008", "18017", 
"19001", "19006", "19009", "19010", "19012", "19018", "19019", "19021", "19025", "19026", "19031", "19039", "19041", "19045", "19046", "19047", "19048", "19049",
"20045", "20063", "20067", "20083", "20087", "20091", "20107", "20115", "20157", "20174", "20227", "20293", "20338", "20350", "20375", "20385", "20390", "20399", "20403", "20409", "20519", "20539", "20553", "20565",
"20079", "20124", "20308", "20421", "20515", "21001", "21015", "21034", "21041", "21048", "21060", "21074", "21090", "21106", "21114", "21119", "21122", "21125", "21132", "21136", "21140", "21143", "21163", "21181", "29015", "29017", "29019", "29023", "29025", "29027", "29028", "29029", "29032", "29041", "29044", "29051", "29053", "29054", "29056", "29057", "29058", "29059",
"21149", "21156", "21054", "21174", "11004", "22006", "22008", "22011", "22014", "23003", "23005", "23004",
"24011", "24024", "24028", "24035", "24055", "25006", "25012", "26025", "26029", "26030", "26043", "27004", "27013",
"28041", "28022", "28027", "28032", "28033", "28003", "28009", "28038", "30123", "30133", "29001", "29002", "29005", "29009", "29010", "29018", "29024", "29026", "29031", "29033", "29035", "29036", "29038", "29039", "29043", "29048", "29049", "29050", "29060",
"30003", "30116", "30145", "30039", "30082", "30206", "30014", "30044", "30068", "30196", "30048", "30059", "30089", "30108", "30120", "30199",
"30022", "30030", "30074", "30081", "30085", "30099", "30101", "30115", "30118", "30135", "30138", "30140", "30185",
"30033", "30040", "30124", "30131", "30175", "30011", "30028", "30090", "30100", "30105", "30193", "30026", "30036", "30038", "30065", "30087", "30092", "30093", "30136", "30182",
"31002", "31013", "31038", "31041", "31050", "31063" , "31090", "31093", "31095", "31100", "31101",
"32017", "32032", "32050", "32056", "32057"]

edos = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20","21","22","23","24","25","26","27","28","29","30","31","32"]


for edo in edos:
    aup.log(f"starting state {edo}")
    ##DOWNLOAD HEXES
    #query = f"SELECT * FROM hexgrid.hexgrid_mx WHERE \"CVEGEO\" LIKE \'{edo}%%\'"
    #hex_8 = aup.gdf_from_query(query, geometry_col='geometry')
    #hex_8 = hex_8[hex_8['CVEGEO'].isin(mpos)]
    #aup.log(f"downloaded hex 8 for {edo}")
    query = f"SELECT * FROM hexgrid.hexgrid_9 WHERE \"CVEGEO\" LIKE \'{edo}%%\'"
    hex_8 = aup.gdf_from_query(query, geometry_col='geometry')
    hex_8 = hex_8[hex_8['CVEGEO'].isin(mpos)]
    aup.log(f"downloaded hex 9 for {edo}")
    #df_9 = hex_9.drop(['geometry'], axis = 1)
    df_8 = hex_8.drop(['geometry'], axis = 1)
    final_8 = pd.DataFrame(columns=['hex_id_9','CVEGEO_x', 11, 21, 22, 23, 31, 32, 33,
        43, 46, 48, 49, 51, 52, 53, 54, 55, 56, 61, 62, 71, 72, 81, 93, 99,'CVEGEO_y',
        '0 a 5 personas', '6 a 10 personas', '11 a 30 personas', '31 a 50 personas',
        '51 a 100 personas', '101 a 250 personas','251 y mas personas', 'no especificado',
        'year'])
    years = [10, 11, 13, 15, 16, 17, 18, 19, 20, 21, 22]
    if edo != '15':
        for year in years:
            aup.log(f"starting year 20{year}")
            if year == 10:
                gdf = gpd.read_file(f'/home/jovyan/work/data/denue/{edo}/20{year}/DENUE_{edo}.shp')
            elif year ==11:
                gdf = gpd.read_file(f'/home/jovyan/work/data/denue/{edo}/20{year}/denue{year}{edo}c_{edo}.shp')
            elif year ==13:
                gdf = gpd.read_file(f'/home/jovyan/work/data/denue/{edo}/20{year}/DENUE_Nacional_{edo}.shp')
            elif year ==15:
                gdf = gpd.read_file(f'/home/jovyan/work/data/denue/{edo}/20{year}/DENUE_INEGI_{edo}_.shp')
            elif year ==16:
                gdf = gpd.read_file(f'/home/jovyan/work/data/denue/{edo}/20{year}/denue_{edo}_shp/conjunto_de_datos/denue_inegi_{edo}_.shp')
            elif year ==17:
                gdf = gpd.read_file(f'/home/jovyan/work/data/denue/{edo}/20{year}/conjunto_de_datos/denue_inegi_{edo}_.shp')
            elif 18<=year <=22:
                if edo == '15':
                    p1 = gpd.read_file(f'/home/jovyan/work/data/denue/{edo}/20{year}/conjunto_de_datos/denue_inegi_15_1.shp')
                    p2 = gpd.read_file(f'/home/jovyan/work/data/denue/{edo}/20{year}/conjunto_de_datos/denue_inegi_15_2.shp')
                    gdf = p1.append(p2)
                else:
                    gdf = gpd.read_file(f'/home/jovyan/work/data/denue/{edo}/20{year}/conjunto_de_datos/denue_inegi_{edo}_.shp')
            gdf = gdf.to_crs("EPSG:4326")
            gdf.rename(columns = {'clase_act':'codigo_act', 'des_perocu':'per_ocu', 'CLASE_ACT':'codigo_act', 'PERS_OCUP':'per_ocu', 'PER_OCU':'per_ocu', 'CODIGO_ACT':'codigo_act' }, inplace = True)
            gdf['per_ocu'] = gdf['per_ocu'].str.lower()    
            #gdf['codigo_root'] = gdf['codigo_act'].str[:2]
            gdf['codigo_root'] = gdf.codigo_act.astype(str).str[:2].astype(int)
            ####### DO SPATIAL JOINS for RESOLUTION
            df_sjoin = gpd.sjoin(hex_8, gdf) #Spatial join Points to polygons
            ######## PIVOT FOR BUSINESS SIZE
            size_pivot = pd.pivot_table(df_sjoin,index='hex_id_9',columns='per_ocu',aggfunc={'per_ocu':len})
            size_pivot.columns = size_pivot.columns.droplevel()
            hex_size_8 = df_8.merge(size_pivot, how='left', on='hex_id_9')
            #### PIVOT FOR BUSINESS CATEGORY
            cat_pivot = pd.pivot_table(df_sjoin,index='hex_id_9',columns='codigo_root',aggfunc={'codigo_root':len})
            cat_pivot.columns = cat_pivot.columns.droplevel()
            hex_cat_8 = df_8.merge(cat_pivot, how='left', on='hex_id_9')
            #### TEMP MERGE
            temp_8 = hex_cat_8.merge(hex_size_8, how='left', on='hex_id_9')
            temp_8['year'] = ('20'+str(year))
            temp_8.rename(columns = {'251 y mã¡s pers':'251 y mas personas', '251 y mã¡s personas':'251 y mas personas',
                '251 y más personas': '251 y mas personas', '101 a 250 perso': '101 a 250 personas',
                '11 a 30 persona':'11 a 30 personas', '31 a 50 persona':'31 a 50 personas',
                '51 a 100 person':'51 a 100 personas'
                }, inplace = True)
            final_8 = final_8.append(temp_8)
            final_8 = final_8.fillna(0)
    #Due to memory constraints the hexes are uploaded in groups of 10,000
    c_hex = len(final_8)/10000
    for p in range(int(c_hex)+1):
        hex_upload = final_8.iloc[int(10000*p):int(10000*(p+1))].copy()
        aup.df_to_db_slow(hex_upload, "denue_change_hex9", 'denue_change', if_exists="append")
        aup.log(f"uploaded {p} / {c_hex} hex into DB ")
    aup.log("Finished uploading hexes ")