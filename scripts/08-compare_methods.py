import os
import sys
import numpy as np
import pandas as pd
import geopandas as gpd



module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup




### this function checks distance to parameters and makes the exception for mixed level schools
def check(schema, type, save=False):
    ## calls from DB the grid *hexagonal or square* that already has the distances for 
    # elementary, mixed level, middle school, health, recreation, and supply
    hex = aup.gdf_from_db(type+'_dist', 'infonavit')
    #created gdf to store the distance in the grid figure to each type of school
    schools = gpd.GeoDataFrame()
    schools = hex[['dist_primaria', 'dist_secundaria', 'dist_mixto']]
    schools['prim'] = 0
    schools['secu'] = 0
    #starts iterating over every row, if they are within the desired distance, the gdf schools will get a 1 on the row, otherwise a 0.
    for r in range(int(len(schools))):
        prim = schools.iloc[r,0]
        secu = schools.iloc[r,1]
        mixto = schools.iloc[r,2]
        if 0<prim<= 2000:
            schools.iat[r, 3] = 1
        if 0<secu <= 2500:
            schools.iat[r, 4] = 1
        #In case of mixed level schools e make it sort of a wild card. If the figure already has an elementary within distance, the mixed level school may
        #fulfill the need of a middle school (if it is not within distance). and viceversa. however if the grid lacks both types of school
        #the mixed level school cannot replace both.
        if 0<mixto <= 2000 and prim >2000:
            if secu<= 2500:
                schools.iat[r, 3] = 1
        if 0<mixto <= 2500 and secu >2500:
            if prim <2000:
                schools.iat[r, 4] = 1
    #similar to the scools df, we create another one to verify the distance to the othe amenities
    otros = hex[['dist_salud', 'dist_abasto', 'dist_recreacion']]
    otros['salud'] = 0
    otros['abasto'] = 0
    otros['recreacion'] = 0
    #iterates over rows and sotres distance in 'otros' If the amenity is within the desired diatnce it sores a 1 otherwise a 0
    for r in range(int(len(otros))):
        salud = otros.iloc[r,0]
        abasto = otros.iloc[r,1]
        rec = otros.iloc[r,2]
        if 0<salud<= 2500:
            otros.iat[r, 3] = 1
        if 0<abasto <= 2500:
            otros.iat[r, 4] = 1
        if 0<rec <= 2000:
            otros.iat[r, 5] = 1

    #we merge the gdfs with the 0s and 1s
    merge = schools.merge(otros, left_index=True, right_index=True)
    #we add the values into a new column to know how many types of amenities are within the desired distance
    verify = merge[['prim', 'secu', 'salud', 'abasto', 'recreacion']]
    verify['fulfill'] = verify.sum(axis =1)
    #we finally create the gdf bool that has the geometry and the 1s 0s and the total numer of amenities per figure
    bool = gpd.GeoDataFrame()
    bool = hex[[ 'geometry']]
    bool = bool.merge(verify, left_index=True, right_index=True)
    bool = bool.reset_index()
    #Upload to DB
    aup.gdf_to_db_slow(bool, type+'_check', 'infonavit', if_exists = "append")

#This function works comparing two different types of methods, however it cannot run comparing a square grid and a hex grid. It only works with H3 grids
# because it needs to have the same index code in both methods to compare
def compare(schema, compare1, compare2, save = False):
    #Obtain from DB the hexes of both methods and set the hex id as index.
    obscd = aup.gdf_from_db(compare2+'_check', 'infonavit')
    obscd = obscd.set_index('hex_id_8')

    infon = aup.gdf_from_db(compare1+'grid_check', 'infonavit')
    infon = infon.set_index('hex_id_8')

    #We create a ne gdf with the columns fulfill to check
    obscd_merge = gpd.GeoDataFrame()
    infon_merge = gpd.GeoDataFrame()
    obscd_merge['check_obscd'] = obscd['fulfill']
    infon_merge['infon_obscd'] = infon['fulfill']

    check = obscd_merge.merge(infon_merge, left_index=True, right_index=True)
    check['verify'] = 0
    #iterates over every row checking how many amenities the hex has within distance.
    #If the OBSCD and INFONAVIT method both ave all 5, it stores a 5.
    #If only the OBSCD method has all 5 it sotres a 4
    #If only the INFONAVIT method has all 5 it stores a 3
    #If no methods have it, it stores a -1
    for r in range(int(len(check))):
        obscd_val = check.iloc[r,0]
        infon_val = check.iloc[r,1]
        if obscd_val == 5 and infon_val == 5:
            check.iat[r,2] = 5
        if obscd_val == 5 and infon_val != 5:
            check.iat[r,2] = 4
        if obscd_val != 5 and infon_val == 5:
            check.iat[r,2] = 3
        if obscd_val != 5 and infon_val != 5:
            check.iat[r,2] = -1
    check = check.set_index('hex_id_8')

    hex_base = obscd[['CVEGEO', 'geometry']]
    #We create the hex gdf that has the geometry and the compare values
    upload_hex = hex_base.merge(check, left_index=True, right_index=True)
    #reset index
    upload_hex = upload_hex.reset_index()
    #upload to gdf
    aup.gdf_to_db_slow(upload_hex, 'compare_bool', 'infonavit')


if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')
    schema = 'infonavit'
    save = True
    figure = 'grid'
    compare1 = 'infonavit_hex'
    compare2 = 'obscd_hex'
    ### possible figures: grid, infonavit_hex, obscd_hex
    check(schema, type=figure, save = save)
    compare(schema, compare1=compare1, compare2 =compare2, save = save)









