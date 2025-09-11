import pandas as pd
from spatial_kde import spatial_kernel_density
from ast import literal_eval
import geopandas as gpd
import pandas as pd
import numpy as np
import momepy as mp

import rasterio as ro
import distancerasters as dr

import os
import sys

module_path = os.path.abspath(os.path.join('../../'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup
else:
    import aup



# Función para clasificar según la terminación de codigo_act
def asignar_tipo(codigo):
    if pd.isna(codigo):  # Si está vacío
        return 'Sin código'

    # Asegurar que sea string para evaluar el código
    codigo_str = str(codigo).strip()

    if not codigo_str.isdigit():
        return 'Código inválido'

    # Define tus conjuntos de códigos
    categorias = {
        'agropecuario': ['11'],

        'industria': {'31', '32', '33'},

        'servicios':{'48', '49','51', '52', '53', '54',
                     '55','56','81'},

        'alojamiento': {'72'},

        'comercio': {'43', '46'},

        'cultural_recreativo': {'71'},

        'educacion': {'61'},

        'salud': {'62'},

        'gobierno': {'93'},

        'otros': {'21','22','23'} }

    for k in categorias.keys():
        if codigo_str[:2] in categorias[k]:
            return k

def number_of_jobs(per_ocu):
    jobs_dict = {'0 a 5 personas':3,
                '6 a 10 personas':8,
                '11 a 30 personas':20,
                '31 a 50 personas':40,
                '51 a 100 personas':75,
                '101 a 250 personas':175,
                '251 y más personas':325}
    per_ocu_num = jobs_dict[per_ocu]
    return per_ocu_num


def process_block_activities(idx, block_gdf, denue_gdf, output_dir):
    """Process all activities for a single block"""
    cvegeo_mza = block_gdf.cvegeo_mza
    denue_block = denue_gdf.loc[denue_gdf.cvegeo_mza == cvegeo_mza].copy()

    if denue_block.empty:
        return f"Skipped {cvegeo_mza} - no data"

    results = []
    for act in denue_block.tipo_act.unique():
        output_file = f"{output_dir}kde_mnz_{cvegeo_mza}_{act}.tif"
        denue_act = denue_block.loc[denue_block.tipo_act==act].copy()
        denue_act = denue_act.reset_index(drop=True)

        try:
            spatial_kernel_density(
                points=denue_act,
                radius=(denue_block['d_mean'].mean() / 2),
                output_path=output_file,
                output_pixel_size=1.0,
                output_driver="GTiff",
                weight_col="per_ocu_num",
            )
            results.append(f"Completed {cvegeo_mza}_{act}")
        except Exception as e:
            results.append(f"Failed {cvegeo_mza}_{act}: {str(e)}")

    return results


def bld_tess_overlapping_fix(tess_gdf, bld_gdf):
    tess_bld_intersect = tess_gdf.overlay(bld_gdf[['full_plus_code','geometry']],
                   how='union', keep_geom_type=True)

    tess_bld_intersect.loc[tess_bld_intersect.full_plus_code_1.isna(),
        'full_plus_code_1'] = tess_bld_intersect.loc[tess_bld_intersect.full_plus_code_1.isna(),
            'full_plus_code_2']

    tess_bld_intersect.loc[tess_bld_intersect.full_plus_code_2.isna(),
        'full_plus_code_2'] = tess_bld_intersect.loc[tess_bld_intersect.full_plus_code_2.isna(),
            'full_plus_code_1']


    tess_bld_intersect.loc[
    tess_bld_intersect.full_plus_code_1==tess_bld_intersect.full_plus_code_2,'full_plus_code'] = tess_bld_intersect.loc[
    tess_bld_intersect.full_plus_code_1==tess_bld_intersect.full_plus_code_2,'full_plus_code_1']

    tess_bld_intersect.loc[
    tess_bld_intersect.full_plus_code_1!=tess_bld_intersect.full_plus_code_2,'full_plus_code'] = tess_bld_intersect.loc[
    tess_bld_intersect.full_plus_code_1!=tess_bld_intersect.full_plus_code_2,'full_plus_code_2']

    tess_bld_intersect = tess_bld_intersect.dissolve(by='full_plus_code')
    tess_bld_intersect = tess_bld_intersect.reset_index()
    tess_bld_intersect = tess_bld_intersect.drop(columns=['full_plus_code_1','full_plus_code_2'])

    return tess_bld_intersect

def fix_overlapping_bld(gdf):
    # intersect buildings with themsleves
    bld_int = gdf[['full_plus_code','geometry']].overlay(gdf[['full_plus_code','geometry']],
                                                         how='intersection', keep_geom_type=True)
    # count the number of intersections per building
    bld_counter = bld_int.groupby('full_plus_code_1').count()
    # filter building with at least two overlaps
    bld_counter = bld_counter.loc[bld_counter.geometry>=2].reset_index()

    processed_int = [] # save processed buildings to avoid duplicates

    for code_int_1 in bld_counter.full_plus_code_1:
        # identify buildings to be processed
        bld_int_list = [code_int_1]
        # gather the code of the buildings that overlap with code_1
        bld_int_list.extend(list(bld_int.loc[bld_int.full_plus_code_1==code_int_1].full_plus_code_2))
        bld_int_list = list(set(bld_int_list)) # remove duplicate values

        # recalculate area
        gdf['area_m2'] = gdf.area

        # filter complete buildings from original GeoDataFrame
        bld_tmp = gdf.loc[gdf.full_plus_code.isin(bld_int_list)].copy()

        # iterate over smaller overlapping areas
        for i,_ in bld_tmp.loc[bld_tmp.full_plus_code!=code_int_1].iterrows():
            code_int_2 = bld_tmp.loc[i,'full_plus_code']

            # check if buildings where previously processed
            if code_int_1+'-'+code_int_2 in processed_int:
                continue

            area_code_int_1 = bld_tmp.loc[bld_tmp.full_plus_code==code_int_1].area_m2.item()
            area_code_int_2 = bld_tmp.loc[bld_tmp.full_plus_code==code_int_2].area_m2.item()

            # compare and arrange areas
            if (area_code_int_1 > area_code_int_2):

                bld_larger = bld_tmp.loc[bld_tmp.full_plus_code==code_int_1].copy()
                bld_smaller = bld_tmp.loc[bld_tmp.full_plus_code==code_int_2].copy()
                updated_geometry_code = code_int_2 # save code according to smaller geometry to save diff later

            elif (area_code_int_1 < area_code_int_2):

                bld_larger = bld_tmp.loc[bld_tmp.full_plus_code==code_int_2].copy()
                bld_smaller = bld_tmp.loc[bld_tmp.full_plus_code==code_int_1].copy()
                updated_geometry_code = code_int_1 # save code according to smaller geometry to save diff later

            else:

                bld_larger = bld_tmp.loc[bld_tmp.full_plus_code==code_int_1].copy()
                bld_smaller = bld_tmp.loc[bld_tmp.full_plus_code==code_int_2].copy()
                updated_geometry_code = code_int_2 # save code according to smaller geometry to save diff later

            # if buildings haven't been processed analyze the case
            processed_int.append(code_int_1+'-'+code_int_2)
            processed_int.append(code_int_2+'-'+code_int_1)

            # calculate geometric difference
            geom_diff = bld_smaller.difference(bld_larger, align=False)
            gdf.loc[gdf.full_plus_code==updated_geometry_code, 'geometry'] = geom_diff.item()
            gdf['area_in_meters'] = gdf.area

    return gdf

def building_tesselation(cvegeo, block_gdf, bld_block):
    """Process all buildings for a single block"""
    bld_filter = bld_block.loc[bld_block.CVEGEO == cvegeo].copy()
    block_filter = block_gdf.loc[block_gdf.CVEGEO == cvegeo].copy()

    # skip if there aren't any buildings for that specific block
    if bld_filter.empty:
        return None

    # check if buildings overlap
    gdf_int = bld_filter[['full_plus_code','geometry']].overlay(
        bld_filter[['full_plus_code','geometry']],
        how='intersection',
        keep_geom_type=True
    )

    # if buildings overlap, use fix function
    if len(gdf_int) > len(bld_filter):
        try:
            bld_filter = fix_overlapping_bld(bld_filter)
        except:
            return None

    try:
        tess_tmp = mp.morphological_tessellation(bld_filter, clip=block_filter)
    except:
        return None

    # transfer code and building area data
    tess_tmp = tess_tmp.reset_index().rename(columns={'index':'fid'})
    bld_filter = bld_filter[
        ['full_plus_code','area_in_meters','geometry']].reset_index().rename(
            columns={'index':'fid'}).copy()
    tess_tmp = tess_tmp.merge(bld_filter.drop(columns=['geometry']), on=['fid'])
    tess_tmp = tess_tmp.drop(columns=['fid'])

    try:
        tess_tmp = bld_tess_overlapping_fix(tess_tmp, bld_filter)
    except:
        return None

    # assign block's geographic code
    tess_tmp['CVEGEO'] = cvegeo

    return tess_tmp, bld_filter

def kde_to_area_of_prediction(cvegeo, aop_gdf, kde_dir):
    """Process to transfer data from kde raster to area of prediction tesselations by block"""
    kde_block = f'kde_mnz_{cvegeo}'
    aop_tmp = aop_gdf.loc[aop_gdf.CVEGEO==cvegeo].copy()

    # iterate over every file
    for filename in os.listdir(kde_dir):

        # gather those corresponding to the specific block
        if filename.startswith(kde_block):

            # skip complementary raster files
            if filename.endswith('.aux.xml'):
                continue

            kde_act = filename.replace(kde_block+'_',"").replace('.tif',"").lower()

            # read file
            raster_kde = ro.open(kde_dir+filename)

            aop_tmp[kde_act] = aop_tmp.geometry.apply(
                lambda geom: aup.clean_mask(geom, raster_kde)).apply(np.ma.mean)
    return aop_tmp


def check_for_lists(val):
    try:
        val = literal_eval(val)
        return val
    except:
        return val

def raster_conditional(rarray):
    return (rarray == 1)

def road_type_to_area_of_prediction(aop_road, edges, road_class, road_type, pixel_size, bounds, output_dir):

    aup.log(f"Processing road type: {road_class}")

    edges_road_type = edges.loc[edges.highway.isin(road_type)].copy()

    rv_array, affine = dr.rasterize(edges_road_type,
                                    pixel_size=pixel_size,
                                    bounds=bounds,
                                    output=output_dir+f"{road_class}_rasterized.tif")

    # generate distance array and output to geotiff
    my_dr = dr.DistanceRaster(rv_array, affine=affine,
                          output_path=output_dir+f"{road_class}_distance.tif",
                          conditional=raster_conditional)

    raster_distance = ro.open(output_dir+f"{road_class}_distance.tif")

    aop_road[road_class+'_distance'] = aop_road.geometry.apply(
                lambda geom: aup.clean_mask(geom, raster_distance)).apply(np.ma.mean)

    aop_road = aop_road[['fid',road_class+'_distance']].copy()

    return aop_road
