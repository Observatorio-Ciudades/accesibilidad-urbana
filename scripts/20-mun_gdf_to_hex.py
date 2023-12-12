import geopandas as gpd
import pandas as pd

import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


def main(city, res, mun_gdf, ageb_gdf, save=False):

    #############################################

    # assertion errors
    # dissolve mun_gdf and ageb_gdf
    mun_buff = mun_gdf.to_crs("EPSG:6372").buffer(500)
    mun_buff = mun_buff.to_crs("EPSG:4326")
    mun_buff = gpd.GeoDataFrame(geometry=mun_buff).dissolve()

    ageb_buff = ageb_gdf.to_crs("EPSG:6372").buffer(500)
    ageb_buff = ageb_buff.to_crs("EPSG:4326")
    ageb_buff = gpd.GeoDataFrame(geometry=ageb_buff).dissolve()

    # merge gdfs
    gdf_merge = pd.concat([ageb_buff, mun_buff])
    gdf_merge = gdf_merge.dissolve()


    #############################################

    # create hexagons grid
    hex_gdf = aup.create_hexgrid(gdf_merge, res)

    aup.log(f'Created {len(hex_gdf)} hexagons for {city}')

    # hexagons to municipality
    hex_cnt = gpd.GeoDataFrame(geometry=hex_gdf.representative_point(),
                               crs="EPSG:4326")
    hex_cnt = hex_cnt.merge(hex_gdf[[f'hex_id_{res}']],
                            left_index=True, right_index=True)
    
    cnt_join = hex_cnt.sjoin(mun_gdf).drop(columns='index_right')

    hex_merge = hex_gdf.merge(cnt_join[[f'hex_id_{res}','CVEGEO','NOMGEO','city']],
             on=f'hex_id_{res}', how='outer')
    aup.log(f'Merged hexagons with municipalities for {city}')
    aup.log(f'Total hex_gdf: {len(hex_gdf)} and hex_merge: {len(hex_merge)}')

    mun_hex_intersect = len(hex_merge.loc[hex_merge.CVEGEO.notna()])

    assert len(hex_gdf) == len(hex_merge), 'hex_gdf does not match hex_merge'

    # hexagons to ageb - define urban/rural
    ageb_join = ageb_gdf.sjoin(hex_merge).drop(columns='index_right')

    pop_join = ageb_gdf.loc[ageb_gdf.cve_geo.isin(list(ageb_join.cve_geo.unique()))].pobtot.sum()
    pop_ageb = ageb_gdf.pobtot.sum()
    aup.log(f'Total ageb population: {pop_ageb} and joined ageb population: {pop_join}')
    
    assert pop_ageb == pop_join, 'Population does not match'

    total_ageb = ageb_gdf.shape[0]
    total_join = ageb_gdf.loc[ageb_gdf.cve_geo.isin(list(ageb_join.cve_geo.unique()))].shape[0]
    aup.log(f'Total ageb: {total_ageb} and joined ageb: {total_join}')

    assert total_ageb == total_join, 'ageb does not match'

    # define urban and rural hexagons
    hex_list = list(ageb_join[f'hex_id_{res}'].unique())
    hex_merge.loc[:,'type'] = 'rural'
    hex_merge.loc[hex_merge[f'hex_id_{res}'].isin(hex_list),'type'] = 'urban'

    # fill missing data
    # fill CVEGEO, city and NOMGEO
    aup.log(f'Filling missing data for {city}')
    ageb_join.loc[:,'CVEGEO'] = ageb_join.cve_geo.str[:5]
    ageb_join.loc[:,'city'] = city
    ageb_join = ageb_join.drop(columns=['NOMGEO'])
    ageb_join = ageb_join.merge(mun_gdf[['CVEGEO','NOMGEO']], on='CVEGEO')

    # drop hex_id duplicates, preparing to join
    ageb_to_merge = ageb_join.drop_duplicates(subset=f'hex_id_{res}')

    aup.log('Dropped duplicates')

    # update missing data
    left_a = hex_merge.set_index(f'hex_id_{res}')
    right_a = ageb_to_merge[[f'hex_id_{res}','CVEGEO','city','NOMGEO']].set_index(f'hex_id_{res}')
    aup.log('Ready to update missing data')
    hex_fill = left_a.reindex(columns=left_a.columns.union(right_a.columns))
    hex_fill.update(right_a)
    hex_fill.reset_index(inplace=True)

    aup.log('Updated missing data')

    hex_fill = hex_fill.loc[~((hex_fill.CVEGEO.isna())&(hex_fill['type']=='rural'))] 

    urban_len = len(hex_fill.loc[hex_fill['type'] == 'urban'])
    rural_len = len(hex_fill.loc[hex_fill['type'] == 'rural'])
    aup.log(f'Created {urban_len} urban and {rural_len} rural hexagons')
    aup.log(f'Final hex_fill length: {len(hex_fill)} compaered to intersection: {mun_hex_intersect} length')

    if save:
        schema = 'hexgrid'
        table = f'hexgrid_{res}_city_2020'

        limit_len = 500000
        if len(hex_fill)>limit_len:
            c_upload = len(hex_fill)/limit_len
            for k in range(int(c_upload)+1):
                aup.log(f"Starting range k = {k} of {int(c_upload)}")
                hex_upload = hex_fill.iloc[int(limit_len*k):int(limit_len*(1+k))].copy()
                aup.gdf_to_db_slow(hex_upload, table, schema, if_exists='append')
        else:
            aup.gdf_to_db_slow(hex_fill, table, schema, if_exists='append')

    
if __name__ == "__main__":
    aup.log('--'*20)
    aup.log('Starting script')

    res_list = [r for r in range(8,12)]

    schema = 'metropolis'
    table = 'metro_gdf_2020'

    query = f"SELECT DISTINCT city from {schema}.{table}"

    city_names = aup.df_from_query(query)

    aup.log(f"Downloaded city names for {len(city_names)} cities")

    save = True

    # for city in city_names.city.unique():
    for city in city_names:
        # download municipality polygons
        schema = 'metropolis'
        table = 'metro_gdf_2020'

        query = f"SELECT * FROM {schema}.{table} WHERE \"city\" = \'{city}\'"

        mun_gdf = aup.gdf_from_query(query)

        aup.log(f'Downloaded {len(mun_gdf)} municipalities for {city}')

        # download ageb polygons

        schema = 'censoageb'
        table = 'censoageb_2020'

        ageb_gdf = gpd.GeoDataFrame()

        for cvegeo in mun_gdf.CVEGEO.unique():
            query = f"SELECT cve_geo,pobtot,geometry FROM {schema}.{table} WHERE \"cve_geo\" LIKE \'{cvegeo}%%\'"
            ageb_gdf = pd.concat([ageb_gdf, aup.gdf_from_query(query)],
                                ignore_index = True, axis = 0)
            
        aup.log(f'Downloaded {len(ageb_gdf)} ageb features for {city} with {ageb_gdf.pobtot.sum()} persons')

        for r in res_list:
            aup.log(f'\n Processing {city} with resolution {r}')
            try:
                main(city, r, mun_gdf, ageb_gdf, save=save)
            except:
                aup.log(f'Assertion error processing {city} with resolution {r}')
                continue
