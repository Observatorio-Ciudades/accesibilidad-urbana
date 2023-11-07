import geopandas as gpd
import pandas as pd

import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


def main(city, res, mun_gdf, ageb_gdf, save=False):

    # create hexagons grid
    hex_gdf = aup.create_hexgrid(mun_gdf, res)

    aup.log(f'Created {len(hex_gdf)} hexagons for {city}')

    # hexagons to municipality
    hex_cnt = gpd.GeoDataFrame(geometry=hex_gdf.representative_point(),
                               crs="EPSG:4326")
    hex_cnt = hex_cnt.merge(hex_gdf[[f'hex_id_{res}']],
                            left_index=True, right_index=True)
    
    cnt_join = hex_cnt.sjoin(mun_gdf).drop(columns='index_right')

    hex_merge = hex_gdf.merge(cnt_join[[f'hex_id_{res}','CVEGEO','NOMGEO','city']],
             on=f'hex_id_{res}')
    aup.log(f'Merged hexagons with municipalities for {city}')
    aup.log(f'Total hex_gdf: {len(hex_gdf)} and hex_merge: {len(hex_merge)}')

    # hexagons to ageb - define urban/rural
    ageb_join = ageb_gdf.sjoin(hex_merge).drop(columns='index_right')

    pop_join = ageb_gdf.loc[ageb_gdf.cve_geo.isin(list(ageb_join.cve_geo.unique()))].pobtot.sum()
    pop_ageb = ageb_gdf.pobtot.sum()
    aup.log(f'Total ageb population: {pop_ageb} and joined ageb population: {pop_join}')

    total_ageb = ageb_gdf.shape[0]
    total_join = ageb_gdf.loc[ageb_gdf.cve_geo.isin(list(ageb_join.cve_geo.unique()))].shape[0]
    aup.log(f'Total ageb: {total_ageb} and joined ageb: {total_join}')

    # define urban and rural hexagons
    hex_list = list(ageb_join[f'hex_id_{res}'].unique())
    hex_merge.loc[:,'type'] = 'rural'
    hex_merge.loc[hex_merge[f'hex_id_{res}'].isin(hex_list),'type'] = 'urban'

    urban_len = len(hex_merge.loc[hex_merge['type'] == 'urban'])
    rural_len = len(hex_merge.loc[hex_merge['type'] == 'rural'])
    aup.log(f'Created {urban_len} urban and {rural_len} rural hexagons')

    if save:
        schema = 'hexgrid'
        table = f'hexgrid_{res}_city'

        aup.gdf_to_db_slow(hex_merge, schema, table, if_exists='append')


if __name__ == "__main__":
    aup.log('--'*20)
    aup.log('Starting script')

    res_list = [r for r in range(8,12)]

    schema = 'metropolis'
    table = 'metro_gdf_2020'

    query = f"SELECT DISTINCT city from {schema}.{table}"

    city_names = aup.df_from_query(query)

    aup.log(f"Downloaded city names for {len(city_names)} cities")

    save = False

    for city in city_names.city.unique():

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
            aup.log(f'Processing {city} with resolution {r}')
            main(city, r, mun_gdf, ageb_gdf, save=save)
