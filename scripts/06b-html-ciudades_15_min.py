import os
import sys

import boto3
import geopandas as gpd
import pandas as pd
from keplergl import KeplerGl

module_path = os.path.abspath(os.path.join("../"))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


def load_data(city, gdf_mun):
    # Creates query to download hex bins
    schema_hex = 'prox_analysis'
    table_hex = 'hex8_15_min'

    if city == 'ZMVM':
        cvegeo = ['09002','09003','09004','09005',
        '09006','09007','09008','09009',
        '09010','09011','09012','09013',
        '09014','09015','09016','09017']

        gdf_tmp = gdf_mun.loc[gdf_mun.CVEGEO.isin(cvegeo)].copy()
        gdf_tmp = gdf_tmp.to_crs("EPSG:6372")
        gdf_tmp = gdf_tmp.buffer(1).reset_index().rename(columns={0:'geometry'})
        gdf_tmp = gpd.GeoDataFrame(gdf_tmp, geometry='geometry')
        gdf_tmp = gdf_tmp.to_crs("EPSG:4326")
        poly_wkt = gdf_tmp.dissolve().geometry.to_wkt()[0]

        query = f"SELECT * FROM {schema_hex}.{table_hex} WHERE (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\'))"
        hex_gdf = aup.gdf_from_query(query, geometry_col="geometry")

        # query = f"SELECT * FROM {hexgrid_schema}.{hexgrid_folder} WHERE \"CVEGEO\" IN {str(tuple(cvegeo))}"
        # hex_mun = aup.gdf_from_query(query, geometry_col="geometry")
        # hex_codes = list(hex_mun.hex_id_8.unique())
        # query = f"SELECT * FROM {schema_hex}.{table_hex} WHERE \"hex_id_8\" IN {str(tuple(hex_codes))}"
        # hex_gdf = aup.gdf_from_query(query, geometry_col="geometry")

    else:
        query = f"SELECT * FROM {schema_hex}.{table_hex} WHERE \"city\" LIKE '{city}'"
        hex_gdf = aup.gdf_from_query(query, geometry_col="geometry")

    return hex_gdf


def make_html(hex_gdf, city, save=False):
    # recalculate max time to amenity
    hex_gdf['max_idx_15_min'] = hex_gdf[['max_escuelas','max_servicios comunitarios',
                                   'max_comercio','max_entretenimiento']].max(axis=1)

    # prepare columns for kepler visualization
    columns_kepler = ['max_idx_15_min', 'max_escuelas', 'max_servicios comunitarios',
                      'max_comercio', 'max_entretenimiento','pobtot',
                  'dens_pobha','pobfem','pobmas','pob_0a14',
                      'pob_15a24', 'pob_25a59','p_60ymas']

    for c in columns_kepler:
        if c == 'dens_pobha':
            hex_gdf[c] = hex_gdf[c].round(2)
            hex_gdf[c] = hex_gdf[c].astype(str) + ' (pob/ha)'
        else:
            hex_gdf[c] = hex_gdf[c].round().astype(int)
            
            if c != 'max_idx_15_min':
                
                if 'max' in c:
                    hex_gdf[c] = hex_gdf[c].astype(str) + ' min'
                else:
                    hex_gdf[c] = hex_gdf[c].astype(str)

    # missing amenities analysis
    lista_amenidades = ['max_preescolar','max_primaria',
                    'max_secundaria','max_salud','max_guarderías',
                    'max_asistencia social','max_alimentos','max_personal',
                    'max_farmacias','max_hogar','max_complementarios','max_social',
                    'max_actividad física', 'max_cultural']

    missing_column_name = 'Equipamiento/servicio prioritario'

    hex_gdf[missing_column_name] = 'No aplica'

    idx = hex_gdf['max_idx_15_min']>15
    hex_gdf.loc[idx,missing_column_name] = hex_gdf[lista_amenidades].idxmax(axis=1)

    # rename missing amenities function
    def missing_amenity(amenity_code):
        dict_names = {'max_preescolar':'Preescolar',
                    'max_primaria':'Primaria',
                    'max_secundaria':'Secundaria',
                    'max_guarderías':'Guardería',
                    'max_salud':'Salud - primer contacto',
                    'max_asistencia social':'Centro de asistencia social',
                    'max_alimentos':'Comercio de productos alimenticios',
                    'max_personal':'Comercio para el cuidado personal',
                    'max_farmacias':'Farmacia',
                    'max_hogar':'Comercio de artículos para el hogar',
                    'max_complementario':'Comercio barrial complementario',
                    'max_social':'Espacios de esparcimiento social',
                    'max_actividad física':'Espacios para la actividad física',
                    'max_cultural':'Espacios de esparcimiento cultural',
                    'No aplica':'No aplica'}
        return dict_names[amenity_code]

    # configuration dictionary for Kepler
    column = 'max_idx_15_min'

    config,_ = aup.hex_config()

    _name = 'Tiempo máximo a todos los servicios'
    #_name = 'max_idx_15_min_2'

    hex_gdf[_name] = hex_gdf[column].astype(str) + ' min'

    bins = [0, 15, 30, 45, 60, hex_gdf[column].max()]
    labels = ['0-15', '15-30', '30-45', '45-60', '60>']
    hex_gdf[f'bins_{column}'] = pd.cut(
        hex_gdf[column], bins=bins, labels=labels, include_lowest=True)

    config["config"]["visState"]["layers"][0]["visualChannels"]["colorField"][
        "name"
    ] = f'bins_{column}'
    # columns to show in kepler
    config["config"]["visState"]["interactionConfig"]["tooltip"]["fieldsToShow"][
        "Análisis de hexágono"] = [_name, 'Tiempo a escuelas',
                    'Tiempo a servicios comunitarios','Tiempo a comercio',
                    'Tiempo a entretenimiento','           ',
                    'Población total','Densidad de población',
                    'Población femenina','Población masculina',
                    'Población de 0 a 14 años','Población de 15 a 24 años',
                    'Población de 25 a 59 años','Población de 60 años y más',
                    '            ',missing_column_name]

    # create filtered GeoDataFrame for Kepler
    hex_kepler = hex_gdf[[_name, 'max_escuelas', 'max_servicios comunitarios',
                      'max_comercio', 'max_entretenimiento','pobtot',
                      'dens_pobha','pobfem','pobmas','pob_0a14',
                      'pob_15a24', 'pob_25a59','p_60ymas',missing_column_name,
                      'geometry',f'bins_{column}']]

    # rename columns from Kepler GeoDataFrame
    rename_columns = {'max_escuelas':'Tiempo a escuelas',
                  'max_servicios comunitarios':'Tiempo a servicios comunitarios',
                  'max_comercio':'Tiempo a comercio',
                  'max_entretenimiento':'Tiempo a entretenimiento',
                  'pobtot':'Población total',
                  'dens_pobha':'Densidad de población',
                  'pobfem':'Población femenina',
                  'pobmas':'Población masculina',
                  'pob_0a14':'Población de 0 a 14 años',
                  'pob_15a24':'Población de 15 a 24 años',
                  'pob_25a59':'Población de 25 a 59 años',
                'p_60ymas':'Población de 60 años y más'}

    for c in rename_columns.keys():
        hex_kepler.rename(columns={c : rename_columns[c]}, inplace=True)

    # rename missing amenity in Kepler GeoDataFrame
    hex_kepler[missing_column_name] = hex_kepler[missing_column_name].apply(missing_amenity)

    # create empty columns for Kepler visualization
    hex_kepler['           '] = '         '
    hex_kepler['            '] = '          '

    # define center coordinates for Kepler map
    longitude = hex_gdf.dissolve().geometry.centroid.x
    latitude = hex_gdf.dissolve().geometry.centroid.y
    config["config"]["mapState"]["latitude"] = latitude[0]
    config["config"]["mapState"]["longitude"] = longitude[0]

    # create Kepler
    map_city = KeplerGl(height=800)
    map_city.config = config
    map_city.add_data(hex_kepler, name='Análisis de hexágono')

    if save:
        output_folder = '../output/html/15_min_city/'
        map_city.save_to_html(file_name=output_folder+f"{city.lower()}.html", read_only=False)
        aup.log(f'saved html for {city}')


def main(gdf_mun, save=False):
    city = gdf_mun.city.unique()[0]
    hex_gdf = load_data(city, gdf_mun)

    aup.log(f"{city} data loaded.")

    make_html(hex_gdf, city, save=save)




if __name__ == "__main__":
    aup.log('--'*20)
    aup.log('Starting script')
    gdf_mun = aup.gdf_from_db('metro_list', 'metropolis')

    gdf_mun = gdf_mun.set_crs("EPSG:4326")

    city_list = ['ZMVM','Monterrey','Guadalajara',
    'Tijuana','Hermosillo','Queretaro','Puebla','Merida',
    'Morelia','Aguascalientes','Leon','Chihuahua']

    for city in gdf_mun.city.unique():
        if city in city_list:
            aup.log(f'\n Starting city {city}')
            main(gdf_mun.loc[gdf_mun.city==city], save=True)


