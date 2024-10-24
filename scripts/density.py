################################################
# Script to calculate the POIs density in Mexico
################################################

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

import aup


def main(year, category_code, category_name):
    query = f'SELECT\
            hex.hex_id_8 AS hex_id_8,\
            hex.geometry as geometry,\
            Count(denue.codigo_act) AS {category_name}\
            FROM "hexgrid"."hexgrid_mx" AS hex\
            JOIN denue.denue_{year} AS denue\
            ON ST_Intersects(hex.geometry, denue.geometry)\
            WHERE denue.codigo_act = {category_code}\
            GROUP BY hex.hex_id_8, hex.geometry;'
    gdf = aup.gdf_from_query(query, geometry_col="geometry")


if __name__ == "__main__":
    categories = {"464111": "Farmacias", "464112": "Farmacias"}
    years = [2010, 2020]
    for year in years:
        for category_code, category_name in categories.items:
            main(year, category_code, category_name)
