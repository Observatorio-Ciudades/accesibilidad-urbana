import io
import os

import aup
import boto3
import geopandas as gpd
import pandas as pd
from keplergl import KeplerGl

import hex_config


def load_data(df, c):
    aup.log(f"{c} loading data")
    # Creates empty GeoDataFrame to store specified municipality polygons
    mun_gdf = gpd.GeoDataFrame()
    # ageb_gdf = gpd.GeoDataFrame()
    hex_bins = gpd.GeoDataFrame()
    year = 2020
    mpos_folder = f"mpos_{year}"
    for i in range(len(df.loc["mpos", c])):
        # Extracts specific municipality code
        m = df.loc["mpos", c][i]
        # Downloads municipality polygon according to code
        query = f"SELECT * FROM marco.{mpos_folder} WHERE \"CVEGEO\" LIKE '{m}'"
        mun_gdf = mun_gdf.append(aup.gdf_from_query(
            query, geometry_col="geometry"))
        # Creates query to download hex bins
        query = f"SELECT * FROM processed.hex_bins_index_{year} WHERE \"CVEGEO\" LIKE '{m}%%'"
        hex_bins = hex_bins.append(
            aup.gdf_from_query(query, geometry_col="geometry"))
    return hex_bins


def make_html(hex_bins, column, c):
    hex_bins[column] = hex_bins[column].astype(float)

    if column == "idx_accessibility":
        config = hex_config.config_idx
        hex_bins[column] = hex_bins[column].round(2)
        bins = [0, 0.2, 0.4, 0.6, 0.8, 1]
        labels = ['0-0.2', '0.2-0.4', '0.4-0.6', '0.6-0.8', '0.8-1']
        hex_bins[f'bins_{column}'] = pd.cut(
            hex_bins[column], bins=bins, labels=labels, include_lowest=True)
        config["config"]["visState"]["layers"][0]["visualChannels"]["colorField"][
            "name"
        ] = f'bins_{column}'
        _name = 'Índice accesibilidad'
        hex_bins[_name] = hex_bins[column].astype(str)
        config["config"]["visState"]["interactionConfig"]["tooltip"]["fieldsToShow"][
            "data"
        ][0]["name"] = _name
    else:
        config = hex_config.config
        config["config"]["visState"]["layers"][0]["visualChannels"]["colorField"][
            "name"
        ] = column
        _name = column.replace('dist_', 'Distancia a ')
        hex_bins[_name] = hex_bins[column].round(2).astype(str) + ' metros'
        _name = column.replace('dist_', 'Distancia a ')
        config["config"]["visState"]["interactionConfig"]["tooltip"]["fieldsToShow"][
            "data"
        ][0]["name"] = _name
    hex_bins["d"] = 0
    longitude = hex_bins.dissolve(by="d").geometry.centroid.x
    latitude = hex_bins.dissolve(by="d").geometry.centroid.y
    config["config"]["mapState"]["latitude"] = latitude[0]
    config["config"]["mapState"]["longitude"] = longitude[0]
    session = boto3.Session(profile_name="observatorio")
    dev_s3_client = session.client("s3")
    map_city = KeplerGl(height=800, data={"data": hex_bins}, config=config)
    map_city.save_to_html(file_name="temp.html", read_only=True)
    with open("temp.html", "r+") as f:
        content = f.read()
        f.seek(0)
        f.truncate()
        content = content.replace(
            'Kepler.gl', f'{c}-{_name}').replace('kepler.gl@2.2', 'kepler.gl@2.3')
        f.write(content)
    with open("temp.html", "rb") as f:
        dev_s3_client.upload_fileobj(
            f,
            "ciudades-plots",
            f"html/{c.replace(' ','-')}_{column}.html",
            ExtraArgs={"ContentType": "text/html", "ACL": "public-read"},
        )
    aup.log(f"{c} {column} done")


def main(c, df, config):
    hex_bins = load_data(df, c)
    aup.log(f"{c} data loaded.")
    for column in [
        "dist_hospitales",
        "dist_farmacia",
        "dist_supermercados",
        "idx_accessibility",
    ]:
        make_html(hex_bins, column, c)


if __name__ == "__main__":
    aup.log("Starting with 06-html-ciudades.py")
    df = pd.read_json("../scripts/Metropolis_CVE.json")
    config = hex_config.config
    cities = [c for c in df.columns.unique()]
    for c in cities:
        main(c, df, config)
    aup.log("All done")
    os.remove(r"temp.html")
