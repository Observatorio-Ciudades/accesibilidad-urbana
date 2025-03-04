import io
import os
import sys

import aup
import boto3
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd


def load_data(df, c, year):
    mun_gdf = gpd.GeoDataFrame()
    hex_bins = gpd.GeoDataFrame()
    hex_grid = gpd.GeoDataFrame()
    aup.log(f"\n Starting municipality filters for {c}")
    # Creates empty GeoDataFrame to store specified municipality polygons and hex grid
    mun_gdf = gpd.GeoDataFrame()
    hex_bins = gpd.GeoDataFrame()
    # Iterates over city names for each metropolitan area or capital
    query = f"SELECT * FROM metropolis.metro_list WHERE \"city\" LIKE \'{c}\'"
    mun_gdf = aup.gdf_from_query(query, geometry_col='geometry')
    query = f"SELECT * FROM metropolis.hexgrid_8_city WHERE \"metropolis\" LIKE \'{c}\'"
    hex_grid = aup.gdf_from_query(query, geometry_col='geometry')
    ###Iterates over municipality code
    for i in range(len(df.loc["mpos", c])):
        # Extracts specific municipality code
        m = df.loc["mpos", c][i]
        # Creates query to download hex bins
        query = f"SELECT * FROM processed.hex_bins_index_{year} WHERE \"CVEGEO\" LIKE '{m}%%'"
        hex_bins = hex_bins.append(aup.gdf_from_query(query, geometry_col="geometry"))
        aup.log(f"Donwloaded hex bins for {m}")
    gdf = mun_gdf.copy()
    gdf = gdf.to_crs("EPSG:6372")
    gdf = gdf.buffer(1).reset_index().rename(columns={0: "geometry"})
    gdf = gdf.to_crs("EPSG:4326")
    poly_wkt = gdf.dissolve(by="index")["geometry"][0].to_wkt()
    schema = "osmnx"
    edges_query = f"SELECT * FROM {schema}.edges WHERE ST_Intersects(geometry, 'SRID=4326;{poly_wkt}')"
    edges = aup.gdf_from_query(edges_query, geometry_col="geometry")
    aup.log(f"{c} Data loaded in memory")
    return hex_bins, edges, hex_grid


def make_plot(hex_bins, edges, c, hex_grid):
    ax_title_size = 30
    fig_title_size = 40
    secondary_label_size = 15
    fig, axes = plt.subplots(2, 2, figsize=(20, 20))
    measures = [
        "dist_farmacia",
        "dist_hospitales",
        "dist_supermercados",
        "idx_accessibility",
    ]
    for ax, measure in zip(axes.flat, measures):
        hex_grid.plot(color="#e6e5e3", ax=ax, zorder=1)
        if measure == "idx_accessibility":
            cmap = "inferno"
            legend_kwds = {"shrink": 0.7, "label": "Índice"}
            vmax = 1
            vmin = 0
        else:
            cmap = "inferno_r"
            legend_kwds = {"shrink": 0.7, "label": "Distancia (m)"}
            vmax = 3000
            vmin = 0
        hex_bins.plot(
            column=measure,
            cmap=cmap,
            ax=ax,
            zorder=2,
            legend=True,
            legend_kwds=legend_kwds,
            vmin=vmin,
            vmax=vmax,
        )
        edges[
            edges["highway"].isin(
                ["primary", "primary_link", "secondary", "secondary_link"]
            )
        ].plot(color="white", ax=ax, zorder=3, linewidth=0.8)
        title = (
            measure.replace("dist_", "Distancia ")
            .replace("idx_accessibility", "Índice Accesibilidad ")
            .replace("farmacia", "farmacias")
        )
        ax.set_title(f"{title}", fontsize=ax_title_size)
        ax.axis("off")
    fig.suptitle(c, fontsize=fig_title_size)
    fig.tight_layout()
    session = boto3.Session(profile_name="observatorio")
    dev_s3_client = session.client("s3")
    img_data = io.BytesIO()
    # plt.savefig(
    #     f"../output/figures/{year}/{year}_{c.replace(' ','-')}.png",
    #     bbox_inches="tight",
    #     dpi=300,
    # )
    plt.savefig(
        img_data,
        bbox_inches="tight",
        dpi=300,
    )
    img_data.seek(0)
    dev_s3_client.upload_fileobj(
        img_data,
        "ciudades-plots",
        f"{year}_{c.replace(' ','-')}.png",
        ExtraArgs={"ContentType": "image/png", "ACL": "public-read"},
    )
    plt.close()


def main(df, c, year):
    hex_bins, edges, hex_grid = load_data(df, c, year)
    make_plot(hex_bins, edges, c, hex_grid)
    aup.log(f"Done with {c}")


if __name__ == "__main__":
    aup.log("--" * 10)
    aup.log("Starting plotting script")
    # years = [2020, 2010]
    years = [2020]
    df = pd.read_json("../scripts/Metropolis_CVE.json")
    df = df.loc[:, "Orizaba":]
    for year in years:
        for c in df.columns.unique():
            main(df, c, year)
