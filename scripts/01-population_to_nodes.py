import os
import sys

import pandas as pd
import geopandas as gpd

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


def main(schema, folder, column_start, column_end, resolution=8, save=False):
    # Read json with municipality codes by capital or metropolitan area
    df = pd.read_json("Metropolis_CVE.json")
    aup.log("Read metropolitan areas and capitals json")
    # Download municipality polygons from database
    gdf = aup.gdf_from_db("mpos_2020", "marco")
    aup.log("Finished downloading mpos_2020 GeoDataFrame from database")
    # Iterate over municipality DataFrame columns to access each municipality code
    for c in df.columns.unique():
        aup.log(f"\n Starting municipality filters for {c}")
        # Creates empty GeoDataFrame to store specified municipality polygons
        mun_gdf = gpd.GeoDataFrame()
        ageb_gdf = pd.DataFrame()
        hex_bins = pd.DataFrame()
        # Iterates over municipality codes for each metropolitan area or capital
        for i in range(len(df.loc["mpos", c])):
            # Extracts specific municipality code
            m = df.loc["mpos", c][i]
            aup.log(f"Extracted CVGEO: {m} for city {c} from Metropolis DataFrame")
            # Filteres municipality GeoDataFrame according to code and appends to mun_gdf
            mun_gdf = mun_gdf.append(gdf.loc[gdf.CVEGEO == m])
            aup.log(f"Filtered {m} GeoDataFrame at: {c}")
            # Creates query used to download AGEB data
            query = f"SELECT * FROM censoageb.censoageb_2020 WHERE \"cve_geo\" LIKE \'{m}%%\'"
            ageb_gdf = ageb_gdf.append(aup.gdf_from_query(query, geometry_col='geometry'))
            aup.log(f"Donwloaded AGEB for {m}")
            #Creates query to download hex bins
            query = f"SELECT * FROM hexgrid.hex_grid WHERE \"CVEGEO\" LIKE \'{m}%%\'"
            hex_bins = hex_bins.append(aup.gdf_from_query(query, geometry_col='geometry'))
            aup.log(f"Donwloaded hex bins for {m}")

        # Reads mun_gdf GeoDataFrame as polygon
        poly = mun_gdf.geometry
        # Extracts coordinates from polygon as DataFrame
        coord_val = poly.bounds
        # Gets coordinates for bounding box
        n = coord_val.maxy.max()
        s = coord_val.miny.min()
        e = coord_val.maxx.max()
        w = coord_val.minx.min()
        aup.log(
            f"Extracted min and max coordinates from the municipality\
            polygon N:{round(n,5)} S:{round(s,5)} E:{round(e,5)} W:{round(w,5)}"
        )

        # Creates query to download nodes from the metropolitan area or capital
        query = f"SELECT * FROM osmnx_new.nodes WHERE (\"x\" between \'{w}\' and \'{e}\') and (\"y\" between \'{s}\' and \'{n}\')"
        nodes = aup.gdf_from_query(query, geometry_col='geometry')
        aup.log(f"Downloaded {len(nodes)} nodes from database for {c}")
        # Adds population data to nodes
        nodes = aup.population_to_nodes(nodes, ageb_gdf, column_start=column_start, 
        column_end=column_end-1, cve_column='cve_geo')

        aup.log(f"Added a total of {nodes.pobtot.sum()} persons to nodes")

        #Adds census data from nodes to hex bins
        hex_temp = gpd.sjoin(nodes, hex_bins) #joins nodes en hex bins
        hex_temp = hex_temp.groupby(f'hex_id_{resolution}').sum() #group hex bins
        hex_temp = hex_temp[ageb_gdf.iloc[:,column_start:column_end].columns.to_list()] #keeps only census columns
        hex_bins = pd.merge(hex_bins, hex_temp, right_index=True,
                        left_on=f'hex_id_{resolution}', how='left').fillna(0) #merges census data to original hex bins
        aup.log(f"Added census data to a total of {len(hex_bins)} hex bins")

        if save == True:
            print('something')



if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')
    censo_column_start = 14 #column where numeric data starts in censo
    censo_column_end = -1 #column where numeric data ends in censo
    schema = 'population'
    folder = 'folder'
    main(schema, folder, censo_column_start, censo_column_end)