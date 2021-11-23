import os
import sys

import pandas as pd
import geopandas as gpd

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


def main(schema, folder_sufix, year, column_start, column_end, resolution=8, save=False):
    # Read json with municipality codes by capital or metropolitan area
    df = pd.read_json("Metropolis_CVE.json")
    aup.log("Read metropolitan areas and capitals json")
    
    #Folder names from database
    mpos_folder = 'mpos_'+year
    censo_folder = 'censoageb_' + year
    
    # Iterate over municipality DataFrame columns to access each municipality code
    for c in df.columns.unique():
        aup.log(f"\n Starting municipality filters for {c}")
        # Creates empty GeoDataFrame to store specified municipality polygons
        mun_gdf = gpd.GeoDataFrame()
        ageb_gdf = gpd.GeoDataFrame()
        hex_bins = gpd.GeoDataFrame()

        if c == 'Tehuacan':

            # Iterates over municipality codes for each metropolitan area or capital
            for i in range(len(df.loc["mpos", c])):
                # Extracts specific municipality code
                m = df.loc["mpos", c][i]
                # Downloads municipality polygon according to code
                query = f"SELECT * FROM marco.{mpos_folder} WHERE \"CVEGEO\" LIKE \'{m}\'"
                mun_gdf = mun_gdf.append(aup.gdf_from_query(query, geometry_col='geometry'))
                aup.log(f"Downloaded {m} GeoDataFrame at: {c}")
                # Creates query used to download AGEB data
                query = f"SELECT * FROM censoageb.{censo_folder} WHERE \"cve_geo\" LIKE \'{m}%%\'"
                ageb_gdf = ageb_gdf.append(aup.gdf_from_query(query, geometry_col='geometry'))
                aup.log(f"Donwloaded AGEB for {m}")
                #Creates query to download hex bins
                query = f"SELECT * FROM hexgrid.hex_grid WHERE \"CVEGEO\" LIKE \'{m}%%\'"
                hex_bins = hex_bins.append(aup.gdf_from_query(query, geometry_col='geometry'))
                aup.log(f"Donwloaded hex bins for {m}")
                
            #Define projections
            mun_gdf = mun_gdf.set_crs("EPSG:4326")
            ageb_gdf = ageb_gdf.set_crs("EPSG:4326")
            hex_bins = hex_bins.set_crs("EPSG:4326")

            #Creates wkt for query
            gdf_tmp = mun_gdf.copy()
            gdf_tmp = gdf_tmp.to_crs("EPSG:6372")
            gdf_tmp = gdf_tmp.buffer(1).reset_index().rename(columns={0:'geometry'})
            gdf_tmp = gdf_tmp.to_crs("EPSG:4326")
            poly_wkt = gdf_tmp.dissolve().geometry.to_wkt()[0]
            aup.log("Created wkt based on dissolved polygon")

            #Download nodes with distance to denue data
            query = f"SELECT * FROM processed.nodes_dist_2020 WHERE ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')"
            nodes = aup.gdf_from_query(query, geometry_col='geometry')
            aup.log(f"Downloaded {len(nodes)} nodes from database for {c}")
            nodes = nodes.to_crs("EPSG:4326")
            # Adds population data to nodes
            nodes_pop = aup.population_to_nodes(nodes, ageb_gdf, column_start=column_start, 
            column_end=column_end-1, cve_column='cve_geo')

            aup.log(f"Added a total of {nodes_pop.pobtot.sum()} persons to nodes")

            #Adds census data from nodes to hex bins
            hex_temp = gpd.sjoin(nodes_pop, hex_bins) #joins nodes en hex bins

            #convert data types
            string_columns = ['cve_geo','cve_ent','cve_mun','cve_loc','cve_ageb',
            'entidad','nom_ent','mun','nom_mun','loc','nom_loc','ageb',
            'mza','cve_geo_ageb','hex_id_8','CVEGEO']

            hex_temp = aup.convert_type(hex_temp, string_column=string_columns)

            hex_temp = hex_temp.groupby(f'hex_id_{resolution}').sum() #group hex bins

            hex_temp = hex_temp[ageb_gdf.iloc[:,column_start:column_end].columns.to_list()] #keeps only census columns
            hex_bins = pd.merge(hex_bins, hex_temp, right_index=True,
                            left_on=f'hex_id_{resolution}', how='left').fillna(0) #merges census data to original hex bins
            aup.log(f"Added census data to a total of {len(hex_bins)} hex bins")

            if save:
                aup.gdf_to_db_slow(hex_bins, "hex_bins_"+folder_sufix, schema=schema, if_exists="append")
                #c_nodes = len(nodes_pop) / 10000
                #aup.log(f"There are a total of {round(c_nodes,2)} nodes divisions")
                #for cont in range(int(c_nodes)+1):
                #    nodes_pop_upload = nodes_pop.iloc[int(10000*cont):int(10000*(cont+1))].copy()
                #    aup.gdf_to_db_slow(nodes_pop_upload, "nodes_"+folder_sufix, schema=schema, if_exists="append")
                #    aup.log(f"Uploaded {cont} out of {round(c_nodes,2)}")



if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')
    censo_column_start = 14 #column where numeric data starts in censo
    censo_column_end = -1 #column where numeric data ends in censo
    year = '2020'
    schema = 'processed'
    folder_sufix = 'pop' #sufix for folder name
    main(schema, folder_sufix, year, censo_column_start, censo_column_end, save=True)