import os
import sys

import pandas as pd
import geopandas as gpd
import math

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


def main(schema, folder_sufix, year, save=False):
    # Read json with municipality codes by capital or metropolitan area
    df = pd.read_json("Metropolis_CVE.json")
    aup.log("Read metropolitan areas and capitals json")
    
    #Folder names from database
    mpos_folder = 'mpos_'+year
    hex_folder = 'hex_bins_dist_'+year
    
    # Iterate over municipality DataFrame columns to access each municipality code
    for c in df.columns.unique():
        aup.log(f"\n Starting municipality filters for {c}")
        # Creates empty GeoDataFrame to store specified municipality polygons
        mun_gdf = gpd.GeoDataFrame()
        hex_bins = gpd.GeoDataFrame()
        # Iterates over municipality codes for each metropolitan area or capital
        for i in range(len(df.loc["mpos", c])):
            # Extracts specific municipality code
            m = df.loc["mpos", c][i]
            # Downloads municipality polygon according to code
            query = f"SELECT * FROM marco.{mpos_folder} WHERE \"CVEGEO\" LIKE \'{m}\'"
            mun_gdf = mun_gdf.append(aup.gdf_from_query(query, geometry_col='geometry'))
            aup.log(f"Downloaded {m} GeoDataFrame at: {c}")
            #Creates query to download hex bins
            query = f"SELECT * FROM processed.{hex_folder} WHERE \"CVEGEO\" LIKE \'{m}%%\'"
            hex_bins = hex_bins.append(aup.gdf_from_query(query, geometry_col='geometry'))
            aup.log(f"Donwloaded hex bins for {m}")
            
        #Define projections
        mun_gdf = mun_gdf.set_crs("EPSG:4326")
        hex_bins = hex_bins.set_crs("EPSG:4326")

        #Creates wkt for query
        gdf_tmp = mun_gdf.copy()
        gdf_tmp = gdf_tmp.to_crs("EPSG:6372")
        gdf_tmp = gdf_tmp.buffer(1).reset_index().rename(columns={0:'geometry'})
        gdf_tmp = gdf_tmp.to_crs("EPSG:4326")
        poly_wkt = gdf_tmp.dissolve().geometry.to_wkt()[0]
        aup.log("Created wkt based on dissolved polygon")

        #Download nodes
        nodes_folder = 'nodes_dist_'+year
        query = f"SELECT * FROM processed.{nodes_folder} WHERE ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')"
        nodes = aup.gdf_from_query(query, geometry_col='geometry')
        aup.log(f"Downloaded {len(nodes)} nodes from database for {c}")
        nodes = nodes.to_crs("EPSG:4326")
        # Filter nodes and hex_bins
        nodes_filter = nodes.loc[(nodes['dist_supermercados'] > 0) &
                        (nodes['dist_farmacia'] > 0) &
                        (nodes['dist_hospitales'] > 0)].copy()

        aup.log(f"Filtered node data")

        hex_filter = hex_bins.loc[(hex_bins['dist_supermercados'] > 0) &
                        (hex_bins['dist_farmacia'] > 0) &
                        (hex_bins['dist_hospitales'] > 0)].copy()

        aup.log(f"Filtered hex_bin data")

        #Datatreatment for extreme values
        nodes_filter['dist_supermercados'] = nodes_filter['dist_supermercados'].apply(lambda x: x if x <= 10000 else 10000)
        nodes_filter['dist_farmacia'] = nodes_filter['dist_farmacia'].apply(lambda x: x if x <= 10000 else 10000)
        aup.log("Deleted extreme values for nodes")

        hex_filter['dist_supermercados'] = hex_filter['dist_supermercados'].apply(lambda x: x if x <= 10000 else 10000)
        hex_filter['dist_farmacia'] = hex_filter['dist_farmacia'].apply(lambda x: x if x <= 10000 else 10000)
        aup.log("Deleted extreme values for hex_bins")

        #calculate index for nodes
        nodes_filter['idx_hospitales'] =  nodes_filter.apply (
            lambda row: 1 / (1 + math.exp( 0.00109861 * (row.loc['dist_hospitales'] - 3000 ))), axis=1)
        nodes_filter['idx_supermercado'] = nodes_filter.apply (
            lambda row: 1 / (1 + math.exp( 0.00627778 * (row.loc['dist_supermercados'] - 650 ))), axis=1)
        nodes_filter['idx_farmacias'] = nodes_filter.apply (
            lambda row: 1 / (1 + math.exp( 0.00627778 * (row.loc['dist_farmacia'] - 650 ))), axis=1)
        nodes_filter['idx_accessibility'] = nodes_filter.apply (
            lambda row: (0.333*row.loc['idx_supermercado']) + (0.334*row.loc['idx_farmacias']) + 
            (0.333*row.loc['idx_hospitales']), axis=1)

        aup.log(f"\nNodes: Calculated index for hospitals {round(nodes_filter.idx_hospitales.mean(),2)} " +
           f"\nCalculated index for supermarket {round(nodes_filter.idx_supermercado.mean(),2)} "+
                f"\nCalculated index for pharmacies {round(nodes_filter.idx_farmacias.mean(),2)} and "+
                    f"\nCalculated index for accessibility {round(nodes_filter.idx_accessibility.mean(),2)}")

        #calculate index for hex_bins
        nodes_hex = gpd.sjoin(nodes_filter, hex_bins, how='left')
        #grouping nodes by hex_bins
        nodes_hex_wgt_mean = nodes_hex.groupby('hex_id_8').agg(
                        {'idx_accessibility':'mean',
                         'idx_hospitales':'mean',
                         'idx_supermercado':'mean',
                         'idx_farmacias':'mean',
                        'osmid':'count'}).rename(columns={'osmid':'node_count'})

        #node counter for weighted index
        node_sum = nodes_hex_wgt_mean['node_count'].sum()
        #calculating weigths by hex_bin
        nodes_hex_wgt_mean['wAcc'] = nodes_hex_wgt_mean.idx_accessibility * nodes_hex_wgt_mean['node_count']
        nodes_hex_wgt_mean['wHsp'] = nodes_hex_wgt_mean.idx_hospitales * nodes_hex_wgt_mean['node_count']
        nodes_hex_wgt_mean['wSpm'] = nodes_hex_wgt_mean.idx_supermercado * nodes_hex_wgt_mean['node_count']
        nodes_hex_wgt_mean['wFrm'] = nodes_hex_wgt_mean.idx_farmacias * nodes_hex_wgt_mean['node_count']
        #calculating input for weigthed average
        nodes_hex_wgt_mean['idx_accessibility_wavg'] = nodes_hex_wgt_mean.wAcc / node_sum
        nodes_hex_wgt_mean['idx_hospitales_wavg'] = nodes_hex_wgt_mean.wHsp / node_sum
        nodes_hex_wgt_mean['idx_supermercado_wavg'] = nodes_hex_wgt_mean.wSpm / node_sum
        nodes_hex_wgt_mean['idx_farmacias_wavg'] = nodes_hex_wgt_mean.wFrm / node_sum

        #merging with hex_bins
        hex_ind = pd.merge(nodes_hex_wgt_mean, hex_bins, 
        left_on=nodes_hex_wgt_mean.index, right_on='hex_id_8', how='left')
        hex_ind_gdf = gpd.GeoDataFrame(hex_ind) #to GeoDataFrame

        aup.log(f"\nhex_bins: Calculated index for hospitals {round(hex_ind_gdf.idx_hospitales.mean(),2)}," +
        f"{round(hex_ind_gdf.idx_hospitales_wavg.sum(),2)}"+
           f"\nCalculated index for supermarket {round(hex_ind_gdf.idx_supermercado.mean(),2)},"+
           f"{round(hex_ind_gdf.idx_supermercado_wavg.sum(),2)}"+
                f"\nCalculated index for pharmacies {round(hex_ind_gdf.idx_farmacias.mean(),2)},"+
                f"{round(hex_ind_gdf.idx_farmacias_wavg.sum(),2)}"+
                    f"\nCalculated index for accessibility {round(hex_ind_gdf.idx_accessibility.mean(),2)},"+
                    f"{round(hex_ind_gdf.idx_accessibility_wavg.sum(),2)}")

        aup.log(f"Columns for gdf are: {hex_ind_gdf.columns}")


        if save:
            aup.gdf_to_db_slow(hex_ind_gdf, "hex_bins_"+folder_sufix, schema=schema, if_exists="append")
            #aup.gdf_to_db_slow(nodes_filter, "nodes_"+folder_sufix, schema=schema, if_exists="append")



if __name__ == "__main__":
    aup.log("\n")
    aup.log(' --'*20)
    aup.log('Starting index script')

    year = '2020'
    schema = 'processed'
    folder_sufix = 'index_'+year #sufix for folder name
    main(schema, folder_sufix, year, save=True)