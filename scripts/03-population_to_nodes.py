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
    df = pd.read_json("/home/jovyan/work/scripts/Metropolis_CVE.json")
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
            query = f"SELECT * FROM hexgrid.hexgrid_mx WHERE \"CVEGEO\" LIKE \'{m}%%\'"
            hex_bins = hex_bins.append(aup.gdf_from_query(query, geometry_col='geometry'))
            aup.log(f"Donwloaded hex bins for {m}")
            
        #Define projections
        mun_gdf = mun_gdf.set_crs("EPSG:4326")
        ageb_gdf = ageb_gdf.set_crs("EPSG:4326")
        hex_bins = hex_bins.set_crs("EPSG:4326")

        #Download nodes with distance to denue data
        if year == '2010':
            _, nodes, _ = aup.graph_from_hippo(mun_gdf, 'networks', edges_folder='edges_2011', 
            nodes_folder='nodes_2011')
        elif year == '2020':
            _, nodes, _ = aup.graph_from_hippo(mun_gdf, 'osmnx')
        aup.log(f"Downloaded {len(nodes)} nodes from database for {c}")

        nodes = nodes.to_crs("EPSG:4326")

        ### VIALIDADES 2011######
        if year == '2010':
            nodes.drop(['ID', 'TIPOVIA', 'TIPO', 
            'NUMERO', 'DERE_TRAN', 'ADMINISTRA', 'NUME_CARR', 'CONDICION', 
            'ORIGEN', 'CALI_REPR', 'CVEGEO', 'NOMVIAL', 'SENTIDO', 'LONGITUD', 'UNIDAD', 
            'vertex_pos', 'vertex_ind', 'vertex_par', 'vertex_p_1', 
            'distance', 'angle'], inplace = True, axis=1)
        ##########

        # columns that won't be divided by nodes
        avg_column = ["prom_hnv", "graproes", "graproes_f", 
        "graproes_m", "prom_ocup", "pro_ocup_c"]
        # Adds population data to nodes
        nodes_pop = aup.socio_polygon_to_points(nodes, ageb_gdf, column_start=column_start, 
        column_end=column_end-1, cve_column='cve_geo', avg_column=avg_column)

        aup.log(f"Added a total of {nodes_pop.pobtot.sum()} persons to nodes")

        #Adds census data from nodes to hex bin

        #### CENSO 2020 ######
        if year == '2020':
            string_columns = ['cve_geo','cve_ent','cve_mun','cve_loc','cve_ageb',
            'entidad','nom_ent','mun','nom_mun','loc','nom_loc','ageb',
            'mza','cve_geo_ageb','hex_id_8', 'x', 'y', 'street_count']
        ###### CENSO 2010#############
        if year == '2010':
            string_columns = [
            'censo', 'cve_ent', 'nom_ent', 'cve_mun', 'nom_mun',
            'cve_loc', 'cve_ageb', 'cve_cd',
            'hex_id_8','x', 'y', 'codigo', 'cve_geo', 'geog', 
            'fecha_act', 'geom', 'institut', 'OID']

        wgt_dict = {'prom_hnv':'pobtot', 'graproes':'pobtot',
        'graproes_f':'pobfem', 'graproes_m':'pobmas',
        'prom_ocup':'pobtot', 'pro_ocup_c':'pobtot'}

        hex_pop = aup.socio_points_to_polygon(hex_bins, nodes_pop, 
        'hex_id_8', string_columns, wgt_dict=wgt_dict, avg_column=avg_column)

        hex_upload = hex_bins.merge(hex_pop, on= 'hex_id_8')

        #aup.log(f"Added census data to a total of {len(hex_pop)} hex bins and {hex_pop.pobtot.sum()} population")
        if save:
            aup.gdf_to_db_slow(hex_upload, "hex_bins_"+folder_sufix, schema=schema, if_exists="append")
            c_nodes = len(nodes_pop) / 10000
            aup.log(f"There are a total of {round(c_nodes,2)} nodes divisions")
            for cont in range(int(c_nodes)+1):
                nodes_pop_upload = nodes_pop.iloc[int(10000*cont):int(10000*(cont+1))].copy()
                aup.gdf_to_db_slow(nodes_pop_upload, "nodes_"+folder_sufix, schema=schema, if_exists="append")
                aup.log(f"Uploaded {cont} out of {round(c_nodes,2)}")



if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')
    censo_column_start = 16 #column where numeric data starts in censo
    censo_column_end = -1 #column where numeric data ends in censo
    year = '2010'
    schema = 'censo'
    folder_sufix = f'pop_{year}' #sufix for folder name
    main(schema, folder_sufix, year, censo_column_start, censo_column_end, save=True)