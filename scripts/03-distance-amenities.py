import os
import sys

import pandas as pd
import geopandas as gpd

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup


def main(schema, folder_sufix, year, amenities, resolution=8, save=False):
    # Read json with municipality codes by capital or metropolitan area
    df = pd.read_json("Metropolis_CVE.json")
    aup.log("Read metropolitan areas and capitals json")
    
    #Folder names from database
    mpos_folder = 'mpos_'+year
    
    # Iterate over municipality DataFrame columns to access each municipality code
    for c in df.columns.unique():
        aup.log(f"\n Starting municipality filters for {c}")
        # Creates empty GeoDataFrame to store specified municipality polygons
        mun_gdf = gpd.GeoDataFrame()
        #ageb_gdf = gpd.GeoDataFrame()
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
            query = f"SELECT * FROM hexgrid.hex_grid WHERE \"CVEGEO\" LIKE \'{m}%%\'"
            hex_bins = hex_bins.append(aup.gdf_from_query(query, geometry_col='geometry'))
            aup.log(f"Donwloaded hex bins for {m}")
            
        #Define projections
        mun_gdf = mun_gdf.set_crs("EPSG:4326")
        hex_bins = hex_bins.set_crs("EPSG:4326")

        # Creates query to download nodes from the metropolitan area or capital
        _, nodes, edges = aup.graph_from_hippo(mun_gdf, 'osmnx_new')
        aup.log(f"Downloaded {len(nodes)} nodes and {len(edges)} from database for {c}")

        #Creates wkt for query
        gdf_tmp = mun_gdf.copy()
        gdf_tmp = gdf_tmp.to_crs("EPSG:6372")
        gdf_tmp = gdf_tmp.buffer(1).reset_index().rename(columns={0:'geometry'})
        gdf_tmp = gdf_tmp.to_crs("EPSG:4326")
        poly_wkt = gdf_tmp.dissolve().geometry.to_wkt()[0]
        aup.log("Created wkt based on dissolved polygon")

        nodes_amenities = gpd.GeoDataFrame()
        i = 0
        
        for a in amenities:
            denue_amenity = gpd.GeoDataFrame()
            for cod in amenities[a]:
                query = f"SELECT * FROM denue_nodes.denue_node_2020 WHERE (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')) AND (\"codigo_act\" = {cod})"
                denue_amenity = denue_amenity.append(aup.gdf_from_query(query, geometry_col='geometry'))
                aup.log(f"Downloaded {len(denue_amenity)} {a} from database for {c}")

            nodes_distance = aup.calculate_distance_nearest_poi(denue_amenity, nodes, edges, a, 'osmid')
            aup.log(f"Calculated distance for a total of {len(nodes_distance)} nodes")

            #Data to hex_bins
            nodes_distance.reset_index(inplace=True)
            hex_dist = aup.group_by_hex_mean(nodes_distance, hex_bins, resolution, a)
            hex_bins = hex_bins.merge(hex_dist[['hex_id_'+str(resolution),'dist_'+a]], 
            on='hex_id_'+str(resolution))
            aup.log(f"Added distance data to {a} to {len(hex_bins)} hex bins")

            if i == 0:
                nodes_amenities = nodes_distance[['osmid','x','y','geometry','dist_'+a]]
            else:
                nodes_amenities = nodes_amenities.merge(
                    nodes_distance[['osmid','dist_'+a]], on='osmid')
            aup.log('Added nodes distance to nodes_amenities')
            i += 1

        if save:
            aup.gdf_to_db(hex_bins, "hex_bins_"+folder_sufix, schema=schema, if_exists="append")
            aup.gdf_to_db(nodes, "nodes_"+folder_sufix, schema=schema, if_exists="append")



if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')
    #censo_column_start = 14 #column where numeric data starts in censo
    #censo_column_end = -1 #column where numeric data ends in censo
    year = '2020'
    schema = 'processed'
    folder_sufix = 'dist' #sufix for folder name
    amenities = {'farmacia':[464111,464112],'hospitales':[622111,622112], 
    'supermercados':[462111,462112]}
    main(schema, folder_sufix, year, amenities)