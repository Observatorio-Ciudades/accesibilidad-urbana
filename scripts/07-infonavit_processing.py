import os
import sys

import pandas as pd
import geopandas as gpd

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup




def distance(schema, folder_sufix, year, amenities, resolution=8, save=False):

    # Read json with municipality codes by capital or metropolitan area
    df = pd.read_json("/home/jovyan/work/scripts/areas.json")
    aup.log("Read metropolitan areas and capitals json")

    #Folder names from database
    mpos_folder = 'mpos_'+year

    # Iterate over municipality DataFrame columns to access each municipality code
    for c in df.columns.unique():

        aup.log(f"\n Starting municipality filters for {c}")
        # Creates empty GeoDataFrame to store specified municipality polygons and hex grid
        mun_gdf = gpd.GeoDataFrame()
        hex_bins = gpd.GeoDataFrame()
        # Iterates over city names for each metropolitan area or capital
        query = f"SELECT * FROM metropolis.metro_list WHERE \"city\" LIKE \'{c}\'"
        mun_gdf = aup.gdf_from_query(query, geometry_col='geometry')
        query = f"SELECT * FROM metropolis.hexgrid_{resolution}_city WHERE \"metropolis\" LIKE \'{c}\'"
        hex_bins = aup.gdf_from_query(query, geometry_col='geometry')
         
        #Define projections
        mun_gdf = mun_gdf.set_crs("EPSG:4326")
        hex_bins = hex_bins.set_crs("EPSG:4326")

        # Creates query to download nodes from the metropolitan area or capital using
        #the geomtery of the metropolitan municipalities
        G, nodes, edges = aup.graph_from_hippo(mun_gdf, 'osmnx')
        aup.log(f"Downloaded {len(nodes)} nodes and {len(edges)} from database for {c}")

        #Creates wkt for query which will be used to download the POIs
        gdf_tmp = mun_gdf.copy()
        gdf_tmp = gdf_tmp.to_crs("EPSG:6372")
        gdf_tmp = gdf_tmp.buffer(1).reset_index().rename(columns={0:'geometry'})
        gdf_tmp = gdf_tmp.to_crs("EPSG:4326")
        poly_wkt = gdf_tmp.dissolve().geometry.to_wkt()[0]
        aup.log("Created wkt based on dissolved polygon")

        #starts counter
        i = 0

        #Create empty gdfs to store the POIs, the square grid and its centroid.
        grid = gpd.GeoDataFrame()
        centroid = gpd.GeoDataFrame()
        poi = gpd.GeoDataFrame()
        query = f"SELECT * FROM infonavit.infonavit_poi WHERE (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\'))"
        poi = aup.gdf_from_query(query, geometry_col='geometry')
        aup.log(f"Downloaded accumulated total of {len(poi)} from database for {c}")
        #Sets the index to its UID
        query = f"SELECT * FROM infonavit.centroid WHERE (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\'))"
        centroid = centroid.append(aup.gdf_from_query(query, geometry_col='geometry'))
        centroid = centroid.set_index('UID')
        #Sets index to its UID
        query = f"SELECT * FROM infonavit.grid WHERE (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\'))"
        grid = grid.append(aup.gdf_from_query(query, geometry_col='geometry'))
        grid = grid.set_index('UID')

        #Creates empty gdf to store data to nodes
        nodes_amenities = gpd.GeoDataFrame()
        #Creates gdf to store data to grid centroid
        centroid_amenities = gpd.GeoDataFrame()
        centroid_amenities = centroid_amenities.append(centroid)
        grid_amenities = gpd.GeoDataFrame()
        grid_amenities = grid_amenities.append(grid)
        for a in amenities:
            #Creates temporary centroid and nodes gdf
            centroid_tmp = gpd.GeoDataFrame()
            centroid_tmp = centroid_tmp.append(centroid)
            nodes_tmp = gpd.GeoDataFrame()
            nodes_tmp = nodes_tmp.append(nodes)
            #Creates gdf for POIs retaining only data needed for the function
            denue_points = gpd.GeoDataFrame()
            denue_points = poi[poi['poi_type']==a]
            denue_points = denue_points[['poi_type', 'geometry', 'x', 'y']]
            ## Calculate distance in a straight line for nodes as a mid step to get distance by street
            denue_amenity = gpd.GeoDataFrame()
            denue_amenity= aup.find_nearest(G, nodes, denue_points, return_distance=True)
            df_temp = nodes
            nodes_distance = nodes
            #Calculate distance in a straight line to centroid of square grid (end result)
            centroid_dist = gpd.GeoDataFrame()
            centroid_dist= aup.find_nearest(G, denue_points, centroid_tmp, return_distance=True)
            centroid_join = gpd.GeoDataFrame()
            centroid_join['dist_'+a]= centroid_dist['distance_node']
            centroid_amenities = centroid_amenities.merge(centroid_join, left_index=True, right_index=True)
            grid_amenities = grid_amenities.merge(centroid_join, left_index=True, right_index=True)
            #Starts calculating distance by street to nodes acording to obscd method
            #Due to memory constraints it is calculated in groups of 100 POIs
            #Method based on 02-distance_amenities
            c_denue = len(denue_amenity)/100
            for k in range(int(c_denue)+1):
                aup.log(f"Starting range k = {k} of {int(c_denue)}")
                denue_process = denue_amenity.iloc[int(100*k):int(100*(1+k))].copy()
                nodes_distance_prep = aup.calculate_distance_nearest_poi(denue_process, nodes, edges, a, 'osmid')
                df_int = pd.DataFrame()
                df_int['dist_'+str(k)+a] = nodes_distance_prep['dist_'+a]
                df_temp = df_temp.merge(df_int, left_index=True, right_index=True)
            aup.log(f"finished")
            df_temp.drop(['x', 'y', 'street_count','geometry'], inplace = True, axis=1)
            df_min = pd.DataFrame()
            df_min['dist_'+a] = df_temp.min(axis=1)
            nodes_distance = nodes_distance.merge(df_min, left_index=True, right_index=True)

            aup.log(f"Calculated distance for a TOTAL of {len(nodes_distance)} nodes")

            #Summarizes distance data of nodes into their corresponding hexbin by area
            nodes_distance.reset_index(inplace=True)
            nodes_distance = nodes_distance.set_crs("EPSG:4326")
            hex_bins = hex_bins.set_crs("EPSG:4326")
            hex_dist = aup.group_by_hex_mean(nodes_distance, hex_bins, resolution, a)
            hex_bins = hex_bins.merge(hex_dist[['hex_id_'+str(resolution),'dist_'+a]], 
            on='hex_id_'+str(resolution))
            aup.log(f"Added distance data to {a} to {len(hex_bins)} hex bins")
            #Creates nodes_amenities which will be uploaded with the data of all types of amenities run in the process
            #In the first iteration it will duplicate nodes_distance, afterards it will merge
            if i == 0:
                nodes_amenities = nodes_distance[['osmid','x','y','geometry','dist_'+a]]
            else:
                nodes_amenities = nodes_amenities.merge(
                    nodes_distance[['osmid','dist_'+a]], on='osmid')
            aup.log('Added nodes distance to nodes_amenities')
            i += 1
            hex_bins = hex_bins.set_crs("EPSG:4326")
            nodes_amenities = nodes_amenities.set_crs("EPSG:4326")

        if save:
            aup.gdf_to_db_slow(hex_bins, "hex_bins_"+folder_sufix, schema=schema, if_exists="append")
            aup.gdf_to_db_slow(centroid_amenities, "centroid_"+folder_sufix, schema=schema, if_exists="append")
            aup.gdf_to_db_slow(grid_amenities, "grid_"+folder_sufix, schema=schema, if_exists="append")
            c_nodes = len(nodes_amenities)/10000
            for p in range(int(c_nodes)+1):
                nodes_upload = nodes_amenities.iloc[int(10000*p):int(10000*(p+1))].copy()
                aup.gdf_to_db_slow(nodes_upload, "nodes_"+folder_sufix, schema=schema, if_exists="append")
                aup.log("uploaded nodes into DB ")
            aup.log("Finished uploading nodes ")


if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')
    year = '2020'
    schema = 'infonavit'
    folder_sufix = 'dist' #sufix for folder name
    amenities = {'primaria':['primaria'], 'secundaria':['secundaria'], 'mixto':['mixto'], 'salud':['salud'], 'abasto':['abasto'], 'recreacion':['recreacion']}
    save = True
    distance(schema, folder_sufix, year, amenities, save = save)
