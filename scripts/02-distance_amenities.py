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
    df = pd.read_json("/home/jovyan/work/scripts/Metropolis_CVE.json")
    aup.log("Read metropolitan areas and capitals json")
    
    #Folder names from database
    mpos_folder = 'mpos_'+year
    
    # Iterate over municipality DataFrame columns to access each municipality code
    for c in df.columns.unique():
        aup.log(f"\n Starting municipality filters for {c}")
        # Creates empty GeoDataFrame to store specified municipality polygons and hex grid
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
            #Creates query to download hex bins according to code
            query = f"SELECT * FROM hexgrid.hexgrid_mx WHERE \"CVEGEO\" LIKE \'{m}%%\'"
            hex_bins = hex_bins.append(aup.gdf_from_query(query, geometry_col='geometry'))
            aup.log(f"Donwloaded hex bins for {m}")
            
        #Define projections for municipalities and hexgrids
        mun_gdf = mun_gdf.set_crs("EPSG:4326")
        hex_bins = hex_bins.set_crs("EPSG:4326")

        # Creates query to download OSMNX nodes and edges from the DB
        # by metropolitan area or capital using the municipality geometry
        _, nodes, edges = aup.graph_from_hippo(mun_gdf, 'osmnx', edges_folder='edges_speed')
        nodes_analysis = nodes.reset_index().copy()
        edges['time_min'].fillna(edges['time_min'].mean(),inplace=True)
        aup.log(f"Downloaded {len(nodes)} nodes and {len(edges)} from database for {c}")
        aup.log(f"")

        #Creates wkt for query
        # It will be used to download the POI
        gdf_tmp = mun_gdf.copy()
        gdf_tmp = gdf_tmp.to_crs("EPSG:6372")
        gdf_tmp = gdf_tmp.buffer(1).reset_index().rename(columns={0:'geometry'})
        gdf_tmp = gdf_tmp.to_crs("EPSG:4326")
        poly_wkt = gdf_tmp.dissolve().geometry.to_wkt()[0]
        aup.log("Created wkt based on dissolved polygon")

        nodes_amenities = gpd.GeoDataFrame()
        hex_format = pd.DataFrame()
        i = 0
        
        #Starts iterating by type of amenity defined i.e: pharmacies, supermarkets, 
        # elementary schools, etc
        for a in amenities:
            #creates empty gdf for the POIs
            denue_amenity = gpd.GeoDataFrame()
            #Based on the SCIAN code, the POIs will be downloaded from the DB
            for cod in amenities[a]:
                query = f"SELECT * FROM denue_nodes.denue_node_2020 WHERE (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')) AND (\"codigo_act\" = {cod})"
                denue_amenity = denue_amenity.append(aup.gdf_from_query(query, geometry_col='geometry'))
            aup.log(f"Downloaded accumulated total of {len(denue_amenity)} {a} from database for {c}")
            df_temp = nodes.copy()
            nodes_distance = nodes.copy()


            ##Added this if in case there are no amenities of the type in the city. Prevents it from 
            #Crashing if len = 0
            if len(denue_amenity) == 0:
                nodes_distance['time_'+a] = 0
                aup.log(f"0 {cod} found in {c}")
            else:
                #Due to memory constraints, the total number of POIs will be divided in groups of 100
                #These will run with the calculate nearest distance poi function by group and will be stored
                # to check later
                c_denue = len(denue_amenity)/500
                for k in range(int(c_denue)+1):
                    aup.log(f"Starting range k = {k} of {int(c_denue)}")
                    denue_process = denue_amenity.iloc[int(500*k):int(500*(1+k))].copy()
                    nodes_distance_prep = aup.calculate_distance_nearest_poi(denue_process, nodes_analysis, 
                    edges, a, 'osmid', wght='time_min')
                    #A middle gdf is created whose columns will be the name of the amenity and the group number it belongs to
                    df_int = pd.DataFrame()
                    df_int['time_'+str(k)+a] = nodes_distance_prep['dist_'+a]
                    #The middle gdf is merged into the previously created temporary gdf to store the data
                    df_temp = df_temp.merge(df_int, left_index=True, right_index=True)
                aup.log(f"finished")
                #Once all groups of 100 are run, we drop the non-distance values from the temporary gdf
                df_temp.drop(['x', 'y', 'street_count','geometry'], inplace = True, axis=1)
                #We apply the min function to find the minimum value. This value is sent to a new df_min
                df_min = pd.DataFrame()
                df_min['time_'+a] = df_temp.min(axis=1)
                #We merge df_min which contains the shortest distance to the POI with nodes_distance which will store
                #all final data
                nodes_distance = nodes_distance.merge(df_min, left_index=True, right_index=True)

            aup.log(f"Calculated distance for a TOTAL of {len(nodes_distance)} nodes")

            #Data to hex_bins
            #In this process the data in the nodes will be summarized into the hexbin they fall.
            nodes_distance.reset_index(inplace=True)
            nodes_distance = nodes_distance.set_crs("EPSG:4326")
            hex_bins = hex_bins.set_crs("EPSG:4326")
            col_name = f'time_{a}'
            hex_dist = aup.group_by_hex_mean(nodes_distance, hex_bins, resolution, col_name)
            hex_bins = hex_bins.merge(hex_dist[['hex_id_'+str(resolution),col_name]], 
            on='hex_id_'+str(resolution))
            aup.log(f"Added distance data to {a} to {len(hex_bins)} hex bins")

            #If it is the first round nodes_amenities is created equal to nodes_distance
            #If it is the second or more, the new nodes_distance is merged.
            #This way we obtain the final gdf of interest that will contain the minimum disstance
            #to each type of amenity
            if i == 0:
                nodes_amenities = nodes_distance[['osmid','x','y','geometry','time_'+a]]
            else:
                nodes_amenities = nodes_amenities.merge(
                    nodes_distance[['osmid','time_'+a]], on='osmid')
            aup.log('Added nodes distance to nodes_amenities')
            i += 1
            #We define the projections and upload
            hex_bins = hex_bins.set_crs("EPSG:4326")
            nodes_amenities = nodes_amenities.set_crs("EPSG:4326")

            #Give more efficient format to table
            
            for idx,row in hex_bins.iterrows():
                data = {'hex_id_8': ['id'],
                'time': ['time'], 'amenity':['amenity']}
                hex_data = pd.DataFrame(data)
                hex_id = hex_bins.loc[idx, 'hex_id_8']
                time = hex_bins.loc[idx, 'time_'+a]
                amenity = a
                hex_data['hex_id_8']= hex_id
                hex_data['time']= time
                hex_data['amenity']= str(amenity)
                hex_format = hex_format.append(hex_data)

        if save:
            aup.gdf_to_db_slow(hex_format, "hex_bins_"+folder_sufix, schema=schema, if_exists="append")
            #Due to memory constraints the nodes are uploaded in groups of 10,000
            #c_nodes = len(nodes_amenities)/10000
            #for p in range(int(c_nodes)+1):
            #    nodes_upload = nodes_amenities.iloc[int(10000*p):int(10000*(p+1))].copy()
            #    aup.gdf_to_db_slow(nodes_upload, "nodes_"+folder_sufix, schema=schema, if_exists="append")
            #    aup.log("uploaded nodes into DB ")
            #aup.log("Finished uploading nodes ")
            


### select the desired types of amenities, name them and select their SCIAN code.
if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')
    year = '2020'
    schema = 'time_amenities_clean'
    folder_sufix = 'time_2020' #sufix for folder name
    amenities = {'denue_supermercado': [462111,462112],'denue_farmacia': [464111,464112], 
            'denue_hospital': [622111,622112], 'denue_kinder':[611111, 611112], 'denue_primaria':[611121, 611122],
            'denue_secundaria':[611131, 611132], 'denue_escuela_mixta': [611171, 611172], 
            'denue_carniceria': [461121], 'denue_polleria': [461122], 'denue_pescaderia': [461123], 
            'denue_verduleria': [461130] }
    main(schema, folder_sufix, year, amenities, save = True)