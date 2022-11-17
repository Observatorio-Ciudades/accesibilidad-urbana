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
    denue_folder = 'denue_node_'+year
    
    # Iterate over cities and download municipalities gdf
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
        i = 0
        
        #Starts iterating by type of amenity defined i.e: pharmacies, supermarkets, 
        # elementary schools, etc
        for a in amenities:
            #creates empty gdf for the POIs
            denue_amenity = gpd.GeoDataFrame()
            #Based on the SCIAN code, the POIs will be downloaded from the DB
            for cod in amenities[a]:
                query = f"SELECT * FROM denue_nodes.{denue_folder} WHERE (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')) AND (\"codigo_act\" = {cod})"
                denue_amenity = denue_amenity.append(aup.gdf_from_query(query, geometry_col='geometry'))
            aup.log(f"Downloaded accumulated total of {len(denue_amenity)} {a} from database for {c}")
            df_temp = nodes.copy()
            nodes_distance = nodes.copy()


            ##Added this if in case there are no amenities of the type in the city. Prevents it from 
            #Crashing if len = 0
            if len(denue_amenity) == 0:
                nodes_distance['time_'+a] = 0
                aup.log(f"0 {cod} found in {c}")
                ##### This hecks if the number of points is an exact multiple of 250, if it is it will run with segments of 200, in order to avoid a crash.
            elif len(denue_amenity) % 250:
                #Due to memory constraints, the total number of POIs will be divided in groups of 250
                #These will run with the calculate nearest distance poi function by group and will be stored
                # to check later
                c_denue = len(denue_amenity)/200
                for k in range(int(c_denue)+1):
                    aup.log(f"Starting range k = {k} of {int(c_denue)}")
                    denue_process = denue_amenity.iloc[int(200*k):int(200*(1+k))].copy()
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
            else:
                #Due to memory constraints, the total number of POIs will be divided in groups of 250
                #These will run with the calculate nearest distance poi function by group and will be stored
                # to check later
                c_denue = len(denue_amenity)/250
                for k in range(int(c_denue)+1):
                    aup.log(f"Starting range k = {k} of {int(c_denue)}")
                    denue_process = denue_amenity.iloc[int(250*k):int(250*(1+k))].copy()
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
            hex_format = hex_bins[['hex_id_8', 'geometry']]
            hex_format['time'] = hex_bins['time'+a].copy()
            hex_format['amenity'] = str(a)
            hex_format['metropolis'] =str(c)     

            node_format = nodes_amenities[['osmid', 'x', 'y', 'geometry']]
            node_format['time'] = nodes_amenities['time'+a].copy()
            node_format['amenity'] = str(a)
            node_format['metropolis'] =str(c)
            if save:
                aup.gdf_to_db_slow(hex_format, "hex_bins_"+folder_sufix, schema=schema, if_exists="append")
                aup.log(f"uploaded hexes for {a} in {c} ")
                #Due to memory constraints the nodes are uploaded in groups of 10,000
                c_nodes = len(node_format)/10000
                for p in range(int(c_nodes)+1):
                    nodes_upload = node_format.iloc[int(10000*p):int(10000*(p+1))].copy()
                    aup.gdf_to_db_slow(nodes_upload, "nodes_"+folder_sufix, schema=schema, if_exists="append")
                    aup.log("uploaded nodes into DB ")
                aup.log(f"uploaded nodes for {a} in {c} ")
                aup.log("Finished uploading nodes ")
            


### select the desired types of amenities, name them and select their SCIAN code.
if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')
    year = '2020'
    schema = 'prox_analysis'
    folder_sufix = 'proximity_2020' #sufix for folder name
    amenities = {'denue_preescolar':[611111, 611112], 'denue_primaria':[611121, 611122],
            'denue_secundaria':[611131, 611132], 'denue_escuela_mixta': [611171, 611172], 
            'denue_casa_adultos_mayores': [623311, 623312], 'denue_guarderias':[624411, 624412],
            'denue_dif':[931610], 'denue_biblioteca':[519121, 519122], 'denue_supermercado':[462111],
            'denue_abarrotes':[461110], 'denue_carnicerias': [461121, 461122, 461123],
            'denue_farmacias':[464111, 464112], 'denue_ropa':[463211, 463212, 463213, 463215, 463216, 463218],
            'denue_calzado':[463310], 'denue_muebles':[466111, 466112, 466113, 466114],
            'denue_lavanderia':[812210], 'denue_cafe':[722515], 
            'denue_restaurante_insitu':[722511, 722512, 722513, 722514, 722519],
            'denue_restaurante_llevar':[722516, 722518, 722517],
            'denue_bares':[722412],
            'denue_museos':[712111, 712112], 'denue_cines':[512130],
            'denue_centro_cultural':[711311, 711312],
            'denue_parque_natural':[712190], 'denue_papelerias':[465311],
            'denue_libros':[465312], 'denue_revistas_periodicos':[465313],
            'denue_ferreteria_tlapaleria':[467111],
            'denue_art_limpieza':[467115], 'denue_pintura':[467113],
            'denue_peluqueria':[812110]
            
            }
    main(schema, folder_sufix, year, amenities, save = True)