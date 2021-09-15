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
    df = pd.read_json("/home/jovyan/work/scripts/areas.json")
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
        hex_pop = gpd.GeoDataFrame()
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
            query = f"SELECT * FROM processed.hex_bins_pop WHERE \"CVEGEO\" LIKE \'{m}%%\'"
            hex_pop = hex_pop.append(aup.gdf_from_query(query, geometry_col='geometry'))
            aup.log(f"Donwloaded hex bins for {m}")
            
        #Define projections
        mun_gdf = mun_gdf.set_crs("EPSG:4326")
        hex_bins = hex_bins.set_crs("EPSG:4326")

        # Creates query to download nodes from the metropolitan area or capital
        G, nodes, edges = aup.graph_from_hippo(mun_gdf, 'osmnx')
        aup.log(f"Downloaded {len(nodes)} nodes and {len(edges)} from database for {c}")

        #Creates wkt for query
        gdf_tmp = mun_gdf.copy()
        gdf_tmp = gdf_tmp.to_crs("EPSG:6372")
        gdf_tmp = gdf_tmp.buffer(1).reset_index().rename(columns={0:'geometry'})
        gdf_tmp = gdf_tmp.to_crs("EPSG:4326")
        poly_wkt = gdf_tmp.dissolve().geometry.to_wkt()[0]
        aup.log("Created wkt based on dissolved polygon")

        nodes_amenities_street = gpd.GeoDataFrame()
        nodes_amenities_linear = gpd.GeoDataFrame()
        hex_street = hex_bins
        hex_linear = hex_bins
        i = 0
        
        for a in amenities:
            nodes_calc = nodes
            denue_amenity = gpd.GeoDataFrame()
            denue = gpd.GeoDataFrame()
            for cod in amenities[a]:
                query = f"SELECT * FROM denue.denue_2020 WHERE (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')) AND (\"codigo_act\" = {cod})"
                denue = denue.append(aup.gdf_from_query(query, geometry_col='geometry'))
                query = f"SELECT * FROM denue_nodes.denue_node_2020 WHERE (ST_Intersects(geometry, \'SRID=4326;{poly_wkt}\')) AND (\"codigo_act\" = {cod})"
                denue_amenity = denue_amenity.append(aup.gdf_from_query(query, geometry_col='geometry'))
                aup.log(f"Downloaded {len(denue_amenity)} {a} from database for {c}")
            denue_points = denue[['id','codigo_act', 'geometry']]
            denue_points['x'] = denue['longitud'].copy()
            denue_points['y'] = denue['latitud'].copy()
            ##Calculate distance travelling by street
            nodes_distance = gpd.GeoDataFrame()
            nodes_distance = nodes_calc
            df_temp = gpd.GeoDataFrame()
            df_temp = nodes_calc
            c_denue = len(denue_amenity)/150
            for k in range(int(c_denue)+1):
                aup.log(f"Starting range k = {k} of {int(c_denue)}")
                denue_process = denue_amenity.iloc[int(150*k):int(150*(1+k))].copy()
                nodes_distance_prep = aup.calculate_distance_nearest_poi(denue_process, nodes_calc, edges, a, 'osmid')
                df_int = pd.DataFrame()
                df_int['dist_'+str(k)+a] = nodes_distance_prep['dist_'+a]
                df_temp = df_temp.merge(df_int, left_index=True, right_index=True)
            aup.log(f"finished")
            df_temp.drop(['x', 'y', 'street_count','geometry'], inplace = True, axis=1)
            df_min = pd.DataFrame()
            df_min['dist_'+a] = df_temp.min(axis=1)
            nodes_distance = nodes_distance.merge(df_min, left_index=True, right_index=True)
            aup.log(f"Calculated street distance for a TOTAL of {len(nodes_distance)} nodes")
            ## Calculate distance in a straight line
            nodes_linear = gpd.GeoDataFrame()
            nodes_linear = nodes_calc
            df_temp = gpd.GeoDataFrame()
            df_temp = nodes_calc
            c_denue = len(denue)/150
            for k in range(int(c_denue)+1):
                aup.log(f"Starting range k = {k} of {int(c_denue)}")
                nodes_tmp = nodes_calc
                denue_process = denue_points.iloc[int(150*k):int(150*(1+k))].copy()
                nodes_linear_prep= aup.find_nearest(G, denue_process, nodes_tmp, return_distance=True)
                df_int = pd.DataFrame()
                df_int['dist_'+str(k)+a] = nodes_linear_prep['distance_node']
                df_temp = df_temp.merge(df_int, left_index=True, right_index=True)
            aup.log(f"finished")
            df_temp.drop(['x', 'y', 'street_count','geometry', 'osmid'], inplace = True, axis=1)
            df_min = pd.DataFrame()
            df_min['dist_'+a] = df_temp.min(axis=1)
            nodes_linear = nodes_linear.merge(df_min, left_index=True, right_index=True)
            nodes_linear.drop(['osmid'], inplace = True, axis=1)
            aup.log(f"Calculated linear distance for a TOTAL of {len(nodes_linear)} nodes")
            nodes.drop(['osmid', 'distance_node'], inplace = True, axis=1)
        
            #Data to hex_bins
            nodes_distance.reset_index(inplace=True)
            nodes_linear.reset_index(inplace=True)
            nodes_distance = nodes_distance.set_crs("EPSG:4326")
            nodes_linear = nodes_linear.set_crs("EPSG:4326")
            hex_bins = hex_bins.set_crs("EPSG:4326")
            hex_dist_street = aup.group_by_hex_mean(nodes_distance, hex_bins, resolution, a)
            hex_dist_lin = aup.group_by_hex_mean(nodes_linear, hex_bins, resolution, a)
            hex_street = hex_street.merge(hex_dist_street[['hex_id_'+str(resolution),'dist_'+a]], 
            on='hex_id_'+str(resolution))
            hex_linear = hex_linear.merge(hex_dist_lin[['hex_id_'+str(resolution),'dist_'+a]], 
            on='hex_id_'+str(resolution))
            aup.log(f"Added distance data to {a} to {len(hex_bins)} hex bins")

            if i == 0:
                nodes_amenities_street = nodes_distance[['osmid','x','y','geometry','dist_'+a]]
                nodes_amenities_linear = nodes_linear[['osmid','x','y','geometry','dist_'+a]]
            else:
                nodes_amenities_street = nodes_amenities_street.merge(
                    nodes_distance[['osmid','dist_'+a]], on='osmid')
                nodes_amenities_linear = nodes_amenities_linear.merge(
                    nodes_linear[['osmid','dist_'+a]], on='osmid')
            aup.log('Added nodes distance to nodes_amenities')
            i += 1
            hex_street = hex_street.set_crs("EPSG:4326")
            hex_linear = hex_linear.set_crs("EPSG:4326")
            nodes_amenities_street = nodes_amenities_street.set_crs("EPSG:4326")
            nodes_amenities_linear = nodes_amenities_linear.set_crs("EPSG:4326")
        
        #Define projections
        hex_street = hex_street.set_crs("EPSG:4326")
        hex_linear = hex_linear.set_crs("EPSG:4326")
        hex_pop = hex_pop.set_crs("EPSG:4326")

        # keep relevant columns in gdf_pop: ID and total population
        hex_pop = hex_pop[['hex_id_8', 'pobtot']]

        # merge hexes with distance with hexes with population
        pop_street = hex_street.merge(hex_pop, on= 'hex_id_8')
        pop_linear = hex_linear.merge(hex_pop, on= 'hex_id_8')

        #### Assign mixed grade schools as a wildcard: As long as there is an elementary 
        # or middle school ithin range, a mixed grade school 
        # within range can substitute one of the missing ones

        # Make mixed level schools a "Wild card" for elementary and middle schools for street distance´

        dist_prim_mix = pop_street[['dist_mixto', 'dist_primaria']]
        pop_street['dist_prim_mix'] = dist_prim_mix.min(axis=1)

        dist_secun_mix = pop_street[['dist_mixto', 'dist_secundaria']]
        pop_street['dist_secun_mix'] = dist_secun_mix.min(axis=1)


        # find the longest distance to any of the 3 amenities in streets
        hex_max = pop_street[['dist_salud', 'hex_id_8']]
        hex_max = hex_max.rename(columns={'dist_salud':'dist_max'})
        dist_street = pop_street[['dist_salud', 'dist_prim_mix', 'dist_secun_mix', 'dist_primaria', 'dist_secundaria']]

        for r in range(int(len(pop_street))):
            secun = dist_street.loc[[r], ['dist_secundaria']]
            secun = secun.values
            prim = dist_street.loc[[r], ['dist_primaria']]
            prim = prim.values
            if ((secun<=2500) or(prim<=2500) and(secun !=0) and (prim!= 0)):
                comp = dist_street[['dist_salud', 'dist_prim_mix', 'dist_secun_mix']]
                maxim = comp.max(axis =1)
                insertion = maxim.iloc[r]
                hex_max.at[r, 'dist_max'] = insertion
            else:
                comp = dist_street[['dist_salud', 'dist_primaria', 'dist_secundaria']]
                maxim = comp.max(axis =1)
                insertion = maxim.iloc[r]
                hex_max.at[r, 'dist_max'] = insertion
        dist_max = hex_max['dist_max']
        pop_street = pop_street.merge(dist_max, left_index = True, right_index = True)

        # Make mixed level schools a "Wild card" for elementary and middle schools for LINEAR distance´

        dist_prim_mix = pop_linear[['dist_mixto', 'dist_primaria']]
        pop_linear['dist_prim_mix'] = dist_prim_mix.min(axis=1)

        dist_secun_mix = pop_linear[['dist_mixto', 'dist_secundaria']]
        pop_linear['dist_secun_mix'] = dist_secun_mix.min(axis=1)


        # find the longest distance to any of the 3 amenities in LINEAR
        hex_max = pop_linear[['dist_salud', 'hex_id_8']]
        hex_max = hex_max.rename(columns={'dist_salud':'dist_max'})
        dist_linear = pop_linear[['dist_salud', 'dist_prim_mix', 'dist_secun_mix', 'dist_primaria', 'dist_secundaria']]

        for r in range(int(len(pop_linear))):
            secun = dist_linear.loc[[r], ['dist_secundaria']]
            secun = secun.values
            prim = dist_linear.loc[[r], ['dist_primaria']]
            prim = prim.values
            if ((secun<=2500) or(prim<=2500) and(secun !=0) and (prim!= 0)):
                comp = dist_linear[['dist_salud', 'dist_prim_mix', 'dist_secun_mix']]
                maxim = comp.max(axis =1)
                insertion = maxim.iloc[r]
                hex_max.at[r, 'dist_max'] = insertion
            else:
                comp = dist_linear[['dist_salud', 'dist_primaria', 'dist_secundaria']]
                maxim = comp.max(axis =1)
                insertion = maxim.iloc[r]
                hex_max.at[r, 'dist_max'] = insertion
        dist_max = hex_max['dist_max']
        pop_linear = pop_linear.merge(dist_max, left_index = True, right_index = True)

        summary_linear = pop_linear[['geometry', 'hex_id_8', 'CVEGEO', 'pobtot', 'dist_max']]
        summary_street = pop_street[['geometry', 'hex_id_8', 'CVEGEO', 'pobtot', 'dist_max']]

        compare_street_linear = pop_linear[['geometry', 'hex_id_8', 'CVEGEO', 'pobtot']]
        compare_street_linear['dist_lin'] = summary_linear['dist_max']
        compare_street_linear['dist_street'] = summary_street['dist_max']
        compare_street_linear['bool_street'] = 0
        compare_street_linear['bool_lin'] = 0
        for s in range(int(len(compare_street_linear))):
            linear = compare_street_linear.loc[[s], ['dist_lin']]
            linear = linear.values
            street = compare_street_linear.loc[[s], ['dist_street']]
            street = street.values
            if 0<linear<=2500:
                compare_street_linear.at[s, 'bool_lin'] = 3
            if 0<street<=2500:
                compare_street_linear.at[s, 'bool_street'] = 1
            if linear == 0:
                compare_street_linear.at[s, 'bool_lin'] = -100
            if street == 0:
                compare_street_linear.at[s, 'bool_street'] = -100
            if linear>2500:
                compare_street_linear.at[s, 'bool_lin'] = 0
            if street>2500:
                compare_street_linear.at[s, 'bool_street'] = 0
        compare_street_linear['bool_tot'] = compare_street_linear['bool_lin'] + compare_street_linear['bool_street']
        
        if save:
            aup.gdf_to_db_slow(pop_street, "hex_street", schema=schema, if_exists="append")
            aup.gdf_to_db_slow(pop_linear, "hex_linear", schema=schema, if_exists="append")
            aup.gdf_to_db_slow(summary_street, "summary_street_", schema=schema, if_exists="append")
            aup.gdf_to_db_slow(summary_linear, "summary_linear_", schema=schema, if_exists="append")
            aup.gdf_to_db_slow(compare_street_linear, "compare_hexes", schema=schema, if_exists="append")

            c_nodes = len(nodes_amenities_street)/10000
            for p in range(int(c_nodes)+1):
                nodes_upload = nodes_amenities_street.iloc[int(10000*p):int(10000*(p+1))].copy()
                aup.gdf_to_db_slow(nodes_upload, "nodes_street_"+folder_sufix, schema=schema, if_exists="append")
                aup.log("uploaded nodes into DB ")
            c_nodes = len(nodes_amenities_linear)/10000
            for p in range(int(c_nodes)+1):
                nodes_upload = nodes_amenities_linear.iloc[int(10000*p):int(10000*(p+1))].copy()
                aup.gdf_to_db_slow(nodes_upload, "nodes_linear_"+folder_sufix, schema=schema, if_exists="append")
                aup.log("uploaded nodes into DB ")
            aup.log("Finished uploading nodes ")



if __name__ == "__main__":
    aup.log('--'*10)
    aup.log('Starting script')
    year = '2020'
    schema = 'infonavit'
    folder_sufix = 'dist' #sufix for folder name
    amenities = {'primaria':[611121,611122], 
    'secundaria':[611131,611132],
    'mixto':[611171,611172],
    'salud':[621111,621112,621113,621114,621115,621116,621491,621492,622111,622112]}
    save = True
    main(schema, folder_sufix, year, amenities, save = save)