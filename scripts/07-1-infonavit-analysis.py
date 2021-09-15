import os
import sys

import pandas as pd
import geopandas as gpd

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    import aup

# Read json with municipality codes by capital or metropolitan area
df = pd.read_json("/home/jovyan/work/scripts/areas.json")
aup.log("Read metropolitan areas and capitals json")

index = pd.Index(['Aguascalientes', 'Merida', 'Saltillo', 'Tampico', 'Monterrey', 'Guadalajara'])
hex_fulfill = pd.DataFrame(index = range(6))
hex_fulfill = hex_fulfill.set_index(index)
#Create columns to check
hex_fulfill['hex_tot']= 0
hex_fulfill['hex_pop_tot']= 0
# Streets
hex_fulfill['hex_urb_street']= 0
hex_fulfill['hex_all_street']= 0
hex_fulfill['hex_prim_street']= 0
hex_fulfill['hex_secu_street']= 0
hex_fulfill['hex_salud_street']= 0
#Linear
hex_fulfill['hex_urb_linear']= 0
hex_fulfill['hex_all_linear']= 0
hex_fulfill['hex_prim_linear']= 0
hex_fulfill['hex_secu_linear']= 0
hex_fulfill['hex_salud_linear']= 0
#Population
hex_fulfill['hex_urb_street_pop']= 0
hex_fulfill['hex_all_street_pop']= 0
hex_fulfill['hex_prim_street_pop']= 0
hex_fulfill['hex_secu_street_pop']= 0
hex_fulfill['hex_salud_street_pop']= 0

hex_fulfill['hex_urb_linear_pop']= 0
hex_fulfill['hex_all_linear_pop']= 0
hex_fulfill['hex_prim_linear_pop']= 0
hex_fulfill['hex_secu_linear_pop']= 0
hex_fulfill['hex_salud_linear_pop']= 0

# Iterate over municipality DataFrame columns to access each municipality code
for c in df.columns.unique():
    aup.log(f"\n Starting municipality filters for {c}")
    #ageb_gdf = gpd.GeoDataFrame()
    gdf_street = gpd.GeoDataFrame()
    gdf_linear = gpd.GeoDataFrame()
    gdf_compare = gpd.GeoDataFrame()
    # Iterates over municipality codes for each metropolitan area or capital
    for i in range(len(df.loc["mpos", c])):
        # Extracts specific municipality code
        m = df.loc["mpos", c][i]
        # Downloads municipality polygon according to code
        query = f"SELECT * FROM infonavit.hex_linear WHERE \"CVEGEO\" LIKE \'{m}\'"
        gdf_linear = gdf_linear.append(aup.gdf_from_query(query, geometry_col='geometry'))
        aup.log(f"Downloaded hex bins linear at: {c}")
        #Creates query to download hex bins
        query = f"SELECT * FROM infonavit.hex_street WHERE \"CVEGEO\" LIKE \'{m}%%\'"
        gdf_street = gdf_street.append(aup.gdf_from_query(query, geometry_col='geometry'))
        aup.log(f"Donwloaded hex bins street for {m}")
        query = f"SELECT * FROM infonavit.compare_hexes WHERE \"CVEGEO\" LIKE \'{m}%%\'"
        gdf_compare = gdf_compare.append(aup.gdf_from_query(query, geometry_col='geometry'))
        aup.log(f"Donwloaded hex bins compare for {m}")

    gdf_compare.to_file(c+'_hexes.shp')
    aup.log(f"Created Shapefile for {c}")
    #Define projections
    gdf_street = gdf_street.set_crs("EPSG:4326")
    gdf_linear = gdf_linear.set_crs("EPSG:4326")


    aup.log(f"Assigned total hex and total pop for {c}")
    #assign total hexes and total popuation to record dataframe
    hex_fulfill.at[c, 'hex_tot'] = int(len(gdf_street))
    hex_fulfill.at[c, 'hex_pop_tot'] = gdf_street['pobtot'].sum()
    aup.log(f"Dropped non connected for {c}")
    # drop unconnected hexes
    gdf_street = gdf_street[(gdf_street != 0).all(1)]
    gdf_linear = gdf_linear[(gdf_linear != 0).all(1)]

    hex_fulfill.at[c, 'hex_urb_street'] = int(len(gdf_street))
    hex_fulfill.at[c, 'hex_urb_linear'] = int(len(gdf_linear))
    

    #Assign urban population in hexes to record DataFrame
    hex_fulfill.at[c, 'hex_urb_street_pop']= gdf_street['pobtot'].sum()
    hex_fulfill.at[c, 'hex_urb_linear_pop']= gdf_linear['pobtot'].sum()
    aup.log(f"Assigned connected hex and pop for {c}")
    
    #Find how many hexes fulfill each requirement travelling by streets
    hex_valid_prim =gdf_street.loc[gdf_street['dist_prim_mix']<=2500]
    hex_pop_prim = hex_valid_prim['pobtot'].sum()

    hex_valid_secu =gdf_street.loc[gdf_street['dist_secun_mix']<=2500]
    hex_pop_secu = hex_valid_secu['pobtot'].sum()

    hex_valid_salud =gdf_street.loc[gdf_street['dist_salud']<=2500]
    hex_pop_salud = hex_valid_salud['pobtot'].sum()

    hex_valid_tot = gdf_street.loc[gdf_street['dist_max']<=2500]
    hex_pop_tot = hex_valid_tot['pobtot'].sum()
    aup.log(f"Calculated hexes and pop for each requirement  for STREET for {c}")

    hex_fulfill.at[c, 'hex_all_street'] = int(len(hex_valid_tot))
    hex_fulfill.at[c, 'hex_prim_street'] = int(len(hex_valid_prim))
    hex_fulfill.at[c, 'hex_secu_street'] = int(len(hex_valid_secu))
    hex_fulfill.at[c, 'hex_salud_street'] = int(len(hex_valid_salud))

    hex_fulfill.at[c, 'hex_all_street_pop'] = int(hex_pop_tot)
    hex_fulfill.at[c, 'hex_prim_street_pop'] = int(hex_pop_prim)
    hex_fulfill.at[c, 'hex_secu_street_pop'] = int(hex_pop_secu)
    hex_fulfill.at[c, 'hex_salud_street_pop'] = int(hex_pop_salud)
    aup.log(f"ASSIGNED hexes and pop for each requirement in STREET for {c}")

    #Find how many hexes fulfill each requirement by linear distance
    hex_valid_prim =gdf_linear.loc[gdf_linear['dist_prim_mix']<=2500]
    hex_pop_prim = hex_valid_prim['pobtot'].sum()

    hex_valid_secu =gdf_linear.loc[gdf_linear['dist_secun_mix']<=2500]
    hex_pop_secu = hex_valid_secu['pobtot'].sum()

    hex_valid_salud =gdf_linear.loc[gdf_linear['dist_salud']<=2500]
    hex_pop_salud = hex_valid_salud['pobtot'].sum()

    hex_valid_tot = gdf_linear.loc[gdf_linear['dist_max']<=2500]
    hex_pop_tot = hex_valid_tot['pobtot'].sum()
    aup.log(f"Calculated hexes and pop for each requirement in LINEAR for {c}")

    hex_fulfill.at[c, 'hex_all_linear'] = int(len(hex_valid_tot))
    hex_fulfill.at[c, 'hex_prim_linear'] = int(len(hex_valid_prim))
    hex_fulfill.at[c, 'hex_secu_linear'] = int(len(hex_valid_secu))
    hex_fulfill.at[c, 'hex_salud_linear'] = int(len(hex_valid_salud))

    hex_fulfill.at[c, 'hex_all_linear_pop'] = int(hex_pop_tot)
    hex_fulfill.at[c, 'hex_prim_linear_pop'] = int(hex_pop_prim)
    hex_fulfill.at[c, 'hex_secu_linear_pop'] = int(hex_pop_secu)
    hex_fulfill.at[c, 'hex_salud_linear_pop'] = int(hex_pop_salud)
    aup.log(f"ASSIGNED hexes and pop for each requirement in LINEAR for {c}")


hex_fulfill.to_csv('fulfill_hexes_final.csv')
