import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd

import shutil,os,sys


from config import shp_target_region, shp_col_name, shp_target_value, sampling_percent

from config import input_file, input_anonymized, input_anonymized_clipped
from config import INPUT_DIR, OUTPUT_DIR,GPX_DIR, RES_CSV_DIR, CSV_DIR, CACHE_LOC_DIR

from annomize import anonymize_column_values


def initialize():  
    # remove old files
    remove_dir(OUTPUT_DIR)
    remove_dir(INPUT_DIR)
    remove_dir(GPX_DIR)
    remove_dir(CSV_DIR)
    remove_dir(RES_CSV_DIR)
    remove_dir(CACHE_LOC_DIR) # remove old map cache


    # create necessary directories
    check_dir(INPUT_DIR)
    check_dir(OUTPUT_DIR)
    check_dir(GPX_DIR)
    check_dir(CSV_DIR)
    check_dir(RES_CSV_DIR)
    
    print("Cleaning dirs: input/ and output/ ")
    return



def check_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def remove_dir(path):
    if  os.path.exists(path):
         #os.system("rm -rf "+path)
         shutil.rmtree(path)

 
        

   
def df2gdf(df_raw):
    
    # convert to geoDataFrame
    gdf_probe = gpd.GeoDataFrame(
        df_raw, geometry=gpd.points_from_xy(df_raw.longitude, df_raw.latitude))
    gdf_probe.crs = 'epsg:4326'
    #gdf_probe = gdf_probe.to_crs(epsg=3857) 
    #gdf_probe.crs 
    return gdf_probe

def clip_points(gdf_probe, gdf_shp):
    gdf_target = gdf_shp.query(shp_col_name +"=='"+ shp_target_value +"'")
    #gdf_target = gdf_shp[gdf_shp.NAME_2 == 'Bagmati']
    gdf_probe_clipped = gpd.clip(gdf_probe, gdf_target)
    if len(gdf_probe_clipped)==0:
        sys.exit("No Points in selected Region")
    return gdf_probe_clipped, gdf_target

def plot_map(gdf_probe_clipped, gdf_shp, msg='Map'):
    fig, ax = plt.subplots(figsize=(18, 8))
    gdf_probe_clipped.plot(ax=ax, color="purple")
    gdf_shp.boundary.plot(ax=ax)
    gdf_shp.boundary.plot(ax=ax, color="cyan")
    ax.set_title(msg, fontsize=20)
    ax.set_axis_off()
    plt.show()
    return
  

def get_points_within_target_region(gps_csv, display_plot = False):
    # save original input file
    #shutil.copy(gps_csv, input_file)
    df = pd.read_csv(gps_csv) # pandas can deal with encodings
    df.to_csv(input_file,index=False)
        
    # anonymize id column
    anonymize_column_values( 'ap_id', input_file, input_anonymized)
    
    df_raw    = pd.read_csv(input_anonymized)
    df_raw['ap_id'] = df_raw['ap_id'].apply(str) # ap_id are assumed to be string
    gdf_probe = df2gdf(df_raw)
    
    gdf_shp = gpd.read_file(shp_target_region) # shapefile is already 'epsg:4326'
    gdf_shp.head(1)

    #gdf_shp = gdf_shp.to_crs(epsg=3857) 
    #gdf_shp.crs
    gdf_probe_clipped,gdf_target = clip_points(gdf_probe, gdf_shp)
    
    # plot the clipped
    if display_plot:
        plot_map(gdf_probe, gdf_shp, "ALL Points ")    
        plot_map(gdf_probe_clipped, gdf_target, "Target Area Points Clipped") 
    
    df_selected_cols = gdf_probe_clipped[['ap_id','timestamp','latitude','longitude']]
    df_selected_cols.to_csv(input_anonymized_clipped,index=False)
    return df_selected_cols, gdf_target#gdf_probe_clipped[['ap_id','timestamp','latitude','longitude']]
    
