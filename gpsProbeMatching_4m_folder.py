import warnings
warnings.filterwarnings("ignore")
from datetime import datetime ,date

import sys,os
sys.path.append('py/') 

import shutil
from functions import initialize, get_points_within_target_region, df2gdf, plot_map, check_dir, get_all_files_from_dir
from preprocess import preprocess_data
from map_matching import map_match_csv2gpx, map_match_csv2gpx_multithread
from generate_route_by_pyroutelib import generate_osm_routes_main
from config import max_threads, OUTPUT_DIR, INPUT_DIR, map_matched_gps_probe

from get_raw_files import get_raw_files_main

raw_dir = '/mnt/lv/heromiya/PHLMobilityData/'    ## original raw file location
mapmatching_input_dir = '/mnt/lv/bidur/PHL_raw_data/clean_input/new/' ## prepare csv in requireds format(ap_id, ) for map-matching
backup_dir = '/mnt/lv/bidur/PHL_raw_data/csv_4_mobmap/new/' 
check_dir(backup_dir)
start_date = date(2019, 7, 1) # PDP_philippines_20190404.csv
end_date = date(2019, 7, 7)

print ("Map Matching PHL data for July1 - July7")
# uncomment below to prepare csv into <mapmatching_input_dir>     
get_raw_files_main(raw_dir, mapmatching_input_dir, start_date , end_date)    


#mapmatching_input_dir = '/mnt/lv/bidur/PHL_raw_data/clean_input/'
#input_dir = '/home/bidur/map_match_gps_data/raw_data/phl_test_csv/'
arr_input_csv = get_all_files_from_dir(mapmatching_input_dir)

print("\n\n Start: Process Daily Data")
for gps_csv in arr_input_csv:
    
    print(gps_csv, 'START ', str( datetime.now() ))
 
    # 1. remove old data and create necessary directories
    initialize()

    # 2. ananymize ap_id column to int value ,   clip points within boundary
    gdf_probe_clipped, gdf_target = get_points_within_target_region (gps_csv, anonymize=True, display_plot = False)
    #print('----2 done----')
    
    
    # 3. Preprocess: cleaning data & applying sampling
    df_sample = preprocess_data()
    #print('----3 done----')

    # 4. map matching with osm roads using graphhopper
    df_mapped_route = map_match_csv2gpx_multithread(df_sample) # multithreaded
    
    #Use this for normal execution without multi thread ( IF user permission do not allow multi-threading):
    #df_mapped_route = map_match_csv2gpx(df_sample)
    #print('----4 done----')
    
        
    # 5. save final csv to backup dir
    csv_path, csv_name = os.path.split(gps_csv)
    final_data_path = backup_dir+csv_name
    shutil.copyfile(map_matched_gps_probe,final_data_path)
    
    print(gps_csv, 'END ', str( datetime.now() ))
    print('Saved at: ', final_data_path)
    print('__________________________________________________________________')
    

print ("---END---")