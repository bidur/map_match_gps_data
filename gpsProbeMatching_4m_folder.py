import warnings
warnings.filterwarnings("ignore")
from datetime import datetime ,date

import sys,os
sys.path.append('py/') 

import shutil
from functions import initialize, get_points_within_target_region, df2gdf, plot_map, check_dir, check_dir, get_all_files_from_dir
from preprocess import preprocess_data
from map_matching import map_match_csv2gpx, map_match_csv2gpx_multithread
from generate_route_by_pyroutelib import generate_osm_routes_main
from config import max_threads, OUTPUT_DIR, INPUT_DIR, map_matched_gps_probe

from get_raw_files import get_raw_files_main


#### DEFINE INPUT OUTPUT DIRs
'''
raw_dir = '/home/bidur/map_match_gps_data/raw_phl'
mapmatching_input_dir = '/home/bidur/map_match_gps_data/raw_phl/phl_sample_clean/'
backup_dir ='/home/bidur/map_match_gps_data/raw_phl/bak/'
'''
raw_dir = '/mnt/lv/heromiya/PHLMobilityData/'    ## original raw file location
mapmatching_input_dir = '/mnt/lv/bidur/PHL_raw_data/clean_input/201907/' ## prepare csv in requireds format(ap_id, ) for map-matching
backup_dir = '/mnt/lv/bidur/PHL_raw_data/csv_4_mobmap/201907/' 
  
    
   
#### DEFINE DATES TO FILTER
start_date = date(2019, 7, 1) # NPL_data_20190404.csv
end_date = date(2019, 7, 7)

#### csv from mapmatching_input_dir are processed and final csv are saved in backup_dir
#get_raw_files_main(raw_dir, mapmatching_input_dir, start_date , end_date)     #Files from raw_dir are processed and saved in mapmatching_input_dir 
  
 


#### Below loop, csv from mapmatching_input_dir are processed and final csv are saved in backup_dir
arr_input_csv = get_all_files_from_dir(mapmatching_input_dir)

print(arr_input_csv)
print("_____________________________________")
for gps_csv in arr_input_csv:
    
    csv_path, csv_name = os.path.split(gps_csv)
      
    
    if '20190703' in gps_csv:
        print("Completed 20190703")
        continue
    if '20190704' in gps_csv:
        print("Completed 20190704")
        continue
    if '20190706' in gps_csv:
        print("Completed 20190706")
        continue
    
    
    print(csv_name, 'START ', str( datetime.now() ))
 
    # 1. remove old data and create necessary directories
    initialize()

    # 2. ananymize ap_id column to int value ,   clip points within boundary
    #gdf_probe_clipped, gdf_target = get_points_within_target_region (gps_csv, anonymize=True, display_plot = False)
    gdf_probe_clipped, gdf_target = get_points_within_target_region (gps_csv, anonymize=False, display_plot = False)
    
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
    check_dir(backup_dir)# create directory if not exit
    
    final_data_path = backup_dir+csv_name
    shutil.copyfile(map_matched_gps_probe,final_data_path)
    #csv_path, csv_name = os.path.split(gps_csv)
    #time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    #shutil.copyfile('/mnt/lv/bidur/PHL_raw_data/map_match_gps_data/output/time_dist_log.csv',backup_dir + '0log_'+ time_str+ csv_name)
    
    print(gps_csv, 'END ', str( datetime.now() ))
    print('Saved at: ', final_data_path)
    print('__________________________________________________________________')
    

print("ALL TASK COMPLETED!!")
