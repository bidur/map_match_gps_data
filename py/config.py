import pathlib

# target boundary # Philippines

shp_target_region = 'raw_data/phl/Country.shp'
shp_col_name      = 'NAME_ENGLI'   # config.py
shp_target_value  = 'Philippines'   # config.py
'''
# target boundary # NEPAL
shp_target_region = 'raw_data/gadm36_NPL_shp/gadm36_NPL_2.shp'
shp_col_name      = 'NAME_2'   # config.py
shp_target_value  = 'Gandaki'   # config.py
'''
# say how much data to be used?
sampling_percent = 100  # valid values 1 to 100
target_osm_pbf = 'philippines-latest.osm.pbf' #'philippines-latest.osm.pbf' #'nepal-latest.osm.pbf' #'kyushu-latest.osm.pbf'#

################ DO NOT EDIT BELOW
# Files and directory
ROOT_DIR 		=  '/home/bidur/map_match_gps_data/' #'C:/Users/epinurse/Desktop/PHL_Mobility_data/map_match_gps_data-main/'
PY_DIR		 = pathlib.Path(ROOT_DIR, 'py')

INPUT_DIR 		= pathlib.Path(ROOT_DIR, 'input')
OUTPUT_DIR 		= pathlib.Path(ROOT_DIR, 'output')
TEMP_DIR 		= pathlib.Path(OUTPUT_DIR, 'temp_csv') 

input_file = pathlib.Path(INPUT_DIR, '1_input.csv')  
input_anonymized = pathlib.Path(INPUT_DIR, '2_anonymized_input.csv') 
input_anonymized_clipped = pathlib.Path(INPUT_DIR, '3_anonymized_clipped.csv') 
input_preprocessed = pathlib.Path(INPUT_DIR, '4_preprocessed.csv')  # preprocessed sampled daa

######## map-matching

MAP_MATCHING_PATH 	= pathlib.Path(ROOT_DIR, 'map-matching-master')
GPX_DIR 			= pathlib.Path(MAP_MATCHING_PATH , 'matching-web/src/test/resources/target/')
CSV_DIR 			= pathlib.Path(INPUT_DIR, 'csv')
RES_CSV_DIR 		= pathlib.Path(OUTPUT_DIR ,'res_csv') # resultant of mapmatching
RES_CSV_GH_OP_DIR 	= pathlib.Path(OUTPUT_DIR ,'res_csv_gh_op') # resultant of mapmatching
CACHE_LOC_DIR 		= pathlib.Path(MAP_MATCHING_PATH , 'graph-cache')

## FInal Output file
map_matched_gps_probe = pathlib.Path(OUTPUT_DIR , '5_final_csv_4_mobmap.csv')

map_matched_gps_probe_pyroutelib = pathlib.Path(OUTPUT_DIR , '6_final_csv_4_mobmap.csv')


##------------- local OSM data file OR online
'''
OSM data is accessed online by pyrouteLib3. For local data option 
comment this line and uncomment the following line.
'''
osm_data_source = "" # for online data download from OSM
		
#osm_data_source =  "okinawa_main_roads_all.osm" 
#osm data location For offline processing
