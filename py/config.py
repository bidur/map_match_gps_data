
# target boundary # NEPAL
shp_target_region = 'raw_data/gadm36_NPL_shp/gadm36_NPL_2.shp'
shp_col_name      = 'NAME_2'   # config.py
shp_target_value  = 'Bagmati'   # config.py

# say how much data to be used?
sampling_percent = 5  # valid values 1 to 100
target_osm_pbf = 'nepal-latest.osm.pbf' #'philippines-latest.osm.pbf' #'nepal-latest.osm.pbf' #'kyushu-latest.osm.pbf'#

################ DO NOT EDIT BELOW
# Files and directory
ROOT_DIR 		= '/home/bidur/map_match_gps_data/'
PY_DIR		 = ROOT_DIR + 'py'

INPUT_DIR 		= ROOT_DIR +'input/'
OUTPUT_DIR 		= ROOT_DIR + 'output/'
TEMP_DIR 		= OUTPUT_DIR + 'temp_csv/'

input_file = INPUT_DIR + '1_input.csv'
input_anonymized = INPUT_DIR + '2_anonymized_input.csv'
input_anonymized_clipped = INPUT_DIR + '3_anonymized_clipped.csv'
input_preprocessed = INPUT_DIR + '4_preprocessed.csv' # preprocessed sampled daa

######## map-matching

MAP_MATCHING_PATH 	= ROOT_DIR + 'map-matching-master/'
GPX_DIR 			= MAP_MATCHING_PATH + 'matching-web/src/test/resources/target/'
CSV_DIR 			= INPUT_DIR+ 'csv/'
RES_CSV_DIR 		= OUTPUT_DIR +'res_csv/' # resultant of mapmatching
CACHE_LOC_DIR 		= MAP_MATCHING_PATH + 'graph-cache/'

## FInal Output file
map_matched_gps_probe = OUTPUT_DIR + '5_final_csv_4_mobmap.csv'

