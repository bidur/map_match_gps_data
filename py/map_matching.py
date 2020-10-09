import os

from csv2gpx import  prepare_csv_files, convert_csv2gpx
from gpx2csv import convert_resgpx2csv

from config import target_osm_pbf
from config import  input_anonymized_clipped, input_preprocessed, map_matched_gps_probe
from config import ROOT_DIR, MAP_MATCHING_PATH, CSV_DIR, OUTPUT_DIR, GPX_DIR, RES_CSV_DIR


def prepare_map_cache():
		
	create_map_cache_command ='java -jar matching-web/target/graphhopper-map-matching-web-1.0-SNAPSHOT.jar import map-data/'+target_osm_pbf
	os.chdir(MAP_MATCHING_PATH)
	os.system(create_map_cache_command)
	#os.chdir(os.path.dirname(__file__)) # change dir to  current .py file dir
	os.chdir (ROOT_DIR)
	
	print ('\ncompleted: ' ,create_map_cache_command)
	
	return None


def apply_map_matching():	
	
	os.chdir(MAP_MATCHING_PATH)
	print("Current Working Directory " , os.getcwd())
	
	input_gpx_files = GPX_DIR.replace(MAP_MATCHING_PATH,'') + '*.gpx'
	create_route_command ='java -jar matching-web/target/graphhopper-map-matching-web-1.0-SNAPSHOT.jar match '+ input_gpx_files	
	os.system(create_route_command)
	print ('\ncompleted: ' ,create_route_command)
	
	os.chdir (ROOT_DIR)
	
	#os.chdir(os.path.dirname(__file__)) # change dir to  current .py file dir
	
	#print ('MAP dir: ', MAP_MATCHING_PATH)
	#print ('RUnning FILE DIR: ', os.path.dirname(__file__) )
	#print("Current Working Directory " , os.getcwd())
	
	return None


def map_match_csv2gpx(df_sample):
	
	prepare_csv_files(df_sample,CSV_DIR) # csv2gpx.py
	convert_csv2gpx(CSV_DIR,GPX_DIR)
	
	# Apply graphhopper MapMatching
	#1.prepare map cache
	prepare_map_cache()
	# 2. apply map matching
	apply_map_matching() # match input GPS probe to road network
	# 3. convert result to csv and popuate timestamp based on the input file
	df_mapped_route = convert_resgpx2csv(CSV_DIR, GPX_DIR, RES_CSV_DIR) # Convert road mapped GPS Probe to CSV and update timestamp
	
	# gpx2csv.py -> get a single file
	
	if 'id' not in df_mapped_route.columns:
		df_mapped_route.insert(0,'id',range(len(df_mapped_route)))# add a new id column
		
	df_mapped_route.to_csv(map_matched_gps_probe,index=False) # mobmap visualization without using OSM routes from pyRouteLib3

	print ("\n\n Map Matching and route generation completed")
	print (map_matched_gps_probe)
	#shutil.copy(output_file, map_matched_gps_probe)
	
	return df_mapped_route
