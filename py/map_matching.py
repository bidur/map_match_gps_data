import os,sys,pathlib,glob

from csv2gpx import  prepare_csv_files, convert_csv2gpx
from gpx2csv import convert_resgpx2csv

from config import target_osm_pbf, max_threads
from config import  input_anonymized_clipped, input_preprocessed, map_matched_gps_probe
from config import ROOT_DIR, MAP_MATCHING_PATH, CSV_DIR, OUTPUT_DIR, GPX_DIR, RES_CSV_DIR, BATCH_OUTPUT_DIR
from functions import write2file,check_dir

from datetime import datetime 
import pandas as pd
  


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
	
	print('MAP_MATCHING_PATH: ', MAP_MATCHING_PATH)
	print ('GPX_DIR: ', GPX_DIR)
	
	input_gpx_dir = GPX_DIR#.replace(MAP_MATCHING_PATH,'')  

	input_gpx_files = os.path.join(input_gpx_dir, '*.gpx') #input_gpx_dir + '*.gpx'
	create_route_command ='java -jar matching-web/target/graphhopper-map-matching-web-1.0-SNAPSHOT.jar match '+ input_gpx_files	
	os.system(create_route_command)
	print ('\ncompleted: ' ,create_route_command)
	
	os.chdir (ROOT_DIR)
	
	#os.chdir(os.path.dirname(__file__)) # change dir to  current .py file dir
	
	#print ('MAP dir: ', MAP_MATCHING_PATH)
	#print ('RUnning FILE DIR: ', os.path.dirname(__file__) )
	#print("Current Working Directory " , os.getcwd())
	
	return None

def apply_map_matching_multithread(thread_id):  
    print('< apply_map_matching_multithread() >')
    
    os.chdir(MAP_MATCHING_PATH)
    print("Current Working Directory " , os.getcwd())
    
    #print('MAP_MATCHING_PATH: ', MAP_MATCHING_PATH)
    
    thread_gpx_dir = pathlib.Path(GPX_DIR, str(thread_id))
    print ('GPX_DIR: ', thread_gpx_dir)

    input_gpx_files = os.path.join(thread_gpx_dir, '*.gpx') #input_gpx_dir + '*.gpx'
    create_route_command ='java -jar matching-web/target/graphhopper-map-matching-web-1.0-SNAPSHOT.jar match '+ input_gpx_files    
    os.system(create_route_command)
    print ('\n Thread #'+str(thread_id)+' completed: ' ,create_route_command)
    
    os.chdir (ROOT_DIR)
    
    #os.chdir(os.path.dirname(__file__)) # change dir to  current .py file dir
    
    #print ('MAP dir: ', MAP_MATCHING_PATH)
    #print ('RUnning FILE DIR: ', os.path.dirname(__file__) )
    #print("Current Working Directory " , os.getcwd())
    
    return None


def map_match_csv2gpx(df_sample):
    # Apply graphhopper MapMatching
    
    
    #1.prepare map cache
    write2file("prepare_map_cache(),START," + str( datetime.now() )  )
    prepare_map_cache()
    write2file("prepare_map_cache(),END," +  str( datetime.now() )  )
   
    
    
    
    #2.prepare input to graphhopper
    prepare_csv_files(df_sample,CSV_DIR) # csv2gpx.py
    convert_csv2gpx(CSV_DIR,GPX_DIR)
    
    # 3. apply map matching
    write2file(",apply_map_matching(),START," +  str( datetime.now() )  )
    apply_map_matching() # match input GPS probe to road network
    write2file(",apply_map_matching(),END," +  str( datetime.now() )  )
    
    write2file(",convert_resgpx2csv(),START," +  str( datetime.now() )  )
    # 4. convert result to csv and popuate timestamp based on the input file
    df_mapped_route = convert_resgpx2csv(CSV_DIR, GPX_DIR, RES_CSV_DIR) # Convert road mapped GPS Probe to CSV and update timestamp
    write2file(",convert_resgpx2csv(),END," +  str( datetime.now() )  )
    
    # gpx2csv.py -> get a single file
    
    if 'id' not in df_mapped_route.columns:
        df_mapped_route.insert(0,'id',range(len(df_mapped_route)))# add a new id column
        
    df_mapped_route.to_csv(map_matched_gps_probe,index=False) # mobmap visualization without using OSM routes from pyRouteLib3

    print ("\n\n Map Matching and route generation completed")
    print (map_matched_gps_probe)
    #shutil.copy(output_file, map_matched_gps_probe)
    
    return df_mapped_route

def map_match_csv2gpx_multithread(df_sample):
    # Apply graphhopper MapMatching
    print('< map_match_csv2gpx_multithread() >')
    
    
    #1.prepare map cache
    write2file( str(max_threads) +",prepare_map_cache(),START," + str( datetime.now() )  )
    prepare_map_cache()
    write2file( str(max_threads) +",prepare_map_cache(),END," +  str( datetime.now() )  )
    
    print("<< multithreaded_process() -> START " +  str( datetime.now() )  )

    write2file( str(max_threads) +",multithreaded_process(),START," + str( datetime.now() )  )
    df_mapped_route = multithreaded_process(df_sample)
    write2file( str(max_threads) +",multithreaded_process(),END," + str( datetime.now() )  )
    print( "<< multithreaded_process() -> END " + str( datetime.now() )  )
    
    return df_mapped_route
    '''
    #2.prepare input to graphhopper
    prepare_csv_files(df_sample,CSV_DIR) # csv2gpx.py
    convert_csv2gpx(CSV_DIR,GPX_DIR)
    
    # 3. apply map matching
    apply_map_matching() # match input GPS probe to road network
    
    
    # 4. convert result to csv and popuate timestamp based on the input file
    df_mapped_route = convert_resgpx2csv(CSV_DIR, GPX_DIR, RES_CSV_DIR) # Convert road mapped GPS Probe to CSV and update timestamp
    
    # gpx2csv.py -> get a single file
    
    if 'id' not in df_mapped_route.columns:
        df_mapped_route.insert(0,'id',range(len(df_mapped_route)))# add a new id column
        
    df_mapped_route.to_csv(map_matched_gps_probe,index=False) # mobmap visualization without using OSM routes from pyRouteLib3

    print ("\n\n Map Matching and route generation completed")
    print (map_matched_gps_probe)
    #shutil.copy(output_file, map_matched_gps_probe)
    
    return df_mapped_route
    '''

def merge_csv(path):
    
    cmmd = "awk 'FNR > 1' "+ str(path) +"/*.csv > " + str( map_matched_gps_probe)
    os.system(cmmd)
    df_mapped_route = pd.read_csv(map_matched_gps_probe, header=None, names=["ap_id", "timestamp", "latitude", "longitude"])
    if 'id' not in df_mapped_route.columns:
        df_mapped_route.insert(0,'id',range(len(df_mapped_route)))# add a new id column
    df_mapped_route.to_csv(map_matched_gps_probe,index=False) # mobmap visualization without using OSM routes from pyRouteLib3

    print ("\n\n Map Matching and route generation completed")
    print (map_matched_gps_probe)
    
    return df_mapped_route


import threading

def process_batch(thread_id, df_slice):
    print('< process_batch() >')

    #print('thread : ', thread_id)
    #print('ap_ids : ', arr_ap_id)
    #display(df_slice.head())
    #INPUT_DIR         = pathlib.Path(ROOT_DIR, 'input')
        
    thread_csv_dir = pathlib.Path(CSV_DIR, str(thread_id))
    check_dir (  thread_csv_dir  )
    thread_gpx_dir = pathlib.Path(GPX_DIR, str(thread_id))
    check_dir (  thread_gpx_dir  )
    thread_res_csv_dir = pathlib.Path(RES_CSV_DIR, str(thread_id))
    check_dir (  thread_res_csv_dir  )
   
    
    
    #2.prepare input to graphhopper
    prepare_csv_files(df_slice,thread_csv_dir) # csv2gpx.py
    convert_csv2gpx(thread_csv_dir,thread_gpx_dir)
    
    # 3. apply map matching
    apply_map_matching_multithread(thread_id) # match input GPS probe to road network
    
    
    # 4. convert result to csv and popuate timestamp based on the input file
    df_mapped_route = convert_resgpx2csv(thread_csv_dir, thread_gpx_dir, thread_res_csv_dir) # Convert road mapped GPS Probe to CSV and update timestamp
    
    batch_op_file =  pathlib.Path(BATCH_OUTPUT_DIR, 'thr_'+str(thread_id)+'.csv')
    df_mapped_route[['ap_id','timestamp','latitude','longitude']].to_csv(batch_op_file, index=False)
    


def multithreaded_process( df_sample):
    
    print('< multithreaded_process() >')
    
    # break the task into smaller slices
    arr_ap_ids = list(df_sample.ap_id.unique())
    slice_size = int( len(arr_ap_ids) / max_threads )
    
    
    # assign to threads
    arr_thr = []
    arr_ap_ids_slice = []
    for i in range(max_threads):
        
        slice_start = i* slice_size
        slice_end = (i+1) * slice_size
        
        #print (i ,' slice_start: ', slice_start , 'slice_end: ', slice_end)
        
        arr_ap_ids_slice = arr_ap_ids[slice_start:slice_end]        
        if (i+1 == max_threads): # last slice
            arr_ap_ids_slice = arr_ap_ids[slice_start:]
            
        df_slice = df_sample[df_sample['ap_id'].isin(arr_ap_ids_slice)]
            
        thr  = threading.Thread(target=process_batch, args=(i, df_slice ) )
        thr.start()
        arr_thr.append(thr)
  
    # Wait for thread to finish
    for i in range(len(arr_thr)):
        arr_thr[i].join()
    
    print('Thread operation  complete')
    
    df_mapped_route = merge_csv(BATCH_OUTPUT_DIR)
    
    return df_mapped_route

    
    '''
    if 'id' not in df_mapped_route.columns:
        df_mapped_route.insert(0,'id',range(len(df_mapped_route)))# add a new id column

    df_mapped_route.to_csv(map_matched_gps_probe,index=False) # mobmap visualization without using OSM routes from pyRouteLib3

    print ("\n\n Map Matching and route generation completed")
    print (map_matched_gps_probe)
    #shutil.copy(output_file, map_matched_gps_probe)
    
    return df_mapped_route
    '''


