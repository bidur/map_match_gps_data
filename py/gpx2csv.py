from xml.dom import minidom
import pandas as pd
import geopandas as gpd 
import datetime
from shapely.geometry import Point # converting latitude/longtitude to geometry
from shapely.ops import nearest_points
import numpy as np
import datetime
import os,glob, shutil, pathlib
 
from functions import remove_dir,check_dir
from functions import write2file

from config import RES_CSV_GH_OP_DIR, OUTPUT_DIR, min_gps_points_allowed

def check_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
        
def remove_dir(path):
	if  os.path.exists(path):
		 #os.system("rm -rf "+path)
		 shutil.rmtree(path)
'''		 
def date_2_unix_sec(d):
    epoch = datetime.datetime.utcfromtimestamp(0)
    dt = None
    try:
        dt = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S.%f')
    except:
        dt=d
    
    return (dt - epoch).total_seconds()

def get_remining_rows( start, df_data):
    rows = []
    for idx in range (start,len(df_data)):
        ts_idx =  date_2_unix_sec(df_data.iloc[idx].timestamp)  
        dt_idx = datetime.datetime.utcfromtimestamp(ts_idx).strftime('%Y-%m-%d %H:%M:%S.%f')
    
        rows.append({'ap_id':df_data.iloc[idx].ap_id,
                         'latitude':df_data.iloc[idx].latitude,
                         'longitude':df_data.iloc[idx].longitude,
                         'timestamp':dt_idx
                        })
        
    return rows  
''' 

def convert_gpx2csv(ap_id, input_file_name):

	
	gpx_input = open(input_file_name, "r")
	gpx_string = gpx_input.read()
	gpx_input.close()		

	mydoc = minidom.parseString(gpx_string)

	row_list = []

	trkpt = mydoc.getElementsByTagName("trkpt")
	for row in trkpt:

		lat = row.getAttribute("lat")		
		lon = row.getAttribute("lon")
		
		ts_val = ''
		if row.getElementsByTagName("time"):
			ts = row.getElementsByTagName("time")[0]
			ts_val = ts.firstChild.data 

		row_data = { 'latitude': lat , 'longitude': lon , 'timestamp': ts_val}
	

		row_list.append(row_data)	

	df_csv = pd.DataFrame(row_list, columns = [ 'latitude', 'longitude','timestamp'])
	
	if 'ap_id' not in df_csv.columns:
		df_csv.insert(0,'ap_id',ap_id) 
	
	if 'id' not in df_csv.columns:
		df_csv.insert(0,'id',range(len(df_csv)))# add a new id column
		

	df_csv['timestamp'] = df_csv.timestamp.fillna(False)
 
	df_csv = create_geometry(df_csv)
	 
	
	#df_csv.to_csv(input_file_name+'.csv',index=False)
	# save csv of gpx file given by graphHopper	
	#print ('grapHopper generated route ',input_file_name+'.csv')
	
	return df_csv	


def create_geometry(df, lon='longitude',lat='latitude'):

	df[lon] = df[lon].astype(float)
	df[lat] = df[lat].astype(float)

	geometry = [Point(xy) for xy in zip(df[lon], df[lat])]
	# Coordinate reference system : WGS84
	crs = {'init': 'epsg:4326'}
	
	gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry) 
	 
	return gdf
       
	
'''
def update_missing_ts(df,ts0, i,cnt,time_delta):
	start =  i - cnt
	
	for j in range(start,i):
		
		final_ts = ts0 + time_delta * (1+j-start) 
		
		df.iloc[j, df.columns.get_loc('timestamp')] = final_ts.strftime("%Y-%d-%m %H:%M:%S.%f")
  
'''



def get_next_index(id_done,list_ids):
    list_ids.sort()
    for i in list_ids:
        if i>=id_done:
            return i
    return -1	
	

 
def read_reference_ts_data(reference_file ):
	
	df_sp = pd.read_csv(reference_file) # original reference file
 
	df_sp['timestamp'] = pd.to_datetime(df_sp['timestamp']) 
	df_sp.sort_values(by=['timestamp'],inplace=True) 
 
	df_sp = create_geometry(df_sp)
 
	if 'id' not in df_sp.columns:
		df_sp.insert(0,'id',range(len(df_sp)))# add a new id column
		
	return df_sp

	

# check next row_id from 10% of the rows in the df_route  	
def get_nearest_row_id(id_done,pt_ref, df_route):
	
	if (id_done <0 ):
		id_done = 0
	id_until = int(round( id_done + ( len(df_route) *0.1 ) 	) )
	id_until = len(df_route) if id_until > len(df_route) else id_until

		
	df_route_slice = df_route[id_done:id_until]

	
	pts3 = df_route_slice.geometry.unary_union
	
	nearest =  df_route_slice.geometry == nearest_points(pt_ref, pts3)[1] 
	
	list_ids = df_route_slice[nearest].id.values
	
	list_ids.sort()
	for i in list_ids:
		if i>=id_done:
			return i
		
	return -1	
	
	
	
def map_timestamp_to_route(df_route,df_ref) :

    id_done = -1
    
    df_route['timestamp'] = ''
    
    #pts3 = df_route.geometry.unary_union
    df_route.at[0,'timestamp'] = df_ref.iloc[0].timestamp  # first
    
    for i, row in df_ref.iterrows():
  
        idx = get_nearest_row_id(id_done,row.geometry, df_route)
        #nearest =  df_route.geometry == nearest_points(row.geometry, pts3)[1] 
			
        #list_ids = df_route[nearest].id.values
        #idx = get_next_index(id_done,list_ids, df_route)

        
        if i == 0 : # ignore backward points 
            continue
        
        if  idx > -1: 
            id_done = idx
            df_route.at[idx,'timestamp'] = df_ref.iloc[i].timestamp   
        
        else: # ignore backward points 
            continue
    	
 
    df_route = df_route.sort_values(by=['id'], ascending=True)
    return df_route[['id','ap_id','latitude','longitude','timestamp']]




def map_timestamp_to_staypoints( df_t, df_sp):
	
	ts_done = [] 
	df_final = df_t.copy()
	#df_final['timestamp'] = df_final.timestamp.fillna(False)
	df_final['timestamp'] = ''
	
	df_final.sort_values(by=['id'],inplace=True)	

	for i in range(len(df_final)):
		#print i
		i_pt = df_final.iloc[i].geometry
		df_within = df_sp.loc[df_sp.geometry.within(i_pt.buffer(0.001))] # within 1 m distance 
		
		if len(df_within):
			
			ts = df_within.iloc[0].timestamp
			if ts not in ts_done:
				df_final.at[i,'timestamp'] = ts
				ts_done.append(ts)
			
	df_final.sort_values(by=['id'],inplace=True)
	
	return df_final [['id','ap_id','latitude','longitude','timestamp']]

'''
def copy_reference_data_to_routes(df_ref,df_route):
    arr_rows = []
    rows = []
    i=0
    j=0

    len_ref = len(df_ref)
    len_route = len(df_route)
    total_rows = len_route + len_ref
    
    #sec = date_2_unix_sec(df.iloc[1].timestamp)  
    #datetime.datetime.utcfromtimestamp(sec).strftime('%Y-%m-%d %H:%M:%S.%f')

    for cnt in range (total_rows):

        ts_i =  date_2_unix_sec(df_ref.iloc[i].timestamp)
        ts_j =  date_2_unix_sec(df_route.iloc[j].timestamp)  


        dt_i = datetime.datetime.utcfromtimestamp(ts_i).strftime('%Y-%m-%d %H:%M:%S.%f')
        dt_j = datetime.datetime.utcfromtimestamp(ts_j).strftime('%Y-%m-%d %H:%M:%S.%f')


        if ts_i < ts_j:
            arr_rows.append({'ap_id':df_ref.iloc[i].ap_id,
                             'latitude':df_ref.iloc[i].latitude,
                             'longitude':df_ref.iloc[i].longitude,
                             'timestamp':dt_i
                            })
            i += 1
            
        else:

            arr_rows.append({'ap_id':df_route.iloc[j].ap_id,
                             'latitude':df_route.iloc[j].latitude,
                             'longitude':df_route.iloc[j].longitude,
                             'timestamp':dt_j
                            })
            j += 1


        if (i == len_ref):
            #print ('copy remaining J')
            rows = get_remining_rows( j,  df_route)
            break

        if (j == len_route):
            #print( 'copy remaining I' )
            rows = get_remining_rows( i,  df_ref)
            break

    arr_rows = arr_rows +rows

    df_all = pd.DataFrame(arr_rows)
    return df_all
'''


'''
def populate_timestamp(df):
		
	df = df.sort_values(by=['id'], ascending=True)
	ts0 = df.iloc[0].timestamp # assumes that the first item contains a valid timestamp
	
	#ts0 = datetime.datetime.strptime(df.iloc[0].timestamp, '%Y-%m-%d %H:%M:%S')
	cnt = 0
	ts=None

	for i in range(len(df)):
	
		try:
			ts = df.iloc[i].timestamp
		except:
			ts = datetime.datetime.strptime(df.iloc[i].timestamp, '%Y-%m-%d %H:%M:%S')

		
		if ts=='':
			cnt += 1
		
		else:
			time_delta = (ts - ts0)/(cnt+1)
			update_missing_ts(df,ts0, i,cnt,time_delta)    
			cnt = 0 
			ts0 = ts 
			
	df = df.replace('', np.NaN) # replace empty cell with null values
	df = df.dropna(subset=['timestamp']) # replace all rows with null values ( some timestamp cell are not populated -> remove those rows)
	 
	return df

'''


def convert_resgpx2csv(original_dir, gpx_dir, res_csv_op_dir):
	
	df_route_all = pd.DataFrame()
	
	print('\nconvert_resgpx2csv ->',res_csv_op_dir)
	arr_done = get_ap_id_done(res_csv_op_dir)

	print ('Matched gpx -> ' + str(gpx_dir) +'/*.res.gpx')
	
	res_gpx_files = glob.glob(os.path.join(gpx_dir, '*.res.gpx'))
	
	#print(res_gpx_files)
	
	for gpx_file_name in res_gpx_files:
		
		
		
		dir_name, file_name = os.path.split(gpx_file_name)
		ap_id = file_name.split('.')[0]
		
		#write2file("gpx2csv(),START, " +str(ap_id) +','  +  str(datetime.datetime.now() ) , pathlib.Path(OUTPUT_DIR, 'gpx2csv.txt') )
			
		output_file_name_only =   ap_id + '_res.csv'
		output_file_name = pathlib.Path(res_csv_op_dir,output_file_name_only)
		
		original_file_name_only  =  ap_id +'.csv'
		original_probe_file =  pathlib.Path(original_dir , original_file_name_only) # for reading timestamp
		
		#print (' completed for '+ ap_id )
		
		if ap_id in arr_done:
			continue
			
			
		#if ap_id not in arr_ts_error:			
		#	continue
			
		df_ref = read_reference_ts_data(original_probe_file)
		if (len(df_ref) < min_gps_points_allowed):
			#print ('<<<'+ap_id+'>>> input pts<5')
			continue
		
	 
		df_route = pd.DataFrame()
		try:
			df_route = convert_gpx2csv(ap_id, gpx_file_name) 
		except:
			#print ('<<<'+ap_id+'>>> XML error')			
			continue
			
		if (len(df_route) < min_gps_points_allowed):
			#print ('<<<'+ap_id+'>>> gpx pts<5')
			continue
				

			 
		df_route = map_timestamp_to_route(df_route,df_ref)
		ap_res_csv = pathlib.Path(RES_CSV_GH_OP_DIR, ap_id + '_GH_OP.csv')
		#ap_res_csv =  ap_id+'_raw_GH.csv'
		df_route.to_csv(ap_res_csv,index=False)#########
		#print("saved+++++++++++++++++",ap_res_csv)
		
		
		try:
			df_route = populate_ts_new(ap_res_csv)
		except:
			print ('<<<'+ap_id+'>>> timestamp problem')
			continue
		
		# copy all data from input reference file to final routes
		#df_route = copy_reference_data_to_routes(df_ref, df_route)

		df_route.to_csv(output_file_name , index=False)
		#print ("Completed:####### ", output_file_name )
		df_route_all = df_route_all.append(df_route)
	
	
	return df_route_all
		


def populate_ts_new(csv_file):
	df_all = pd.read_csv(csv_file)
	df_all = df_all.sort_values(by=['timestamp'], ascending=True)
	df_ts = df_all[~df_all['timestamp'].isnull()] # remove all rows with timestamp null
	#df_ts['timestamp'] = pd.to_datetime(df_ts['timestamp']).dt.round('1s')
	
	arr_ids = df_ts.id.tolist()

	for i in range(len(arr_ids) - 1):
		
		ts1 = datetime.datetime.strptime(df_ts.iloc[i].timestamp, '%Y-%m-%d %H:%M:%S')
		ts2 = datetime.datetime.strptime(df_ts.iloc[i+1].timestamp, '%Y-%m-%d %H:%M:%S')
		#ts1=df_ts.iloc[i].timestamp
		#ts2=df_ts.iloc[i+1].timestamp
		id1 = df_ts.iloc[i].id
		id2 = df_ts.iloc[i+1].id
		ts_delta = (ts2-ts1)*1.0/(id2-id1)
		
		for j in range(id2-id1):
			ts = ts1 + j* ts_delta
			df_all.at[id1+j,'timestamp']=ts
			
	df_all = df_all.replace('', np.NaN) # replace empty cell with null values
	df_all = df_all.dropna(subset=['timestamp']) # replace all rows with null values ( some timestamp cell are not populated -> remove those rows)
	
	# add distance travelled in each step
	#df_all = add_distance_col(df_all)
	 
	return df_all
'''
#from geopy.distance import distance
def add_distance_col(df_all):
	df_all['dist'] = 0
	for i in range(len(df_all)-1):
		lat1 = df_all.iloc[i].latitude
		lat2 = df_all.iloc[i+1].latitude
		lon1 = df_all.iloc[i].longitude
		lon2 = df_all.iloc[i+1].longitude
		dist = distance((lat1,lon1), (lat2,lon2)).m
		df_all.at[i+1,'dist']=dist
	return df_all
'''	
	
		
def get_ap_id_done(path):
				 
	arr_ap_done =[]
	#csv_dir = pathlib.Path('/home/bidur/map_match_gps_data/input/csv')
	csv_files = glob.glob(os.path.join(path, '*.csv'))
	for f in csv_files:
		dir_name, file_name = os.path.split(f)
		
		arr_ap_done.append((file_name.split('_res')[0]))
	  
	#print (arr_ap_done)
	return arr_ap_done
	
	
'''	
import pathlib

ROOT_DIR 		= '/home/bidur/map_match_gps_data/' #'C:/Users/epinurse/Desktop/PHL_Mobility_data/map_match_gps_data-main/'

INPUT_DIR 		= pathlib.Path(ROOT_DIR, 'input')
OUTPUT_DIR 		= pathlib.Path(ROOT_DIR, 'output')

input_file = pathlib.Path(INPUT_DIR, '1_input.csv')  
input_anonymized = pathlib.Path(INPUT_DIR, '2_anonymized_input.csv') 
input_anonymized_clipped = pathlib.Path(INPUT_DIR, '3_anonymized_clipped.csv') 
input_preprocessed = pathlib.Path(INPUT_DIR, '4_preprocessed.csv')  # preprocessed sampled daa

######## map-matching

MAP_MATCHING_PATH 	= pathlib.Path(ROOT_DIR, 'map-matching-master')
GPX_DIR 			= pathlib.Path(MAP_MATCHING_PATH , 'matching-web/src/test/resources/target/')
CSV_DIR 			= pathlib.Path(INPUT_DIR, 'csv')
RES_CSV_DIR 		= pathlib.Path(OUTPUT_DIR ,'res_csv') # resultant of mapmatching
CACHE_LOC_DIR 		= pathlib.Path(MAP_MATCHING_PATH , 'graph-cache')


def main():
	
	original_csv_dir = 'input/csv/'
	res_gpx =  'output/res_gpx/'
	res_csv_op_dir = 'output/res_csv/'
	
	#remove_dir(res_csv_op_dir)
	#check_dir(res_csv_op_dir)

	df_mapped_route = convert_resgpx2csv(CSV_DIR, GPX_DIR, RES_CSV_DIR) # Convert road mapped GPS Probe to CSV and update timestamp
	
	print (df_mapped_route)
	


main()

'''
