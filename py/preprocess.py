import random 
import pandas as pd
import shutil 

from geopy.distance import distance
from datetime import datetime

from config import  sampling_percent
from config import  input_anonymized_clipped, input_preprocessed, max_dist_per_hour_threshold

def get_timestamp_from_str(ts_str):	
	
	if '.' in ts_str: # microsecond present
		ts_str = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S.%f') 
		
	elif 'Z' in ts_str:# 2019-07-01T12:37:25Z

		ts_str = datetime.strptime(ts_str, '%Y-%m-%dT%H:%M:%SZ') 
		
	else:
		ts_str = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S') 
		
	return ts_str

def apply_sampling(sampling_percent, df_all):
	
	# lst = lst[:len(lst)-n]
	
	arr_ap_id = df_all.ap_id.unique() # duplicate ap_id
	#print (  len(arr_ap_id))
		
	id_count_to_keep = int(len(arr_ap_id) * sampling_percent * 0.01 ) 
	#arr_ap_id = arr_ap_id[:id_count_to_keep]
	if  id_count_to_keep == 0:
		id_count_to_keep = 1
		
	
	arr_ap_id_sampled = random.sample(list(arr_ap_id), id_count_to_keep)#  to randomly select samples
	#print (  len(arr_ap_id_sampled))
	
	df_sample = df_all[df_all.ap_id.isin(arr_ap_id_sampled)]
	#df[df.ap_id.isin(arr_ap_id)]
	return df_sample

def calc_distance(df, ap_id):
    large_jump = False
    df_temp = df.copy()
    #display(df_temp) 
    df_temp = df_temp.sort_values(by=['timestamp'], ascending=True)
    df_temp = df_temp.reset_index(drop=True)
    #display(df_temp)
    
    #print('len: ',len(df_temp))
    for idx, row in df_temp.iterrows():
        
        if (idx+1 < len(df_temp)):
            #print('idx', idx)
            lat1 = df_temp.iloc[idx].latitude
            lon1 = df_temp.iloc[idx].longitude
            lat2 = df_temp.iloc[idx+1].latitude
            lon2 = df_temp.iloc[idx+1].longitude
            dist_covered = distance((lat1,lon1), (lat2,lon2)).km
            
            ts1 = df_temp.iloc[idx].timestamp
            ts2 = df_temp.iloc[idx+1].timestamp
            ts1 = get_timestamp_from_str(ts1)
            ts2 = get_timestamp_from_str(ts2)
            time_diff_sec = (ts2-ts1).total_seconds() + 1 # 1 added to avoid divide by zero case below
            dist_covered_in_hour = (dist_covered * 3600)/time_diff_sec

            #if dist_covered>10:# 10Km or more
            if max_dist_per_hour_threshold < dist_covered_in_hour:
                print(ap_id, 'JUMP >', ' threshold(km/hr):', max_dist_per_hour_threshold ,  'time taken(hr):', time_diff_sec/3600, ' dist_covered:', dist_covered)
                #large_jump = True
                return True
    
    #print("no jump FOR: " , ap_id)
                
    return large_jump

def filter_by_distance(df):# remove ap_ids with jump
    arr_ap_id = df.ap_id.unique() # duplicate ap_id
    #print ('b4: ',df.ap_id.unique())
    for ap_id in arr_ap_id:
        #print("____processing", ap_id)
        df_ap_id = df.query('ap_id == "'+str(ap_id)+'"')
        large_jump = calc_distance(df_ap_id, ap_id)
        if large_jump:
            df = df[df['ap_id']!=ap_id]# remove all entries for this ap_id 
            #print ("JUMP ", ap_id)
    arr_ap_id_after = df.ap_id.unique() # duplicate ap_id
    #print ('b4: ',df.ap_id.unique())
    print('ap_id (all):' ,len(arr_ap_id), ' ap_id (No JUMPS):', len(arr_ap_id_after))
    return df
    
    
    
    
def preprocess_data():
	
	df = pd.read_csv(input_anonymized_clipped)# 4900 rows
	df['ap_id'] = df['ap_id'].apply(str) # ap_id are assumed to be string	
	
    # filter out ap_ids with unrealistic JUMPs in distance
	df = filter_by_distance(df) 

	#remove duplicate rows
	df.drop_duplicates(inplace=True) # 48782rows

	# get duplicate rows in terms of 'ap_id' and 'timestamp'
	df_dup = df[df.duplicated(['ap_id','timestamp'],keep=False)] # 294 rows # 60 ap_id
	df_dup = df_dup.sort_values(by=['ap_id'], ascending=True)

	# remove all duplicates from df -> new values will be calculated and kept for the duplicates
	df_clean = df.drop_duplicates(['ap_id','timestamp'], keep=False)
	
    
	# Handle case: same timestamp and differnt location (e.g. L1 and L2 have same timestamp t1) -> remove these values and keep a single location ( avg(l1,l3), t1)
	arr_clean_rows = []
	arr_ap_id = df_dup.ap_id.unique() # duplicate ap_id

	# process each ap_id
	for ap_id in arr_ap_id:
		#ap_id = 'AP521696' #arr_ap_id[0]
		df_ap_id = df_dup.query('ap_id == "'+str(ap_id)+'"')

		# get duplicate ts for the ap_id
		arr_ts =  df_ap_id.timestamp.unique()
		
		# process each ts
		for ts in arr_ts:
			#ts = arr_ts[0]
			df_ts = df_ap_id[df_ap_id['timestamp'].isin([ts])]
			new_lat = df_ts['latitude'].mean()
			new_lon = df_ts['longitude'].mean()
			arr_clean_rows.append({'ap_id': ap_id,  'timestamp':ts, 'latitude': new_lat, 'longitude':new_lon})

		
	# add new row to clean df
	if len(arr_clean_rows):
		df_clean = df_clean.append(arr_clean_rows)

	
	# APPLY sampling_percent
	df_sample = apply_sampling(sampling_percent, df_clean)
	df_sample.to_csv(input_preprocessed, index=False)
	return df_sample



