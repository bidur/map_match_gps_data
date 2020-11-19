from geopy.distance import distance
#from datetime import datetime
import pandas as pd
import glob, os,shutil,pathlib
from datetime import timedelta, date
from datetime import datetime,timedelta, date

from config import max_dist_per_hour_threshold, LOG_DIR




def get_all_files_from_dir(csv_dir, file_type='*.csv'):
    #os.chdir(csv_dir)
    arr_csv=[]
    for file in glob.glob(csv_dir+file_type):
        arr_csv.append(file)
    return arr_csv

def remove_dir(path):
    if  os.path.exists(path):
         #os.system("rm -rf "+path)
         shutil.rmtree(path)
    return

def check_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)    
 

def get_selected_files(arr_raw_files , arr_target_dates, selected_fields_dir_tmp):
    
    check_dir(selected_fields_dir_tmp)# create a folder to save intermediate files
    print("selected Input Files: ")
    for raw_fl in arr_raw_files:
        for target_dt in arr_target_dates:
            
            if target_dt in raw_fl:
                #print (raw_fl)
                csv_path, csv_name = os.path.split(raw_fl)
                target_file = selected_fields_dir_tmp + csv_name
                # Interesting columns in the csv are 1,2,3,4,6,7,8,9 so select them
                copy_cmd = "cut -d ',' -f 1,2,3,4,6,7,8,9 "+ raw_fl +" >  " + target_file
                os.system(copy_cmd)  
                print (csv_name)
    return 
        
        
def daterange(st_date, end_date):
    for cnt in range(int ((end_date - st_date).days)+1):
        yield st_date + timedelta(cnt)

def get_date_list(start_date,end_date):
    arr_target_dates = []
    for dt in daterange(start_date, end_date):
        arr_target_dates.append(dt.strftime("%Y%m%d"))  
    
    return arr_target_dates

    


def calc_distance(df, ap_id, fp_log):
    large_jump = False
    df_temp = df.copy()
    #display(df_temp) 
    #df_temp = df_temp.reset_index(drop=True)
    
    df_temp = df_temp.sort_values(by=['timestamp'], ascending=True)

    Zero_time_cnt = 0
    nonZero_time_cnt = 0
    jump_cnt  = 0

    for idx, row in df_temp.iterrows():
        
        
        if (idx+1 < len(df_temp)):
            #print('idx', idx)
            lat1 = df_temp.iloc[idx].latitude
            lon1 = df_temp.iloc[idx].longitude
            lat2 = df_temp.iloc[idx+1].latitude
            lon2 = df_temp.iloc[idx+1].longitude
            dist_covered_km = distance((lat1,lon1), (lat2,lon2)).km
            
            ts1 = df_temp.iloc[idx].timestamp
            ts2 = df_temp.iloc[idx+1].timestamp
            #ts1 = get_timestamp_from_str(ts1)
            #ts2 = get_timestamp_from_str(ts2)
            time_diff_sec = (ts2-ts1).total_seconds() 
            dist_covered_in_hour = 0 # init
            
            if time_diff_sec > 0:
                dist_covered_in_hour = (dist_covered_km * 3600)/ (time_diff_sec)
                nonZero_time_cnt += 1
            
            else:
                dist_covered_in_hour = 9999999 # infinite
                Zero_time_cnt += 1
             

            if max_dist_per_hour_threshold <  dist_covered_in_hour:
                large_jump = True
                jump_cnt += 1
                #return True
            
            str_line = str(ap_id) +','+ str(idx) +','+ str(lat1) +','+ str(lon1) +','+ str(lat2) +','+ str(lon2) +','
            str_line += str( ts1) +','+ str(ts2) +','+ str(time_diff_sec ) +','+ str(dist_covered_km)+','+ str(dist_covered_in_hour) +','+ str(max_dist_per_hour_threshold)
            fp_log.write(str_line +"\n")       
        
    #fp_log2.write(str(ap_id)+','+str(len(df_temp))+ ','+ str(Zero_time_cnt)+','+str(jump_cnt)   +"\n")

    return large_jump


def filter_by_distance(df, log_file):# remove ap_ids with jump
    print ("log_file:", log_file)
    arr_ap_id = df.ap_id.unique()  

    fp_log = open(log_file, 'a')
    #fp_log2 = open(log_file.replace('.csv','_2.csv'), 'a')    
    #fp_log2.write( str(datetime.datetime.now())  ) 
    #fp_log2.write("ap_id,data_cnt,Zero_time_cnt,jump_cnt"+"\n")
    fp_log.write(str( datetime.now() ) )
    fp_log.write("ap_id,idx,lat1,lat2,lon1,lon2,time1,time2,time_diff(s),dist(km),dist(km/hr),max_km/hr"+"\n")
    

    for ap_id in arr_ap_id:
        #print("____processing", ap_id)
        #ap_id = 1157
        df_ap_id = df.query('ap_id == "'+str(ap_id)+'"')
        large_jump = calc_distance(df_ap_id, ap_id, fp_log)
        if large_jump:
            df = df[df['ap_id']!=ap_id]# remove all entries for this ap_id 
            #print ("JUMP ", ap_id)  
        #break
  
    fp_log.close()
    #fp_log2.close()
    
    arr_ap_id_after = df.ap_id.unique() # duplicate ap_id

    print('ap_id (all):' ,len(arr_ap_id), ' ap_id (No JUMPS):', len(arr_ap_id_after))
    
    return df
    

    
def aggregate_dupliacte_ts(df_ap):
    arr_new_rows = []
    arr_ts = df_ap.timestamp.unique()
    for ts in arr_ts:
           
        df_ts=df_ap.query('timestamp=="'+str(ts)+'"') #get values for particular ap_id
        new_lat = df_ts['latitude'].mean()
        new_lon = df_ts['longitude'].mean()
        ap_id = df_ap['ap_id'].iloc[0]
        
        arr_new_rows.append({'ap_id': ap_id,  'timestamp':ts, 'latitude': new_lat, 'longitude':new_lon})
    return arr_new_rows

def clean_by_aggregating_ts(df):
    
    df_new = pd.DataFrame()
    arr_ap_id = df.ap_id.unique() # get all ap_ids
    for ap_id in arr_ap_id:
        
        df_ap=df.query('ap_id=="'+str(ap_id)+'"') #get values for particular ap_id
        df_ap = df_ap.sort_values(by=['timestamp']) # sort by timestamp

        
        arr_new_rows = aggregate_dupliacte_ts(df_ap)
        if len(arr_new_rows):
            df_new = df_new.append(arr_new_rows)
        
    return df_new


def prepare_target_csv(raw_csv_dir, mapmatching_input_dir):
    
    print("prepare_target_csv()")

    arr_csv = get_all_files_from_dir(raw_csv_dir)
    
    #print("\n The following files are prepared: ")
    log_file0 = pathlib.Path(LOG_DIR, '0_input_summary.csv' )
    fp_log0 = open(log_file0, 'a')
    fp_log0.write("Time,csvInput,originalAP_IDs,TimestampAggregatedAP_IDs,AP_IDsAfterJumpRemoved,originalRows,TimestampAggregatedRows,RowsAfterJumpRemoved"+"\n")
    fp_log0.close()

    for csv in arr_csv:
 
        csv_path, csv_name = os.path.split(csv)
        print("\t\t Start: ", csv_name, str( datetime.now() ))
        
        log_file = pathlib.Path(LOG_DIR, '1_'+csv_name)
        
        df = pd.read_csv(csv)

        df_sel_cols=pd.DataFrame()
        
        df_sel_cols['ap_id'] = pd.factorize(df.dailyid)[0] +200 # convert ap_id from string to integer and start from 200

        df_sel_cols['timestamp'] = df['year'].map(str) + '-' + df['month'].map(str) + '-' + df['day'].map(str)+ ' ' + df['hour'].map(str)+':' + df['minute'].map(str)+':30'
        df_sel_cols['latitude'] = df['latitude']
        df_sel_cols['longitude'] = df['longitude']
        df_sel_cols['dailyid'] = df['dailyid']
        
        df_sel_cols['timestamp'] = pd.to_datetime(df_sel_cols['timestamp'], format='%Y-%m-%d %H:%M:%S')
        
        #clean_csv = mapmatching_input_dir + csv_name
        #df_sel_cols.to_csv(clean_csv,index=False)
              
        print(csv, " clean_by_aggregating_ts()")
        df_ts_aggregated = clean_by_aggregating_ts(df_sel_cols)

        # filter out ap_ids with unrealistic JUMPs in distance
        print(csv, " filter_by_distance()")
        df_final = filter_by_distance(df_ts_aggregated, log_file) 
        
        #clean_csv = mapmatching_input_dir +'fn_'+ csv_name
        clean_csv = mapmatching_input_dir + csv_name
        df_final.to_csv(clean_csv,index=False)
        
        #print (csv, " original data count: ", len(df_sel_cols), " After ts aggregated: ", len(df_ts_aggregated), " After Jump FIlter: ", len(df_final))
        #print (csv, " original ap_id count: ", len(df_sel_cols.ap_id.unique()), " After ts aggregated: ", len(df_ts_aggregated.ap_id.unique()), " After Jump FIlter: ", len(df_final.ap_id.unique()))
        
        str_log0 =  str( datetime.now() ) +','+ csv_name   +','+ str(len(df_sel_cols.ap_id.unique()))      +','+  str(len(df_ts_aggregated.ap_id.unique()))      +','
        str_log0 += str(len(df_final.ap_id.unique()))    +','+    str(len(df_sel_cols))   +','+  str(len(df_ts_aggregated))  +','+ str(len(df_final))
        print(str_log0)
        fp_log0 = open(log_file0, 'a')
        fp_log0.write( str_log0   +"\n")
        fp_log0.close()
        
        
        print ('Handled TS and Jump : ', clean_csv ,  str( datetime.now() ) +"\n~~~~~~~~~~~~~~~~~~~~~")
    
    return


def get_raw_files_main(raw_dir, mapmatching_input_dir, start_date ,end_date):    

    remove_dir(mapmatching_input_dir) # remove old data if exist
    check_dir(mapmatching_input_dir)# create folder to keep final data
    check_dir(LOG_DIR)
    
    selected_fields_dir_tmp = mapmatching_input_dir + 'tmp/'  ## extract selected fields and save intermediate csv
    
    arr_target_dates = get_date_list(start_date,end_date)
    arr_raw_files = get_all_files_from_dir(raw_dir, file_type='*.csv')# get the list of raw input files
    
    #print(arr_raw_files)
    

    # select the files matching the given dates
    get_selected_files(arr_raw_files, arr_target_dates, selected_fields_dir_tmp) 
  
    prepare_target_csv( selected_fields_dir_tmp, mapmatching_input_dir )
   
    
    # remove intermediate files
    shutil.rmtree(selected_fields_dir_tmp)
    
    print("csv preparation for map-matching input complete")
    
    return 

'''
raw_dir = '/mnt/lv/heromiya/PHLMobilityData/'    ## original raw file location
mapmatching_input_dir = '/mnt/lv/bidur/PHL_raw_data/clean_input/a_20190209/' ## prepare csv in requireds format(ap_id, ) for map-matching
start_date = date(2019, 2, 9) # NPL_data_20190404.csv
end_date = date(2019, 2, 9)
    
get_raw_files_main(raw_dir, mapmatching_input_dir, start_date , end_date)    
'''