
import pandas as pd
import glob, os,shutil
from datetime import timedelta, date


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
    
    for raw_fl in arr_raw_files:
        for target_dt in arr_target_dates:
            
            if target_dt in raw_fl:
                #print (raw_fl)
                csv_path, csv_name = os.path.split(raw_fl)
                target_file = selected_fields_dir_tmp + csv_name
                # Interesting columns in the csv are 1,2,3,4,6,7,8,9 so select them
                copy_cmd = "cut -d ',' -f 1,2,3,4,6,7,8,9 "+ raw_fl +" >  " + target_file
                os.system(copy_cmd)  
    return 
        
        
def daterange(st_date, end_date):
    for cnt in range(int ((end_date - st_date).days)+1):
        yield st_date + timedelta(cnt)

def get_date_list(start_date,end_date):
    arr_target_dates = []
    for dt in daterange(start_date, end_date):
        arr_target_dates.append(dt.strftime("%Y%m%d"))  
    
    return arr_target_dates

    
def prepare_target_csv(raw_csv_dir, clean_csv_dir):

    arr_csv = get_all_files_from_dir(raw_csv_dir)
    
    print("\n The following files are prepared: ")

    for csv in arr_csv:
        csv_path, csv_name = os.path.split(csv)
        df = pd.read_csv(csv)

        df_final=pd.DataFrame()
        df_final['ap_id'] = df['dailyid']
        df_final['timestamp'] = df['year'].map(str) + '-' + df['month'].map(str) + '-' + df['day'].map(str)+ ' ' + df['hour'].map(str)+':' + df['minute'].map(str)+':30'
        df_final['latitude'] = df['latitude']
        df_final['longitude'] = df['longitude']
        clean_csv = clean_csv_dir +'clean_'+ csv_name
        df_final.to_csv(clean_csv,index=False)
        print(clean_csv)
        
    return


def get_raw_files_main(raw_dir, clean_csv_dir, start_date ,end_date):    

    remove_dir(clean_csv_dir) # remove old data if exist
    check_dir(clean_csv_dir)# create folder to keep final data
    
    selected_fields_dir_tmp = clean_csv_dir + 'tmp/'  ## extract selected fields and save intermediate csv
    
    arr_target_dates = get_date_list(start_date,end_date)
    arr_raw_files = get_all_files_from_dir(raw_dir, file_type='*.csv')

    get_selected_files(arr_raw_files, arr_target_dates, selected_fields_dir_tmp) 
  
    prepare_target_csv( selected_fields_dir_tmp, clean_csv_dir )
    
    # remove intermediate files
    shutil.rmtree(selected_fields_dir_tmp)
    
    print("csv preparation for map-matching input complete")
    
    return 

