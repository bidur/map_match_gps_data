'''
- csv files from  /mnt/lv/heromiya/PHLMobilityData/  contains many fields .
- Interesting fields are got via: 
	 cut -d ',' -f 1,2,3,4,6,7,8,9 PDP_philippines_20190704.csv >  PHL_20190704.csv
- Then the below program is used
'''

import pandas as pd
import glob, os,shutil

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
        
def prepare_csv(raw_csv_dir, clean_csv_dir):
    arr_csv = get_all_files_from_dir(raw_csv_dir)
    
    
    remove_dir(clean_csv_dir) # remove old data if exist
    os.makedirs(clean_csv_dir)# create folder to keep final data

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

clean_op_dir = 'clean_input/'
raw_csv_dir = 'raw_input/'
prepare_csv(raw_csv_dir, clean_op_dir)
