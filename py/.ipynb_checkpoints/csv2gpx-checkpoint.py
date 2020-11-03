import sys,os
from datetime import datetime
import pandas as pd
import glob, shutil
from functions import remove_dir,check_dir
import pathlib
# "ap_id","timestamp","latitude","longitude" s.strip('""')


def process_line(line):
    #print(line)
    def strip(s):
        return s.strip()
    name, timestamp, latitude, longitude = \
        map(strip, line.split(','))
    timestamp = datetime.strptime(timestamp.strip('"'),  '%Y-%m-%d %H:%M:%S')

    return dict(
        name=name,
        time=timestamp,
        latitude=latitude,
        longitude=longitude
        
    )


def gpx_header():
    return """\
<?xml version="1.0"?>
<gpx
 version="1.0"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xmlns="http://www.topografix.com/GPX/1/0"
 xsi:schemaLocation="http://www.topografix.com/GPX/1/0 http://www.topografix.com/GPX/1/0/gpx.xsd">
  <trk>
    <trkseg>
"""


def gpx_footer():
    return """\
    </trkseg>
  </trk>
</gpx>
"""


def gpx_datapoint(data):
    foo = data.copy()
    foo['time'] = data['time'].isoformat()
    return """\
<trkpt lat="{latitude}" lon="{longitude}">
  <name>{name}</name>
  <time>{time}</time>
</trkpt>""".format(**foo)


def output_xml(datapoints, output_file):
    print_line = gpx_header() 
    for datum in datapoints:
        print_line +=  gpx_datapoint(datum) 
    print_line +=  gpx_footer()  
    
    #print ("complete")
    f = open(output_file, "w")
    f.write(print_line)
    f.close()


def convert_csv2gpx(csv_dir, gpx_dir):
	csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))
	for fname in csv_files:
		#print (fname)
		head, tail = os.path.split(fname)
		tail_gpx= tail +'.gpx'
		output_file = pathlib.Path(gpx_dir ,tail_gpx)
		with open(fname, 'r') as f:
			lines = f.readlines()
			
		datapoints = map(process_line, lines[1:])
		output_xml(datapoints, output_file)
	
	return None
	
'''
def check_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
        
def remove_dir(path):
	if  os.path.exists(path):
		 #os.system("rm -rf "+path)
		 shutil.rmtree(path)
'''	
def prepare_csv_files(df,csv_dir):

	arr_ap_ids = df.ap_id.unique()
	
	cnt = 0
	arr_id_with_few_data = 0
	for ap_id in arr_ap_ids:
		df_single_ap = df.query('ap_id=="'+ap_id+'"')
		
		if len(df_single_ap) < 10:# skip ap_id with less than 10 data points
			arr_id_with_few_data += 1
			continue
		
		cnt += 1
		df_single_ap = df_single_ap.sort_values('timestamp')
		csv_file = str(ap_id)+'.csv'
		#print('***sorted input: ', csv_file)
		df_single_ap.to_csv(pathlib.Path(csv_dir,csv_file),index=False)
		
	if cnt == 0:
		print("Route Points cannot be generated as all the selected ap_id's have very few input data points")
		print ("\n\nPLEASE PROVIDE NEW INPUT WITH SUFFICIENT DATA \n\n")
		sys.exit("Very Few Data")
		
	elif arr_id_with_few_data>0:
		print('Very Few Data (<10 points) in << ',str(arr_id_with_few_data) , " >> ap_ids")
		
	print (cnt ,' csv file prepared and saved in ', csv_dir)
	
	return None
'''		
	
import glob
path = "/media/bidur/349875C714C5228F/Miyazaki_research/mobilityProject/staypointAnalysis/ap520057/input/csv/*.csv"
for fname in glob.glob(path):
    print(fname)	
'''
'''
if __name__ == "__main__":
    
    #input_file= 'AP520057.csv'
    input_file = '/home/bidur/map_match_gps_data/input/4_preprocessed.csv'
    
    csv_dir = '../input/csv/'
    #gpx_dir = 'input/data4gh/'
    remove_dir(csv_dir)
    remove_dir(gpx_dir)
    check_dir(csv_dir)
    check_dir(gpx_dir)
    
    df = pd.read_csv(input_file)
    df['ap_id'] = df['ap_id'].apply(str) # ap_id are assumed to be string
    
    prepare_csv_files(df, csv_dir)
    
    convert_csv2gpx(csv_dir,gpx_dir)

'''
#https://github.com/des4maisons/csv-to-gpx/blob/master/csv-to-gpx.py
