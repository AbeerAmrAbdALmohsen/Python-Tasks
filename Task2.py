import time
start = time.time()


checksums = {}
duplicates = []

import argparse

parser = argparse.ArgumentParser()

parser.add_argument("path", help = "Enter Path of the directory")
parser.add_argument("-u", action="store_true", dest="unix", default=False)

args = parser.parse_args()

from subprocess import PIPE, Popen

from os import listdir
# Get all files in the current directory
from os.path import isfile, join
files = [item for item in listdir(args.path) if isfile(join(args.path, item ))]

# Iterate over the list of files filenames
for filename in files:
    filename=args.path+"/"+filename
    # Use Popen to call the md5sum utility
    with Popen(["md5sum", filename], stdout=PIPE) as proc:
        checksum= proc.stdout.read().split()[0]
        
        # Append duplicate to a list if the checksum is found
        if checksum in checksums:
            duplicates.append(filename)
        checksums[checksum] = filename

print(f"Found Duplicates: {duplicates}")

import json
import pandas as pd 
from pandas.io.json import json_normalize 

for filename in files:
    finalname_Copy=filename 
    filename=args.path+"/"+filename
    if filename in duplicates:
        print("------------------------------------")
        print(finalname_Copy+" is dplicated")
        print("------------------------------------")
    else:  
        print("------------------------------------")
        print(finalname_Copy)
        records = [json.loads(line) for line in open(filename)]
        print("------------------------------------")

        df = json_normalize(records)
        newdDF = df[['a', 'c','nk','tz','gr','g','h','l','al','hh','r','u','t','hc','cy','ll']].copy()

        arr1 = newdDF['a'].str.split(" ", n = 2, expand = True)  
        arr2 = arr1[0].str.split("/", n = 1, expand = True) 
        newdDF['web_browser'] = arr2[0] 
        newdDF['operating_sys'] = arr1[1].str[1:]
        arr3 = newdDF['r'].str.split("/", n = 5, expand = True) 
        newdDF['from_url'] = arr3[2]
        arr4 = newdDF['u'].str.split("/", n = 5, expand = True) 
        newdDF['to_url'] = arr4[2] 
        newdDF['city'] = newdDF['cy']
        newdDF['longitude'] = newdDF['ll'].str[0]
        newdDF['latitude'] = newdDF['ll'].str[1]
        newdDF['time_zone'] = newdDF['tz']
        newdDF = newdDF.dropna()

        if args.unix:
            newdDF['time_in'] = newdDF['t']
            newdDF['time_out'] = newdDF['hc']

        else:
            time_in_timestamp = []
            for i, row in newdDF.iterrows():
                time_in = pd.to_datetime(row['t'], unit = 's').tz_localize(row['time_zone']).tz_convert('UTC')
                time_in_timestamp.append(time_in)
            newdDF['time_in'] = time_in_timestamp

            time_out_timestamp = []
            for i, row in newdDF.iterrows():
                time_out = pd.to_datetime(row['hc'], unit = 's').tz_localize(row['time_zone']).tz_convert('UTC')
                time_out_timestamp.append(time_out)
            newdDF['time_out'] = time_out_timestamp

        newdDF.drop(columns=['a', 'c','nk','tz','gr','g','h','l','al','hh','r','u','t','hc','cy','ll'], inplace=True)
    
        No_of_rows_transformed=newdDF.shape[0]
        File_Target_Path="/home/abeer/Desktop/Task/target/"+finalname_Copy+".csv"

        print("Number Of Rows Transformed: ",No_of_rows_transformed)
        print("Path Of Target File: ",File_Target_Path)

        newdDF.to_csv(File_Target_Path, index = False)

    
print('The Script took', time.time()-start, 'seconds.')


