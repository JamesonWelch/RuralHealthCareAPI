import requests
import json
import pandas as pd
import io
import os
import sys
import datetime
import time
from sodapy import Socrata

ROOT_DIR = os.path.join(os.getcwd(), "../../..")
DATA_DIR = os.path.join(ROOT_DIR, "Data")
EXCEL_DIR = os.path.join(ROOT_DIR, "Excel_Files")
LOG_DIR = os.path.join(DATA_DIR, "Logs")



def main():
    with open("sources.txt", "r") as source_file:
        sources = []
        for line in source_file:
            sources.append(line)
    for i in range(len(sources)):
        api = sources[i].split("|")[0]
        record = sources[i].split("|")[1]
        try:
            dataFrame(api, record)
        except:
            with open("logs.log", "a") as log:
                log.write(f"{record} {api} failed to load at {now}")
                print(f"*** {record} {api} failed to load ***")


now = datetime.datetime.now()


def dataFrame(api_id, record):
    print("Importing dataframe from API...")
    
    res = requests.get(f'https://opendata.usac.org/resource/{api_id}.json?$select=count(*)')
    jsonRes = res.json()
    s1 = json.dumps(jsonRes)
    d2 = json.loads(s1)
    record_count = int(d2[0]['count'])
    
    fileName = str(now.strftime("%Y-%m-%d_%H-%M") + "_RHC_" + record + ".csv")
    filePath = os.path.join(os.getcwd(), fileName)
    
    call_range = int(record_count / 40000 + 1)
    offset = 0
    
    for i in range(call_range):
         
        client = Socrata("opendata.usac.org", None)
        results = client.get(api_id, limit=40000, offset=offset)
        
        results_df = pd.DataFrame.from_records(results)
        results_df.to_csv(filePath, sep="|", mode='a', header=False)
        
        if i == 0:
            print("Converting DataFrame into CSV")
        
        print(f'Offset #: {offset}')
        
        offset += 40000


main()
time.sleep(5)