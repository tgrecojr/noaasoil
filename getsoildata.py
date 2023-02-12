
import os, io
from os import environ
import requests
import sys
from datetime import datetime
from bs4 import BeautifulSoup
import fieldmappings
import numpy as np
import pandas as pd
import traceback
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

BASE_NOAA_LOCATION = 'https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/'

REQUIRED_ENV_VARS = {
    "INFLUX_TOKEN", 
    "INFLUX_ORG",
    "INFLUX_BUCKET",
    "INFLUX_URL",
    "INFLUX_BATCH_SIZE",
}

def formatdate(date,time):
    localdatetime = date + time
    date_time_obj = datetime.strptime(localdatetime, '%Y%m%d%H%M')
    return date_time_obj

def removevaluesnotrecorded(original_value):
    if (original_value == -99.000 or original_value == -9999.0 or original_value == -9999):
        return np.nan
    else:
        return original_value

def converttofarenheit(celsius_value):
    if celsius_value is not np.nan:
        return (celsius_value * 9/5) + 32
    else:
        return celsius_value
        
def processfile(datafile):
    print("Attempting to process file: {}".format(datafile))
    influx_client = InfluxDBClient(url=os.environ['SOIL_INFLUX_URL'], token=Sos.environ['OIL_INFLUX_TOKEN'], org=os.environ['SOIL_INFLUX_ORG'])
    influx_write_api = influx_client.write_api(write_options=SYNCHRONOUS,batch_size=int(os.environ['SOIL_INFLUX_BATCH_SIZE']), flush_interval=10_000, jitter_interval=2_000, retry_interval=5_000)

    df = pd.read_fwf(
        datafile,
        colspecs=fieldmappings.colspecs, 
        names=fieldmappings.field_names,
        header=None,
        index_col=False,
        dtype=fieldmappings.col_types,
        memory_map=False)
    df['UTC_DATETIME'] = df.apply(lambda row: formatdate(row['UTC_DATE'],row['UTC_TIME']), axis=1)
    df['LOCAL_DATETIME'] = df.apply(lambda row: formatdate(row['LST_DATE'],row['LST_TIME']), axis=1)
    df['SOIL_TEMP_5'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['SOIL_TEMP_5'])),axis=1)
    df['SOIL_TEMP_10'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['SOIL_TEMP_10'])),axis=1)
    df['SOIL_TEMP_20'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['SOIL_TEMP_20'])),axis=1)
    df['SOIL_TEMP_50'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['SOIL_TEMP_50'])),axis=1)
    df['SOIL_TEMP_100'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['SOIL_TEMP_100'])),axis=1)
    df['T_CALC'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['T_CALC'])),axis=1)
    df['T_HR_AVG'] = df.apply(lambda row: converttofarenheit(removevaluesnotrecorded(row['T_HR_AVG'])),axis=1)
    df['P_CALC'] = df.apply(lambda row: removevaluesnotrecorded(row['P_CALC']),axis=1)
    df['RH_HR_AVG'] = df.apply(lambda row: removevaluesnotrecorded(row['RH_HR_AVG']),axis=1)
    df = df.set_index(['UTC_DATETIME'])
    influx_write_api.write(os.environ['SOIL_INFLUX_BUCKET'], record=df,data_frame_measurement_name='soildata',data_frame_tag_columns=['WBANNO'])

def downloadandprocess():

    start_time = datetime.now()
    page = requests.get(BASE_NOAA_LOCATION)
    if page.status_code == 200:
        soup = BeautifulSoup(page.content, "html.parser")
        for a in soup.find_all('a', href=True):
            try:
                #NEED TO STRIP OFF LAST "/ FIRST"
                year = int(a['href'][:-1])
                newpage = BASE_NOAA_LOCATION + a['href']
                subpage = requests.get(BASE_NOAA_LOCATION + a['href'] )
                if subpage.status_code == 200:
                    subpagesoup = BeautifulSoup(subpage.content, "html.parser")
                    for b in subpagesoup.find_all('a', href=True):
                        if ".txt" in b['href']:
                            try:
                                processfile(newpage + b['href'])
                            except Exception as e:
                                traceback.print_exc()
                else:
                    print("Status code {} received when processing {}".format(subpage.status_code,newpage)) 
            except:
                pass
    else:
        print("Status code {} received when processing {}".format(page.status_code,BASE_NOAA_LOCATION))        

    end_time = datetime.now()
    time_taken = end_time - start_time
    print(f"Processed Soil Temps in {time_taken}")

def checkEnvironmentVariables():
    diff = REQUIRED_ENV_VARS.difference(environ)
    if len(diff) > 0:
        sys.exit(f'Failed to start application because {diff} environment variables are not set')
    try:
        INFLUX_BATCH_SIZE = int(os.environ['INFLUX_BATCH_SIZE'])
    except:
        sys.exit(f'INFLUX_BATCH_SIZE must be a proper integer')

def main():
    
    checkEnvironmentVariables()
    downloadandprocess()

if __name__ == "__main__":
    main()
