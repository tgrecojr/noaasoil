
import os
from os import environ
import requests
import sys
from datetime import datetime
from bs4 import BeautifulSoup

BASE_NOAA_LOCATION = 'https://www.ncei.noaa.gov/pub/data/uscrn/products/hourly02/'
REQUIRED_ENV_VARS = {"START_YEAR", 
    "END_YEAR"
}

def processfile(datafile):
    print(datafile)

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
                            processfile(newpage + b['href'])
                else:
                    print("Status code {subpage.status_code} received when processing {newpage}") 
            except:
                pass
    else:
        print("Status code {page.status_code} received when processing {BASE_NOAA_LOCATION}")        

    end_time = datetime.now()
    time_taken = end_time - start_time
    print(f"Processed Soil Temps in {time_taken}")

def checkEnvironmentVariables():
    diff = REQUIRED_ENV_VARS.difference(environ)
    if len(diff) > 0:
        sys.exit(f'Failed to start application because {diff} environment variables are not set')
    try:
        START_YEAR = int(os.environ['START_YEAR'])
        END_YEAR = int(os.environ['END_YEAR'])
    except:
        sys.exit(f'START_YEAR and END_YEAR must both be integers reflecting actual years')

def main():
    
    #checkEnvironmentVariables()
    downloadandprocess()

if __name__ == "__main__":
    main()
