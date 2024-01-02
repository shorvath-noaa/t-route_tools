import os
import numpy as np
from datetime import datetime, timedelta
import requests
import shutil

# Local path
script_dir = os.path.dirname(__file__)

def awsGetFile(url, outpath):
    """Get data from AWS"""

    urlContent = requests.get(url, stream=True)

    outName = url.split('/')[-1]

    urlStatus = urlContent.status_code

    # download the file if it exists, same name as on aws site
    if urlStatus == 200:

        statusReturn = True

        with open(outpath+'/'+outName,'wb') as fOut:

            shutil.copyfileobj(urlContent.raw,fOut)

    # doesn't exist - 404 error
    elif urlStatus == 404:

        statusReturn = False

    # not sure what else might happen ... 
    else:

        statusReturn = False

    return statusReturn


def awsGetOneDay(urlForcing,dateArray,ExtensionPattern,outpath,hourRange):

    year = dateArray[0]
    month = dateArray[1]
    day = dateArray[2]

    if (month<10):
        monthString = '0'+str(month)
    else:
        monthString = str(month)

    if (day<10):
        dayString = '0'+str(day)
    else:
        dayString = str(day)    

    for hour in range(hourRange):

        if (hour<10):
            hourString = '0'+str(hour)
        else:
            hourString = str(hour)

        urlString = urlForcing+'/'+str(year)+'/'+str(year)+monthString+dayString+hourString+'00.'+ExtensionPattern

        statusReturn = awsGetFile(urlString, outpath)

        if (not statusReturn):
            print("Warning - file not found: ",urlString)
        else:
            print("Successfully fetched: ",urlString)

def incrementDate(dateIn):

    #import pdb; pdb.set_trace()

    dateTime = datetime(int(dateIn[0]), int(dateIn[1]), int(dateIn[2]))

    dateTimeNext = dateTime + timedelta(days=1)
    dateOut =np.array([dateTimeNext.year, dateTimeNext.month, dateTimeNext.day])

    return dateOut


def main():
    # Get pandas.DataFrame

    #url = ('https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing/2020/202001012200.CHRTOUT_DOMAIN1.comp')
    #dlDir = out_path
    #awsGetFile(url,dlDir)

    out_path = os.path.join(script_dir, './', 'download')

    AWS_URL = 'https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/forcing'
    ExtensionPattern = 'CHRTOUT_DOMAIN1.comp'

    date1=np.array([2020,1,10])
    date2=np.array([2020,2,2])
    
    finished = False

    while (not finished):

        if ((date1 == date2).all()):
            finished = True

        awsGetOneDay(AWS_URL,date1,ExtensionPattern,out_path,24)

        dateNext = incrementDate(date1)

        print(dateNext)

        if (finished == True):
            # get midnight after the last day
            awsGetOneDay(AWS_URL,dateNext,ExtensionPattern,out_path,1)

        date1 = dateNext


if __name__ == "__main__":
    main()

