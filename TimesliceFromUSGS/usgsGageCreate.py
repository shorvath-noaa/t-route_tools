import os
import netCDF4
import numpy as np
import xarray as xr
from datetime import datetime, timedelta
import shutil
import pandas as pd

# Local path
script_dir = os.path.dirname(__file__)


def dateStr(dateArray):

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

    outStr = str(int(year))+'-'+monthString+'-'+dayString

    return outStr


def relabel_USGS(inFile, outPath, dateTo):
    
    nameStem = ((inFile.split('/')[-1]).split('.nc'))[0]

    dateString = nameStem.split('_')[0]
    nameString = nameStem.split('_')[1]

    dateSplit = dateString.split('-')
    yearOrig = int(dateSplit[0])
    monthOrig = int(dateSplit[1])
    dayOrig = int(dateSplit[2])
    dateTimeOrig = datetime(yearOrig, monthOrig, dayOrig)

    dateTimeTarget = datetime(int(dateTo[0]), int(dateTo[1]), int(dateTo[2]))

    # get timedelta
    dT = dateTimeTarget-dateTimeOrig

    nameTo = dateStr(dateTo)+'_'+nameString+'.ncdf'
    fileOut = outPath+nameTo

    ncIn = xr.open_dataset(inFile)

    # fix global attributes
    udt_String = ncIn.fileUpdateTimeUTC
    sct_String = ncIn.sliceCenterTimeUTC

    udt = datetime.strptime(udt_String, '%Y-%m-%d_%H:%M:%S')
    sct = datetime.strptime(sct_String, '%Y-%m-%d_%H:%M:%S')

    udt_shift = udt+dT
    sct_shift = sct+dT

    udt_shift_String = udt_shift.strftime('%Y-%m-%d_%H:%M:%S')
    sct_shift_String = sct_shift.strftime('%Y-%m-%d_%H:%M:%S')

    qtimeTime = ncIn.queryTime

    if (len(qtimeTime)>0):

        #print('MOD ', inFile)

        qtime = qtimeTime.values

        pdt = pd.to_datetime(qtime)
        pdtLocal = pdt.tz_localize('America/Chicago')
        pdtShift = pdtLocal + dT
        pdtShiftArray = pdtShift.to_numpy(dtype='datetime64[ns]')

        for i in range(len(qtime)):
            ncIn['queryTime'][i]=pdtShiftArray[i]

        timeData = ncIn.time.values
        timeData0 = timeData[0]
        timeDataString = timeData0.decode()
        time_datetime = datetime.strptime(timeDataString, '%Y-%m-%d_%H:%M:%S')
        time_datetime_shift = time_datetime+dT
        time_datetime_shift_string = time_datetime_shift.strftime('%Y-%m-%d_%H:%M:%S')
        time_NDarray = np.fromstring(time_datetime_shift_string, dtype=np.uint8)
        time_bytes = time_NDarray.tobytes()

        for i in range(len(timeData)):
            ncIn['time'][i]=time_bytes

        ncIn.to_netcdf(fileOut)

    else:

        shutil.copy(inFile, fileOut)


    ncIn.close()

    ncfile = netCDF4.Dataset(fileOut,mode='a',format='NETCDF4_CLASSIC')
    ncfile.fileUpdateTimeUTC=udt_shift_String
    ncfile.sliceCenterTimeUTC=sct_shift_String
    ncfile.close()


def incrementDate(dateIn):

    dateTime = datetime(int(dateIn[0]), int(dateIn[1]), int(dateIn[2]))

    dateTimeNext = dateTime + timedelta(days=1)
    dateOut =np.array([dateTimeNext.year, dateTimeNext.month, dateTimeNext.day])

    return dateOut


# generate list of in-files according to a certain name pattern
def inFiles(inFolder, pattern):
    
    filesIn = []

    files = os.listdir(inFolder)

    for file in files:
    
        if file.startswith(pattern):
            fileIn = inFolder+file
            filesIn.append(fileIn)

    return filesIn


# analyze nc file: get gage IDs needed
def getGageIDs(ncFile):

    ncIn = xr.open_dataset(ncFile)

    gageIter = ncIn.stationId.values

    gageList = []

    for gagePick in gageIter:

        # get string (list), remove leading white spaces 
        gageStr = gagePick.decode('utf-8').lstrip()
        gageList.append(gageStr)
    
    return gageList


def usgsAsciiCrawl(fileName, gageList):

    fileAscii = open(fileName, 'r')
    lines = fileAscii.readlines()
    nLines = len(lines)

    timeBins = []
    stationLists = []
    flowLists = []
    countFound = 0
    foundList = []

    lc=0

    while (lc<nLines):

        lineScan = lines[lc]

        # sweep through header
        if (lc==0):

            while (lineScan.startswith('# Data provided for site') == False):

                lc += 1
                lineScan = lines[lc]

        idScan = lineScan.split('site')[-1].strip()

        if (idScan in gageList and 'Discharge, cubic feet per second' in lines[lc+2]):
            
            countFound += 1
            foundList.append(idScan)
            
            print('Processing gage ID :',idScan)

            while (lineScan.startswith('USGS') == False):

                lc += 1
                lineScan = lines[lc]

            while ('USGS' in lineScan):

                timeStr1 = lineScan.split('\t')[2]
                disChrgStr = lineScan.split('\t')[4]

                disChrgFloat = float(disChrgStr)*0.0283168 # conversion to cubic meters per sec from cubic feet

                timeStr1Split = timeStr1.split(' ')
                timeStr2 = timeStr1Split[0]+'-'+(timeStr1Split[1].replace(':',"-"))

                if (timeStr2 in timeBins):

                    i = timeBins.index(timeStr2)

                    stationLists[i].append(idScan)
                    flowLists[i].append(disChrgFloat)

                else:

                    timeBins.append(timeStr2)
                    stationLists.append([])
                    flowLists.append([])
                    i = len(timeBins)

                    stationLists[i-1].append(idScan)
                    flowLists[i-1].append(disChrgFloat)

                lc += 1
                lineScan = lines[lc]

            lc -= 1
            lineScan = lines[lc]

        else:

            lc += 1

            while (lineScan.startswith('# Data provided for site') == False and lc<nLines):

                lc += 1
                if (lc<nLines):
                    lineScan = lines[lc]    

    print ('DONE WITH TIMESLICE COLLECTION')

    return timeBins, stationLists, flowLists


def writeTimeSlices (ncFile, outFolder, timeBins, stationLists, flowLists, freqString):

    dsIn = xr.open_dataset(ncFile)

    i=0

    for timeBin in timeBins:

        #timeStamp = pd.to_datetime(timeBin, format='%Y-%m-%d-%H-%M-%S')
        timeStamp = pd.to_datetime(timeBin, format='%Y-%m-%d-%H-%M')

        timeBinFull = timeStamp.strftime('%Y-%m-%d_%H:%M:%S')
        timeBinFullFileName = timeStamp.strftime('%Y-%m-%d_%H:%M:%S')
        timeBinFullByte = timeBinFull.encode('utf8')
        timeStampNow = pd.Timestamp.now()
        timeNowStr = timeStampNow.strftime('%Y-%m-%d_%H:%M:%S')

        nStations = len(stationLists[i])

        ds=dsIn.head(nStations)
        ds.attrs['fileUpdateTimeUTC'] = timeNowStr
        ds.attrs['sliceCenterTimeUTC'] = timeBinFull

        for j in range(nStations):

            ds.time.values[j] = timeBinFullByte
            ds.stationId.values[j] = stationLists[i][j].rjust(15)
            ds.discharge.values[j] = np.float32(flowLists[i][j])
            ds.discharge_quality[j] = 100
            ds.queryTime[j] = timeStamp.to_numpy()

        fileName = outFolder+'/'+timeBinFullFileName+'.'+freqString+'.usgsTimeSlice.ncdf'
        ds.to_netcdf(fileName)

        i+=1

def main():

    ncFile = script_dir+'/2020-07-25_00_00_00.15min.usgsTimeSlice.ncdf'
    usgsAsciiFile = script_dir+'/usgs_Ike.dat'

    freq = '15min'
    outFolder = script_dir+'/output'

    gageList = getGageIDs(ncFile)
    timeBins, stationLists, flowLists = usgsAsciiCrawl(usgsAsciiFile, gageList)
    writeTimeSlices (ncFile, outFolder, timeBins, stationLists, flowLists, freq)


if __name__ == "__main__":
    main()

