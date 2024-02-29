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
    timeLists = []
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

                timeStamp = pd.to_datetime(timeStr1, format='%Y-%m-%d %H:%M')

                minutes = timeStamp.minute
                minutesMod15 = minutes%15
                timeStampRound15 = timeStamp

                # round filename to the nearest 15 minutes
                if (minutesMod15 != 0):

                    minutesLL = 15*int(minutes/15)
                    timeStampRound15 = timeStampRound15.replace(minute=minutesLL)

                    if (minutesMod15>=8):
                        timeStampRound15 = timeStampRound15 + timedelta(minutes = 15)

                timeStr_15 = timeStampRound15.strftime('%Y-%m-%d %H:%M:%S')

                timeStr15_Split = timeStr_15.split(' ')
                timeStr15_Str = timeStr15_Split[0]+'-'+(timeStr15_Split[1].replace(':',"-"))

                timeStr1Split = timeStr1.split(' ')
                timeStr_Analog = timeStr1Split[0]+'-'+(timeStr1Split[1].replace(':',"-"))

                if (timeStr15_Str in timeBins):

                    i = timeBins.index(timeStr15_Str)

                    stationLists[i].append(idScan)
                    flowLists[i].append(disChrgFloat)
                    timeLists[i].append(timeStr_Analog)

                else:

                    timeBins.append(timeStr15_Str)
                    timeLists.append([])
                    stationLists.append([])
                    flowLists.append([])
                    i = len(timeBins)

                    stationLists[i-1].append(idScan)
                    flowLists[i-1].append(disChrgFloat)
                    timeLists[i-1].append(timeStr_Analog)

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

    return timeBins, stationLists, flowLists, timeLists


def writeTimeSlices (ncFile, outFolder, timeBins, stationLists, flowLists, timeLists, freqString):

    dsIn = xr.open_dataset(ncFile)

    i=0

    for timeBin in timeBins:

        timeStamp = pd.to_datetime(timeBin, format='%Y-%m-%d-%H-%M-%S')

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

            timeEntryStamp = pd.to_datetime(timeLists[i][j], format='%Y-%m-%d-%H-%M')
            timeEntryFull = timeEntryStamp.strftime('%Y-%m-%d_%H:%M:%S')
            timeEntryFullByte = timeEntryFull.encode('utf8')
            ds.time.values[j] = timeEntryFullByte
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
    timeBins, stationLists, flowLists, timeLists = usgsAsciiCrawl(usgsAsciiFile, gageList)
    writeTimeSlices (ncFile, outFolder, timeBins, stationLists, flowLists, timeLists, freq)


if __name__ == "__main__":
    main()

