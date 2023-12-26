
import qgis

import netCDF4
import numpy as np
import xarray

import csv

import importlib
from importlib import reload

import os
import operator
import random
import shutil

import pandas as pd
import numpy as np
import geopandas as gpd
import time
import json
from pathlib import Path

# extract the nexus IDs from a given geopackage
def extract_IDs_geopackage(geoPackageFile, columnName):
    # Arguments (all file paths need to be global):
    #
    #   geoPackageFile: full path to geopackage file
    #
    #   columnName: name of column in which nexus IDs are located
    #               (typically that's 'nexus')
    #
    # Returns:
    #
    #   list of nexus IDs (unique integer number)
    #

    # load geopackage, and extract sub-layers (geo-data layers)
    mainLayer = QgsVectorLayer(geoPackageFile,"test","ogr")
    subLayers = mainLayer.dataProvider().subLayers()

    for subLayer in subLayers:
        
        name = subLayer.split('!!::!!')[1]
    
        #print(name)
    
        if (name == columnName):
    
            fileID = "%s|layername=%s" % (geoPackageFile, name,)
            # create vector layer
            sub_vlayer = QgsVectorLayer(fileID, name, 'ogr')
        
            field_names = [field.name() for field in sub_vlayer.fields()]
        
            print('Extract ',columnName,' id from vector layer with columns ',field_names)
        
            idIndex = field_names.index('id')
        
            #print(idIndex)
        
            # get all features and extract a list of nexus IDs
            feats = sub_vlayer.getFeatures()
            nexusIDs = []
        
            for feat in feats:
                attrs = feat.attributes()
                # extract integer part of nexus IDs and append them to list
                idName = attrs[idIndex]
                idInt = int(idName.split('-')[-1])
                nexusIDs.append(idInt)
            
            nexusSet = set(nexusIDs)
    
            return list(nexusSet)
            


# extract the nexus IDs from a given geopackage
def extract_IDs_NHD(geoPackageFile, vectorLayer, entryName_NHD, entryName_HY):
    # Arguments (all file paths need to be global):
    #
    #   geoPackageFile: full path to geopackage file
    #
    #   vectorLayer: layer in geopackage in which the NHD IDs are saved, typically network 
    #
    #   entryName_NHD: name of column in which NHD IDs are located, typically hf_id
    #
    #   entryName_HY: name of column in which HY features IDs are located, typically id
    #
    # Returns:
    #
    #   list of NHD IDs (unique integer number)
    #

    # load geopackage, and extract sub-layers (geo-data layers)
    mainLayer = QgsVectorLayer(geoPackageFile,"test","ogr")
    subLayers = mainLayer.dataProvider().subLayers()

    for subLayer in subLayers:
        
        name = subLayer.split('!!::!!')[1]
    
        #print(name)
    
        if (name == vectorLayer):
    
            fileID = "%s|layername=%s" % (geoPackageFile, name,)
            # create vector layer
            sub_vlayer = QgsVectorLayer(fileID, name, 'ogr')
        
            field_names = [field.name() for field in sub_vlayer.fields()]
        
            print('Extract ',entryName_NHD,' id from vector layer ',name)
        
            index_NHD = field_names.index(entryName_NHD)
            index_HY = field_names.index(entryName_HY)
        
            #print(idIndex)
        
            # get all features and extract a list of nexus IDs
            feats = sub_vlayer.getFeatures()
            nhdIDs = []
            wbIDs = []
        
            for feat in feats:
                attrs = feat.attributes()
                # extract integer part of nexus IDs and append them to list
                nhdEntry = attrs[index_NHD]
                #print(nhdEntry)
                if (nhdEntry != NULL):
                    nhdNumber = int(nhdEntry)
                    wbEntry = attrs[index_HY]
                    wbNumber = int(wbEntry.split('-')[-1])
                    nhdIDs.append(nhdNumber)
                    wbIDs.append(wbNumber)
            
    returnID_zip = list(zip(wbIDs, nhdIDs))
    
    returnID_zip.sort()
    
    return returnID_zip, nhdIDs


# generate list of in- and output file namses for forcing files in 
# a given folder 
def ForcingInFiles(inFolder):
    # Arguments (all file paths need to be global):
    #
    #   infolder: folder in which the "large", pre-subset 
    #              .comp forcing files are located (global path)
    #
    #   csvOutfolder: folder for the subsetted forcing files 
    #                 (glibal path))
    #
    #
    # Returns:
    #
    #   filesIn: list of global names for in-files
    #
    
    filesIn = []

    # iterate through forcing files in directory and define output names for each
    files = os.listdir(inFolder)
    for file in files:
    
        #print(file)
    
        if file.endswith('.comp'):

            fileIn = inFolder+file
        
            print(fileIn)
        
            filesIn.append(fileIn)

    return filesIn


# perform subsetting
def SubsetForcing_CVS(filesIn, filesOut, nexusList):
    # Arguments (all file paths need to be global):
    #
    #   filesIn: list of global paths for "large", non-subset forcing files
    # 
    #   filesOut: list of global paths for output (subsetted) file names
    #
    #   nexusList: list of nexus IDs (unique integer number)
    #
    # Returns:
    #
    #   Nothing so far
    
    nFiles = len(filesIn)

    for n in range (0,nFiles):
    
        print ('File number ',n+1,' out of ',nFiles)

        #load dataframe from source forcing file
        dfIn = pd.read_csv(filesIn[n])

        # column names
        header = dfIn.columns

        # make sure data formats remain consistent
        convertDict = {"feature_id": int, header[1]: str}    
        dfIn2 = pd.read_csv(filesIn[n], dtype = convertDict)
    
        # this line does the subsetting work
        dfSelect = dfIn2.loc[dfIn["feature_id"].isin(nexusList)]

        # consistent data formats
        dfSelect = dfSelect.astype(convertDict)

        #print(filesOut[n])
    
        # write to csv
        dfSelect.to_csv(filesOut[n], index=False)


def makeForcingFiles(inFile, outPath, ID_list, NHDlist):
    
    nameStem = ((inFile.split('/')[-1]).split('.CHRTOUT'))[0]
    
    outNC = outPath+nameStem+'.CHRTOUT_DOMAIN1'
    outCSV = outPath+nameStem+'.CHRTOUT_DOMAIN1.csv'

    ncIn = xarray.open_dataset(inFile)
    
    subsetNC = ncIn.sel(feature_id = NHDlist)
    subsetNC.to_netcdf(outNC)

    hyID = []
    flow = []

    prevHY = -1
    flow_avg = 0

    for ll in ID_list:

        if (ll[0] == prevHY):
            
            flow_avg += float(ncIn.sel(feature_id=ll[1]).streamflow.values)
            avgN += 1
        
        else:

            if (prevHY>=0):
                hyID.append(prevHY)
                flow.append(flow_avg/avgN)

            prevHY = ll[0]
            flow_avg = float(ncIn.sel(feature_id=ll[1]).streamflow.values)
            avgN = 1
            
    with open(outCSV, 'w', newline='') as csvOut:
        
        header = ['feature_id', nameStem]
        
        csvWriter = csv.writer(csvOut)
        csvWriter.writerow(i for i in header)
        csvWriter.writerows(zip(hyID,flow))
        
    csvOut.close()
        
    print(nameStem, outNC)


inFolder = 'C:/Users/JurgenZach/Documents/Geomodels/SubsetForcings_Retro/In/'

outPathList = []
outPathList.append('C:/Users/JurgenZach/Documents/Geomodels/SubsetForcings_Retro/Out/')

gpkgList = []
gpkgList.append('C:/Users/JurgenZach/Documents/Geomodels/SubsetForcings_Retro/Diff_Texas_BuffaloBayou_NGENv201.gpkg')

filesIn = ForcingInFiles(inFolder)

vectorLayer = 'network'
entryName_NHD = 'hf_id'
entryName_HY = 'id'

i=0

for gpkg in gpkgList:
    
    outPath = outPathList[i]
    i+=1

    for fileIn in filesIn:

        ID_list, NHDlist = extract_IDs_NHD(gpkg, vectorLayer, entryName_NHD, entryName_HY)
        makeForcingFiles(fileIn, outPath, ID_list, NHDlist)




