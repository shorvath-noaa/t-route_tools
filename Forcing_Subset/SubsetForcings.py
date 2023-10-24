
import qgis

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
            


# generate list of in- and output file namses for forcing files in 
# a given folder 
def ForcingInOutFiles(csvFolder,csvOutFolder,addString):
    # Arguments (all file paths need to be global):
    #
    #   csvfolder: folder in which the "large", pre-subset 
    #              csv forcing files are located (global path)
    #
    #   csvOutfolder: folder for the subsetted forcing files 
    #                 (glibal path))
    #
    #   addString: add string to mark the subsetted files, to be inserted
    #              just before the .csv extension; use addString='' if
    #              the file names should be the same as the "large" csv files
    #
    # Returns:
    #
    #   filesIn, filesOut: lists of global names for in- and output files
    #
    
    filesIn = []
    filesOut = []

    # iterate through forcing files in directory and define output names for each
    files = os.listdir(csvFolder)
    for file in files:
    
        #print(file)
    
        if file.endswith('.csv'):

            fileIn = csvFolder+file
        
            print(fileIn)
        
            fileBase = file.split('.c')[0]
            fileOut = csvOutFolder+fileBase+addString+'.csv'
        
            filesIn.append(fileIn)
            filesOut.append(fileOut)        

    return filesIn, filesOut


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


# extract selected nexus IDs in geopackage
geoPackageFile = 'C:/Users/JurgenZach/Documents/Geomodels/SubsetForcings/Models/Austin_Flows_rank_1.gpkg'
columnName = 'nexus'
nexusList = extract_IDs_geopackage(geoPackageFile, columnName)

# generate in- and output names
csvFolder = 'C:/Users/JurgenZach/Documents/Geomodels/SubsetForcings/Forcing_Examples/'
csvOutFolder = 'C:/Users/JurgenZach/Documents/Geomodels/SubsetForcings/Forcing_Subset/'
addString = '_subset'
filesIn, filesOut = ForcingInOutFiles(csvFolder,csvOutFolder,addString)

# run the subsetting
SubsetForcing_CVS(filesIn, filesOut, nexusList)

