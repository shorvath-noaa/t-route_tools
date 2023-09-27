from qgis.core import *
from qgis import processing

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
from joblib import delayed, Parallel

import tRoute_QGIS_interface
reload(tRoute_QGIS_interface)
from tRoute_QGIS_interface import copy_geo_file


# Straightforward: define polygon layer in QGIS to make "cutouts"
# coordinates presently hardcoded to EPSG:5070 - NAD83 / Conus Albers,
# please submit request if other coordinate system options are needed
def define_polygon (pointList, outpath, polygonName):
    
    # Arguments (all file paths need to be global):
    #
    #   pointList is in the format: 
    #    [(Easting1, Northing1), (Easting2, Northing2), ...]
    #    At least three points are needed
    #
    #   outpath: target output file (auxiliary files are created in the 
    #    same folder
    #
    #   polygonName: name of polygon (any string)
    #
    # Returns:
    #
    #   cookie_cutter: vector layer with "cutout" polygon
    #
    
    # define vector layer for the cutout-polygon
    # for now, the coordinate system is hard-coded
    cookie_cutter = QgsVectorLayer('Polygon?crs=epsg:5070', polygonName, 'memory')
    cookie_cutter_provider = cookie_cutter.dataProvider()
    
    # define cookie cutter as feature and add to project
    cookie_feature = QgsFeature()
    
    # define the "cookie"-cutout-polygon
    polygonSelect = QgsGeometry.fromPolygonXY( [[ QgsPointXY \
                ( pair[0], pair[1] ) for pair in pointList ]] ) 
        
    
    cookie_feature.setGeometry(polygonSelect)
    cookie_cutter_provider.addFeatures([cookie_feature])
    print('Cookie cutter defined: ',cookie_feature.geometry())

    Project = QgsProject.instance()
    Project.addMapLayers([cookie_cutter])

    # return the polygon cutout vector layer
    return cookie_cutter


# cut out a purely geographic subset of a geopackage    
def cut_cookie (root, group, cookie_cutter, polygonName, \
                supernetwork_parameters, outpath, layer_with_IDs, \
                layerNameAdd):
    
    # Arguments (all file paths need to be global):
    #
    #   root: QGS project instance
    #
    #   group: grouping of vector layers that will comprise the cutout geopackage
    #      
    #   cookie_cutter: vector layer comprised by cutout-polygon
    #
    #   polygonName: name of the polygon as previously defined 
    #    (stringl; individual designation)
    # 
    #   supernetwork_parameters: standard file from T-route, pointing to 
    #    "large" model, e.g., Conus  
    #
    #   outpath: output path for geopackage file (global path recommended)
    #
    #   layer_with_IDs: name of the sub-layer in geopackage that 
    #     shall serve to define the water body ID's (generally 'flowpaths'
    #
    #   layerNameAdd: suffix (string) to add to vector layer names
    #     e.g., 'shnip'
    #
    # Returns:
    #
    #   subLayers_without_geography, subLayers_with_geography: Lists of 
    #       vector layers with and without geometry features, respectively
    #
    #   cookieList: list of segment IDs inside the cutout polygon
    #
    
    
    # make a copy of the original "big" file for QGIS windowing operations
    big_copy = copy_geo_file(supernetwork_parameters,'_windowing')

    # load it as test layer, and extract sub-layers (geo-data layers)
    big_layer = QgsVectorLayer(big_copy,"test","ogr")
    sub_layers =big_layer.dataProvider().subLayers()
    
    # list of the segment ID's that end up inside the cookie cutter
    cookieList = []
    
    # prepare iterating through sub-layers
    firstLayerWritten = 'False'
    subLayers_without_geography = []
    subLayers_with_geography = []
    
    # now iterate through sublayers
    for sub_layer in sub_layers:

        # name and file ID of sublayer
        name = sub_layer.split('!!::!!')[1]
        fileID = "%s|layername=%s" % (big_copy, name,)
        # Create layer
        sub_vlayer = QgsVectorLayer(fileID, name, 'ogr')
        # Add layer to map
        QgsProject.instance().addMapLayer(sub_vlayer)
        
        # Check geometry features in layers (or lack thereof)
        nPoints, nPolygons, nMultiPolygons, nLineStrings, nMultiLineStrings, \
                nNoGeo, nSomethingElse, nFeatures, nGeoTotal \
                = check_layer_geometry (sub_vlayer, sub_layer, polygonName)

        # Save layers without geolocation processable by QGIS into list
        # for subsequent cookie-cutting with other methods (not in this routine)
        if (nGeoTotal == 0):

            subLayers_without_geography.append(sub_layer)        
            QgsProject.instance().removeMapLayer( sub_vlayer.id())  

        elif (nGeoTotal == nFeatures):

            # cookie-cutter windowing with QGIS native selection-by-location (w./in polygon)        
            processing.run("native:selectbylocation", \
                {'INPUT':sub_vlayer,'PREDICATE':[0], \
                'INTERSECT':cookie_cutter ,'METHOD':0})
            
            tempOut = processing.run("native:saveselectedfeatures", \
                {'INPUT':sub_vlayer,'OUTPUT':'TEMPORARY_OUTPUT'})['OUTPUT']
            
            tempOut.setName(name+layerNameAdd)
    
            subLayers_with_geography.append(sub_layer)

            QgsProject.instance().addMapLayer(tempOut)
            group.insertChildNode(0, QgsLayerTreeLayer(tempOut))
            QgsProject.instance().removeMapLayer( sub_vlayer.id())

            if (firstLayerWritten == 'False'):
                overWriteFlag = 'True'
                firstLayerWritten = 'True'
            else:
                overWriteFlag = 'False'
        
            processing.run("native:package", {'LAYERS':tempOut, \
                'OUTPUT':outpath,'OVERWRITE':overWriteFlag,'SAVE_STYLES':True, \
                'SAVE_METADATA':True})
    
            if (name == layer_with_IDs):
    
                feats = tempOut.getFeatures()
                cookieList = []

                for feat in feats:
                    attrs = feat.attributes()
                    cookieList.append(attrs[1])
            
    return subLayers_without_geography, subLayers_with_geography, cookieList
 

# calculate some QC statistics and to decide whether to do QGIS based geograpic
# subsetting or subsetting according to segment IDs
def check_layer_geometry (sub_v_layer, layerName, polygonName):
    
    # Arguments: 
    #
    #   sub_v_layer: vector layer to be scanned for geo-features
    #
    #   layerName: name of vector layer
    #
    #   polygonName: name of the polygon as previously defined 
    #    (stringl; individual designation)
    #
    # Returns:
    #
    #   Statistics of geometry features in vector layer:
    #       nPoints
    #       nPolygons
    #       nMultiPolygons
    #       nLineStrings
    #       nMultiLineStrings
    #       nNoGeo: (no geometry at all)
    #       nSomethingElse: geometry, but other
    #       nFeatures: total features 
    #       nGeoTotal: total features with geometry
    #
    
    # get features
    feats = sub_v_layer.getFeatures()

    # initialize/define typed of geometry features
    nPoints = 0
    nLineStrings = 0
    nMultiLineStrings = 0
    nPolygons = 0
    nMultiPolygons = 0
    nNoGeo = 0
    nSomethingElse = 0

    if (layerName != polygonName):
        
        featureList = list(feats)
        
        print ('Geometry check layer: ',layerName)
        nFeatures = len(featureList)
        print ('Number of features: ',nFeatures)
    
        for feat in featureList:

            geoFeature = feat.geometry()
            geoType = QgsWkbTypes.displayString(geoFeature.wkbType())
            
            if (geoType == 'Point'):
                nPoints += 1
            elif (geoType == 'Polygon'):
                nPolygons += 1
            elif (geoType == 'MultiPolygon'):
                nMultiPolygons += 1
            elif (geoType == 'LineString'):
                nLineStrings += 1
            elif (geoType == 'MultiLineString'):
                nMultiLineStrings += 1                
            elif (geoType == 'Unknown'):
                nNoGeo += 1
            else:
                if (nSomethingElse == 0):
                    print ('Captured un-classified geometry feature: ',geoType)
                nSomethingElse += 1
        
        print()
        print ('Geometry feature composition: ')
        print ('Points: ',nPoints)
        print ('LineStrings: ',nLineStrings)
        print ('MultiLineStrings: ',nMultiLineStrings)
        print ('Polygons:',nPolygons)
        print ('MultiPolygons:',nMultiPolygons)
        print ('Non-Geo features:',nNoGeo)
        print ('Uncategorized:',nSomethingElse)
        print()
        
        nGeoTotal = nPoints+nLineStrings+nMultiLineStrings+nPolygons+nMultiPolygons
        
        if (nGeoTotal + nNoGeo < nFeatures):
            print ('Inconsistent geographic feature count!')
        else:
            if (nGeoTotal == 0):
                print ('No geo-features in layer.')
            elif (nGeoTotal == nFeatures):
                print ('Fully defined geo-features in layer.')
            else:
                print ('Geographic elements not defined in all features')

        print()
        print()
        print()
    
        return nPoints, nPolygons, nMultiPolygons, nLineStrings, \
            nMultiLineStrings, nNoGeo, nSomethingElse, nFeatures, nGeoTotal
        
        
# subsetting according to segment IDs        
def window_NoGeo (root, group, supernetwork_parameters, outpath, \
                    select_ID_list, subLayers_without_geography, layerNameAdd):

    # Arguments (all file paths need to be global):
    #
    #   root: QGS project instance
    #
    #   group: grouping of vector layers that will comprise the cutout geopackage
    # 
    #   supernetwork_parameters: standard file from T-route, pointing to 
    #    "large" model, e.g., Conus  
    #
    #   outpath: output path for geopackage file (global path recommended)
    #
    #   select_ID_list: list of segment IDs that correspond to "cutout" polygon
    #
    #   subLayers_without_geography: list of vector layer names to be processed
    #       by this routine
    #
    #   layerNameAdd: suffix (string) to add to vector layer names
    #     e.g., 'shnip'
    #
    # Returns:
    #
    #   Nothing so far
    #


    # make a copy of the original "big" file for QGIS windowing operations
    big_copy = copy_geo_file(supernetwork_parameters,'_windowing_nogeo')

    # load it as test layer, and extract sub-layers (geo-data layers)
    big_layer = QgsVectorLayer(big_copy,"test","ogr")
    sub_layers =big_layer.dataProvider().subLayers()


    for subLayer in subLayers_without_geography:
        
        name = subLayer.split('!!::!!')[1]
        fileID = "%s|layername=%s" % (big_copy, name,)
        # Create layer
        sub_vlayer = QgsVectorLayer(fileID, name, 'ogr')
        # Add layer to map
        QgsProject.instance().addMapLayer(sub_vlayer)    
    
        print('Non-geo layer: ',name)
    
        # delete all features that are not in keeperList
        feats = sub_vlayer.getFeatures()
        delFeats = []
        nKeepers=0
        nDelete=0
        for feat in feats:
            attrs = feat.attributes()
            if (operator.contains(select_ID_list,attrs[1])):
                nKeepers += 1
            else:
                nDelete += 1
                delFeats.append(feat.id())
    
        print('Keeper-features: ',nKeepers)
        print('Deleted features: ',nDelete)
        print()
        print()
    
        res = sub_vlayer.dataProvider().deleteFeatures(delFeats)
    
        sub_vlayer.setName(name+layerNameAdd)
        QgsProject.instance().addMapLayer(sub_vlayer)
        group.insertChildNode(0, QgsLayerTreeLayer(sub_vlayer))
        
        processing.run("native:package", {'LAYERS':sub_vlayer,'OUTPUT':outpath,'OVERWRITE':False,'SAVE_STYLES':True,'SAVE_METADATA':True})
    
