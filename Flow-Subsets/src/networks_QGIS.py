import importlib
from importlib import reload

import os
import operator
import random
import shutil
import copy

from PyQt5.QtGui import QColor

import pandas as pd
import numpy as np
import geopandas as gpd
import time
import json
from pathlib import Path
from joblib import delayed, Parallel

import windowing_QGIS
importlib.reload(windowing_QGIS)
from windowing_QGIS import *
from windowing_QGIS import copy_geo_file

import tRoute_QGIS_interface
reload(tRoute_QGIS_interface)
from tRoute_QGIS_interface import *

from troute.log_level_set import log_level_set
from troute.nhd_network import reverse_dict, extract_connections, replace_waterbodies_connections, reverse_network, reachable_network, reachable, split_at_waterbodies_and_junctions, split_at_junction, dfs_decomposition, headwaters, tailwaters
from troute.nhd_network_utilities_v02 import organize_independent_networks
from troute.nhd_io import read_config_file
from troute.AbstractRouting import MCOnly, MCwithDiffusive, MCwithDiffusiveNatlXSectionNonRefactored, MCwithDiffusiveNatlXSectionRefactored


# Core routine for connected subsets: find sets of segments from a larger geopackage that
# also overlap with a smaller geopackage and extract them, ranked by overlap
def subsetConnections_Tx (connections_BIG, connections_SMALL, draws, ranking_BIG, ranking_SMALL):

    #
    # Arguments: 
    #
    #   connections_BIG: Connections dictionary of "big" model, e.g., Conus or an ngen submodel
    #
    #   connections_BIG: Connections dictionary of "small" model, e.g., geography-only subset
    #
    #   draws:  for quality control, a number of connected sets comprising the "big" geopackage can 
    #           be extracted a number of times (that number being draws). If the results are not 
    #           consistent, the "small" and "big" geopackages may not be well-matched, for 
    #           example, the "small" package may be poorly connected to the "big" one
    #
    #   ranking_BIG: The number of connected networks out of the "big" network to be tested for 
    #                   overlap with the "small" geopackage. 
    #
    #   ranking_SMALL: The maximum number of connected networks to be extracted that overlap 
    #                   with the "small" geopackage 
    #
    # Return:
    #
    #   returnOverlap: list of set of segment IDs that overlap with the "small" model,
    #                   sorted by size of the overlap
    #
    #   returnOverlapComplexity: list of the complexity (total size) of the set of segment 
    #                               IDs that overlap with the "small" model, ranked by overlap
    #
    #   returnOverlapSize: total list of the total sizes if the overlap of the segment
    #                       networks with the "small" geopackage
    #
    
    # extract all the keys from connections_SMALL's connections dictionary
    key_sweep = list(connections_SMALL.keys())
    key_set = set(key_sweep)

    Size_all = []
    Size_outside = []
    Size_inside = []
    
    i=0
    
    # perform random draws to extract rankiong connections networks from the 
    # "big" geopackage; check for inconsistencies among draws
    while (i<draws):
 
        # randomized draw for target segment 
        key_random = random.choice(key_sweep)
        key = key_random
       
        # save previous overlap set
        if (i>0):
            bigBucketOverlap_previous = bigBucketOverlap
        else:      
            bigBucketOverlap_previous = []
       
        # connected subsets among "big" model: prepare output
        bigBuckets = []
        bigBucketComplexity = []
        bigBucketOverlap = []        
        
        # extract connections network potentially overlapping with "small" model
        segment = [key]        
        # revert connections network
        rev_connections_BIG = reverse_network(connections_BIG)
        # extract connected reachable network
        connections_segment_BIG_dict = reachable_network(rev_connections_BIG,\
                        sources=None, targets=segment, check_disjoint=True)

        # extraxt ranking of "big" connections network - first, in case 
        # ranking = 1, just get the maximum network
        if (ranking_BIG == 1):
            uniqueMaxNet, sizeUniqueNet = complexity_dict_max (connections_segment_BIG_dict)
            bigBuckets = [uniqueMaxNet]
            bigBucketComplexity = [sizeUniqueNet]
            print('Big draw: ',i,"out of ",draws)
            print('Max size ',sizeUniqueNet)

        # rankings beyond just the largest
        elif (ranking_BIG > 1):
            bigBuckets, bigBucketComplexity = complexity_dict_N_largest \
                    (connections_segment_BIG_dict,ranking_BIG)
            print('Max size ',bigBucketComplexity[0])

        # eventually introduce error messages
        else:
            print ('Invalid ranking parameter (needs to be 1 or greater')

        # get overlap between complexity-ranked networks in "big" model and 
        # the "small" model. 
        for bucket in bigBuckets:
            bucket_intersect = bucket.intersection(key_set)
            overlap = len(bucket_intersect)
            bigBucketOverlap.append(overlap)

        # provide warning (future error message?) in case subsequent random 
        # draws give inconsistent connections between networks of "big" and 
        # "small" models. 
        if (bigBucketOverlap_previous != bigBucketOverlap):
            print('Warning: inconsistent network connectivity. \
                    Small model likely not well connected to large model.')

        # increment counter
        i+=1

        # once we are done with the QC "draws", get the models that actually 
        # overlap with the "small" model and rank them
        if (i==draws):

            # consistency in complexity ranking
            if (ranking_SMALL > ranking_BIG):
                ranking_SMALL = ranking_BIG
                print('Complexity ranking of networks overlapping with "small" \
                    networks cannot be larger than total complexity ranking')
                print('Resized: ranking_SMALL = ranking_BIG = ',ranking_BIG)

            # sort networks according to largest overlap
            rankedOverlapSize, rankedOverlapIndex = overlap_N_largest \
                    (bigBucketOverlap, ranking_SMALL)

            # prepare ranking results to be returned
            returnOverlapSize = []
            returnOverlapComplexity = []
            returnOverlap = []

            if (rankedOverlapSize[0] == 0):
                print('No overlapping network found. Check size of "small" \
                        ranking, and consider increasing search ranking.')
                
            else:
                j=0
                # pass all the networks found with non-zero overlaps                   
                for overlapSize in rankedOverlapSize:
                    if (overlapSize > 0):
                        if (j==0):
                            indexBig = rankedOverlapIndex[j]
                            returnOverlapSize.append(rankedOverlapSize[j])
                            returnOverlapComplexity.append(bigBucketComplexity[indexBig])
                            returnOverlap.append(bigBuckets[indexBig])
                            indexBig_prev = indexBig
                        else:
                            indexBig = rankedOverlapIndex[j]
                            bigBucketsNew = bigBuckets[indexBig]
                            if (bigBucketsNew != bigBuckets[indexBig_prev]):
                                returnOverlapSize.append(rankedOverlapSize[j])
                                returnOverlapComplexity.append(bigBucketComplexity[indexBig])
                                returnOverlap.append(bigBucketsNew)
                            indexBig_prev = indexBig
                    j+=1
                    
    return returnOverlap, returnOverlapComplexity, returnOverlapSize


# extract the "largest" connections network out of a connections dictionary,
# based on tailwaters
def complexity_dict_max (connections_dict):

    #
    # Arguments: 
    #
    #   connections_dict: Connections dictionary from geopackage
    #    
    # Returns:
    #
    #   uniqueMaxNet: set of connected segment IDs
    #
    #   sizeUniqueNet: size of the latter
    #   
    
    # get the richest connections network to the segment investigated
    connection_values = [len(list(connections_dict[tw].values())) for tw in connections_dict.keys()]
    
    # maximum index
    maxIndex = connection_values.index(max(connection_values))
    
    # get corresponding segment ID 
    keyList = list(connections_dict.keys())
    segID = keyList[maxIndex]
    
    # get network as list
    maxNetList = list(connections_dict[segID].values())

    # extract set of unique segment IDs, start with the tail-/headwater ID 
    uniqueMaxNet = {segID}
    
    for connections in maxNetList:
        
        if (len(connections) > 0):
            
            uniqueMaxNet = uniqueMaxNet.union(set(connections))
            
    sizeUniqueNet = len(uniqueMaxNet)

    return uniqueMaxNet, sizeUniqueNet
    
    
# extract the N "largest" connections networks out of a connections dictionary,
# based on tailwaters    
def complexity_dict_N_largest (connections_dict,N):
    
    #
    # Arguments: 
    #
    #   connections_dict: Connections dictionary from geopackage
    #
    #   maximum length of size ranking of connected networks to be extracted
    #    
    # Returns:
    #
    #   segBuckets: list of ranked sets of connected segment IDs
    #
    #   segBucketComplexity: list of sizes of the latter
    #       
    
    # get the richest connections network to the segment investigated
    connection_values = [len(list(connections_dict[tw].values())) for tw \
                        in connections_dict.keys()]
    connection_values_sorted = copy.deepcopy(connection_values)
    connection_values_sorted.sort()
    
    # sort corresponding segment ID 
    # custom function to retrieve the index of an element in the ordered tupel list
    def map_index(element):
        return connection_values.index(element)
    
    keyList = list(connections_dict.keys())
    segBuckets = []
    segBucketComplexity = []
    
    for i in range(1,N+1):
        
        # get maximum connection value
        connValue = connection_values_sorted[-i]
    
        # get original index (before sorting)
        indexOrig = map_index(connValue)
        
        # get segment ID and attached network
        segID = keyList[indexOrig]
        rankingNetList = list(connections_dict[segID].values())
        
        # build up segment list
        # extract set of unique segment IDs, start with the tail-/headwater ID 
        uniqueRankingNet = {segID}
        
        for connections in rankingNetList:
    
            if (len(connections) > 0):
            
                uniqueRankingNet = uniqueRankingNet.union(set(connections))
    
        sizeUniqueNet = len(uniqueRankingNet)    
    
        segBuckets.append(uniqueRankingNet)
        segBucketComplexity.append(sizeUniqueNet)

    return segBuckets, segBucketComplexity    


# sort the largest overlaps from a list thereof
def overlap_N_largest (segBucketOverlap, N):
    
    #
    # Arguments: 
    #
    #   segBucketOverlap: list of overlap sizes corresponding to
    #                       different flow networks
    #
    #   N: ranking extracted
    #    
    # Returns:
    #
    #   overlap: ordered list of ranked overlap sizes
    #
    #   overlapIndex: indices for ranked list in original order
    #           
    
    # get the richest connections network to the segment investigated
    segBucketOverlap_sorted = copy.deepcopy(segBucketOverlap)
    segBucketOverlap_sorted.sort()
        
    # sort corresponding segment ID 
    # custom function to retrieve the index of an element in the ordered tupel list
    def map_index2(element):
        return segBucketOverlap.index(element)
    
    segBucketOverlap_sorted = copy.deepcopy(segBucketOverlap)
    segBucketOverlap_sorted.sort()

    overlap = []
    overlapIndex = []
    
    for i in range(1,N+1):
        
        rankedOverlap = segBucketOverlap_sorted[-i]
        
        overlap.append(rankedOverlap)
        overlapIndex.append(map_index2(rankedOverlap))
        
    return overlap, overlapIndex



# build up a geopackage from a list of flowpath segment IDs
# (generally, connected network from one or more segments)
def one_net_build (root, group, supernetwork_parameters_SMALL, \
        supernetwork_parameters_BIG, outpath_build, ID_net_list, \
        layerNameAdd, renderColor):

    #
    # Arguments (all file paths need to be global):
    #
    #   root: QGS project instance
    #
    #   group: grouping of vector layers that will comprise the output geopackage
    # 
    #   supernetwork_parameters_SMALL: standard file from T-route, pointing to 
    #    "small" model, e.g., geography-only subset of geopackage
    #
    #   supernetwork_parameters_BIG: standard file from T-route, pointing to 
    #    "big" model, e.g., Conus or an ngen model 
    #
    #   outpath_build: output path for geopackage file (global path recommended)
    #
    #   ID_net_list: list of segment IDs for selected geopackage
    #
    #   layerNameAdd: suffix (string) to add to vector layer names
    #     e.g., 'shnip'
    #
    #   renderColor: list of RGB parameters (encoded 0...255 each)
    #
    # Returns:
    #
    #   Nothing so far
    #    

    # make a copy of the original "small" file for QGIS windowing operations
    small_copy = copy_geo_file(supernetwork_parameters_SMALL,'_flowBuild')

    print ('copy destination: ',small_copy)

    # load it as test layer, and extract sub-layers (geo-data layers)
    global_layer = QgsVectorLayer(small_copy,"test","ogr")
    sub_layers =global_layer.dataProvider().subLayers()

    geo_big_path = supernetwork_parameters_BIG["geo_file_path"]

    # prepare iterating through sub-layers
    firstLayerWritten = 'False'    

    for subLayer in sub_layers:
        
        # open the sublayer of the "small" file
        name = subLayer.split('!!::!!')[1]
        fileID_small = "%s|layername=%s" % (small_copy, name,)
        # Create layer
        sub_vlayer_small = QgsVectorLayer(fileID_small, name, 'ogr')
        # define corresponding data provider
        sub_vlayer_small_data = sub_vlayer_small.dataProvider()

        # Extract geometry type
        geoIndex = int(sub_vlayer_small.geometryType())
        if (geoIndex == 0):
            geoString = "Point"
        elif (geoIndex == 1):
            geoString = "LineString"
        elif (geoIndex == 2):
            geoString = "Polygon"
        elif (geoIndex == 3):
            geoString = "MultiLine"        
        elif (geoIndex == 4):
            geoString = "None"        
        else:
            print('Invalid geometry type!!!')

        # create new layer with same geometry and
        # attributes as the "small" layer; 
        # for now: hardcode coordinate system
        newLayer = QgsVectorLayer(geoString+"?crs=epsg:5070", name, "memory")
        newLayer_data = newLayer.dataProvider()
        newLayer_data.addAttributes(sub_vlayer_small.fields())
        newLayer.updateFields()
      
        # open the sublayer of the "big" (e.g., Conus) file
        fileID_big = "%s|layername=%s" % (geo_big_path, name,)
        # Create layer
        sub_vlayer_big = QgsVectorLayer(fileID_big, name, 'ogr')        
        # load features from "big" package (e.g., Conus) layer and convert to a list
        feats = sub_vlayer_big.getFeatures()
        featsList = list(feats)
        
        # Ascii file to save (integer) segment IDs of all "big" layers
        file = open('big_id_list_'+name+'.dat','w')        
        
        print ('Sub=layer: ',subLayer)
        
        i = 0
        big_id_list = []
        
        # get list of all features in the big model
        for feat in featsList:
            
            i+=1

            attrs = feat.attributes()
            
            if (name == 'hydrolocations'):
                id_index = 6
            else:
                id_index = 1

            if (i%1000 == 0):                
                print('i',i)
                print()
            
            if (attrs[id_index] != NULL):
            
                id = int(float(attrs[id_index].split('-')[-1]))
                file.write(str(id)+" \n")
                big_id_list.append(id)
        
                if (operator.contains(ID_net_list,id)):
                    newLayer_data.addFeature(feat)
                    newLayer.updateExtents()
                    if (i%1000 == 0):                
                        print('IN',id)
                        print()
    

        if (firstLayerWritten == 'False'):
            overWriteFlag = 'True'
            firstLayerWritten = 'True'
        else:
            overWriteFlag = 'False'

        group.insertChildNode(0, QgsLayerTreeLayer(newLayer))
        QgsProject.instance().addMapLayer(newLayer, False)  

        if (geoString != "None"):
            newLayer.renderer().symbol().setColor(QColor.fromRgb(renderColor[0],renderColor[1],renderColor[2]))

        processing.run("native:package", {'LAYERS':newLayer,'OUTPUT':outpath_build,'OVERWRITE':overWriteFlag,'SAVE_STYLES':True,'SAVE_METADATA':True})

        file.close()
    

    


