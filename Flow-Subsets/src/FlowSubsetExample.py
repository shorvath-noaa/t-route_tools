# This example extracts a number of connected flow networks
# that overlap with a smaller subset of a larger geopackage, ranked
# by the overlap with the small subset. A typical application is to first 
# subset a large geomodel (e.g., Conus or an ngen submodel) based
# on geography along (such as with a polygon cutout, and then extract 
# smaller geopackages from the geographic subset, associated with 
# flow networks that overlap with said geographic subset. In the present
# example, geopackages are created that are ranked by that overlap (starting
# with the greatest number of segments)

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
from joblib import delayed, Parallel

import windowing_QGIS
importlib.reload(windowing_QGIS)
from windowing_QGIS import *
from windowing_QGIS import copy_geo_file

import tRoute_QGIS_interface
reload(tRoute_QGIS_interface)
from tRoute_QGIS_interface import *

import networks_QGIS
reload(networks_QGIS)
from networks_QGIS import *

# define paths:
bigConfigFile = "C:/Users/JurgenZach/Documents/Geomodels/Flow-Subsets-CapeCode-2/models/bigConfigFile.yaml"
smallConfigFile = "C:/Users/JurgenZach/Documents/Geomodels/Flow-Subsets-CapeCode-2/models/smallConfigFile.yaml"

# define output file
outpath_build = "C:/Users/JurgenZach/Documents/Geomodels/Flow-Subsets-CapeCode-2/models/CapeCod_Flows.gpkg"
nameBase = outpath_build.split('.')[0]
nameExtension = outpath_build.split('.')[1]
nameCheck = nameBase+'_rank_1.'+nameExtension

# check if file already exists and continue only if it does not to avoid 
# overwrites that may potentially cost time to recreate files
if (os.path.isfile(nameCheck)):
    print('Geopackage with that name exists: ',outpath_build)
    print('Recommended: change file name or delete manually.')

else:
    # read configuration files corresponding to "big" (e.g., Conus) geopackage
    print ('Read big-package configuration file')
    log_parameters_BIG, preprocessing_parameters_BIG, \
        supernetwork_parameters_BIG, waterbody_parameters_BIG, \
        compute_parameters_BIG, forcing_parameters_BIG, \
        restart_parameters_BIG, hybrid_parameters_BIG, \
        output_parameters_BIG, parity_parameters_BIG, \
        data_assimilation_parameters_BIG = read_config_file(bigConfigFile)

    # read the key sub vector layers from the "big" geopackage
    print ('Read vector layers from big geopackage')
    flowpaths_BIG, lakes_BIG, network_BIG, table_dict_BIG = \
        read_geo_file(supernetwork_parameters_BIG, waterbody_parameters_BIG, \
        data_assimilation_parameters_BIG )

    # run network preprocessing for big geopackage, maintain original format - mainly to prepare flowpath networks
    print('Run preprocessing of big geopackage')
    dataframe_BIG,connections_BIG,terminal_mask_BIG,\
        terminal_codes_BIG,mask_BIG,flowpath_dict_BIG,upstream_terminal_BIG = \
        preprocess_network_Troute(flowpaths_BIG, supernetwork_parameters_BIG)

    # read configuration files corresponding to "small" (geographic subset) geopackage
    print ('Read small-package configuration file')
    log_parameters_SMALL, preprocessing_parameters_SMALL, \
        supernetwork_parameters_SMALL, waterbody_parameters_SMALL, \
        compute_parameters_SMALL, forcing_parameters_SMALL, \
        restart_parameters_SMALL, hybrid_parameters_SMALL, \
        output_parameters_SMALL, parity_parameters_SMALL, \
        data_assimilation_parameters_SMALL = read_config_file(smallConfigFile)

    # read the key sub vector layers from the "big" geopackage
    print ('Read vector layers from small geopackage')
    flowpaths_SMALL, lakes_SMALL, network_SMALL, table_dict_SMALL = \
        read_geo_file(supernetwork_parameters_SMALL, waterbody_parameters_SMALL\
        , data_assimilation_parameters_SMALL )

    # run network preprocessing for small geopackage, maintain original format
    print('Run preprocessing of big geopackage')
    dataframe_SMALL,connections_SMALL,terminal_mask_SMALL,terminal_codes_SMALL,\
        mask_SMALL,flowpath_dict_SMALL,upstream_terminal_SMALL = \
        preprocess_network_Troute(flowpaths_SMALL, supernetwork_parameters_SMALL)

    # extract sets of segments associated with connected flow networks contained in the "big" 
    # geopackage, that overlap with the "small" geopackage. 
    print('Extract sets of connected segments, ranked by overlap with small geopackage')
    # For quality control, a number of connected sets comprising the "big" geopackage can 
    # be extracted a number of times. If the results are not consistent, the "small" and "big"
    # geopackages may not be well-matched, for example, the "small" package may be poorly connected
    # to the "big" one
    draws = 5
    # The maximum number of connected networks to be extracted that overlap with the "small"
    # geopackage 
    ranking_SMALL = 10
    # The number of connected networks out of the "big" network to be tested for overlap 
    # with the "small" geopackage. 
    ranking_BIG = 50
    segBuckets, segBucketComplexity, segBucketOverlap = subsetConnections_Tx \
        (connections_BIG, connections_SMALL, draws, ranking_BIG, ranking_SMALL)

    roots=[]
    k = 0

    # construction of geopackages saved to files corresponding to each 
    # connected network that overlaps with the "small" geography-based subset
    for bucket in segBuckets:
        
        # construct name of output files (ranked by size of overlap)
        k+=1
        nameOut = nameBase+'_rank_'+str(k)+'.'+nameExtension
        layerNameAdd = ''

        # some QGS instance management
        root = QgsProject.instance().layerTreeRoot()        
        groupName="NetworkBuild_"+str(k)
        group = root.addGroup(groupName)
        
        # passing name and subsets of segments
        outpath_build = nameOut
        ID_net_list = list(segBuckets[k-1])
        
        # generate random render color for visibility
        redCode = random.randint(0,255)
        greenCode = random.randint(0,255)
        blueCode = random.randint(0,255)
        renderColor = [redCode, blueCode, greenCode] 

        # actually build it
        print('Build ranked geopackage file: ',k)    
        one_net_build (root, group, supernetwork_parameters_SMALL, \
            supernetwork_parameters_BIG, outpath_build, ID_net_list, \
            layerNameAdd, renderColor)
        roots.append(root)    

