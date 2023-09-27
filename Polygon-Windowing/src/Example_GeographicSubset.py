# This example subsets part of a geopackage in terms of geography
# alone

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
#from windowing6 import copy_geo_file

import tRoute_QGIS_interface
reload(tRoute_QGIS_interface)
from tRoute_QGIS_interface import *


# define "Cape Cod" rectangle
pointList = [(1789078, 2578907), (2057179, 2578907), (2057179, 2373471), (1789078, 2373471)]
# file name for polygon-only
outpathPolygon = "C:/Users/JurgenZach/Documents/Geomodels/Polygon-Windowing/models/CapeCod1_Polygon.gpkg"
# polygon name
polygonName = 'CapeCodRectangle'
cookie_cutter = define_polygon(pointList, outpathPolygon, polygonName)

# read configuration files corresponding to "big" (Conus) geopackage
bigConfigFile = "C:/Users/JurgenZach/Documents/Geomodels/Polygon-Windowing/src/ConusPolygonWindowing_CapeCod.yaml"
log_parameters_BIG, preprocessing_parameters_BIG, supernetwork_parameters_BIG, waterbody_parameters_BIG, compute_parameters_BIG, forcing_parameters_BIG, restart_parameters_BIG, hybrid_parameters_BIG, output_parameters_BIG, parity_parameters_BIG, data_assimilation_parameters_BIG = read_config_file(bigConfigFile)

# define group
groupName="WindowedLayers"
root = QgsProject.instance().layerTreeRoot()
group = root.addGroup(groupName)

# run windowing of sub-layers with geometry
layer_with_IDs = 'flowpaths'
layerNameAdd = ''
outpathGeopackage = "C:/Users/JurgenZach/Documents/Geomodels/Polygon-Windowing/models/CapeCod1_Package.gpkg"
subLayers_without_geography, subLayers_with_geography, cookie_by_ID = cut_cookie (root, group, cookie_cutter, polygonName, supernetwork_parameters_BIG, outpathGeopackage, layer_with_IDs, layerNameAdd)

# run windowing of sub-layers without geometry
window_NoGeo (root, group, supernetwork_parameters_BIG, outpathGeopackage, cookie_by_ID, subLayers_without_geography, layerNameAdd)
