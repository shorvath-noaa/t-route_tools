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

from troute.log_level_set import log_level_set
from troute.nhd_network import reverse_dict, extract_connections, replace_waterbodies_connections, reverse_network, reachable_network, reachable, split_at_waterbodies_and_junctions, split_at_junction, dfs_decomposition, headwaters, tailwaters
from troute.nhd_network_utilities_v02 import organize_independent_networks
from troute.nhd_io import read_config_file
from troute.AbstractRouting import MCOnly, MCwithDiffusive, MCwithDiffusiveNatlXSectionNonRefactored, MCwithDiffusiveNatlXSectionRefactored



# Import Geopackage (HYfeatures compatible) 
def read_geopkg(file_path, data_assimilation_parameters, waterbody_parameters):

    #
    # Arguments: 
    #
    #  file_path: path to geopackage (absolute path recommended)
    #
    #  data_assimilation_parameters: imported from troute .yaml configuration files
    #
    #  waterbody_parameters: imported from troute .yaml configuration files
    #
    # Returns:
    #
    #  flowpaths, lakes, network: vector layers of geopackage
    #
    #  table_dict: table of all network layers
    #

    # Establish which layers we will read. We'll always need the flowpath tables
    layers = ['flowpaths','flowpath_attributes']

    print ('file_path',file_path)

    # If waterbodies are being simulated, read lakes table
    if waterbody_parameters.get('break_network_at_waterbodies',False):
        layers.append('lakes')

    # If any DA is activated, read network table as well for gage information
    streamflow_nudging = data_assimilation_parameters.get('streamflow_da',{}).get('streamflow_nudging',False)
    usgs_da = data_assimilation_parameters.get('reservoir_da',{}).get('reservoir_persistence_usgs',False)
    usace_da = data_assimilation_parameters.get('reservoir_da',{}).get('reservoir_persistence_usace',False)
    rfc_da = waterbody_parameters.get('rfc',{}).get('reservoir_rfc_forecasts',False)
    if any([streamflow_nudging, usgs_da, usace_da, rfc_da]):
        layers.append('network')
    
    print('Layers to read in',layers)
    
    table_dict = {layers[i]: gpd.read_file (filename=file_path, layer=layers[i]) for i in range(len(layers))}
    flowpaths = pd.merge(table_dict.get('flowpaths'), table_dict.get('flowpath_attributes'), on='id')
    lakes = table_dict.get('lakes', pd.DataFrame())
    network = table_dict.get('network', pd.DataFrame())

    return flowpaths, lakes, network, table_dict



# Wrapper for Geopackage Import (HYfeatures compatible) 
def read_geo_file(supernetwork_parameters, waterbody_parameters, data_assimilation_parameters):

    #
    # Arguments: 
    #
    #  supernetwork_parameters: imported from troute .yaml configuration files
    #
    #  data_assimilation_parameters: imported from troute .yaml configuration files
    #
    #  waterbody_parameters: imported from troute .yaml configuration files
    #
    # Returns:
    #
    #  flowpaths, lakes, network: vector layers of geopackage
    #
    #  table_dict: table of all network layers
    #

    geo_file_path = supernetwork_parameters["geo_file_path"]
    
    file_type = Path(geo_file_path).suffix
    if(  file_type == '.gpkg' ):        
        flowpaths, lakes, network, table_dict = read_geopkg(geo_file_path, 
                                                data_assimilation_parameters,
                                                waterbody_parameters)

    else:

        raise RuntimeError("Unsupported file type: {}".format(file_type))
    
    return flowpaths, lakes, network, table_dict



# Make a copy of geopackage file in case of corruption by QGIS
def copy_geo_file(supernetwork_parameters, index):
       
    #
    # Arguments: 
    #
    #  supernetwork_parameters: imported from troute .yaml configuration files
    #
    # Returns:
    #
    #  geo_file_path_work: path to copied geopackage (absolute path recommended)
    #      
      
    geo_file_path = supernetwork_parameters["geo_file_path"]
    
    file_type = Path(geo_file_path).suffix

    if(  file_type == '.gpkg' ):        
        
        geo_file_path_work = (geo_file_path.split('.gpk')[-2]) \
                                +'_copy'+str(index)+'.gpkg'
                                
        shutil.copy2(geo_file_path,geo_file_path_work)

    else:

        raise RuntimeError("Unsupported file type: {}".format(file_type))
    
    return geo_file_path_work
    
    

# Aux. function: numeric IDs from flowpath vector layer
def numeric_id(flowpath):
    id = flowpath['id'].split('-')[-1]
    toid = flowpath['toid'].split('-')[-1]
    flowpath['id'] = int(float(id))
    flowpath['toid'] = int(float(toid))
    return flowpath    
    
    
# Preprocessing of network, adapted from tRoute, 
# BUT ALL ENTRIES LEFT INTACT (e.g., no renaming of attributes)
def preprocess_network_maintain_df(flowpaths, supernetwork_parameters):

    #
    # Arguments: 
    #
    #  flowpaths: vector layer from geopackage
    #
    #  supernetwork_parameters: imported from troute .yaml configuration files
    #
    # Returns:
    #
    #  Redundant returns at this point - will be reduced
    #      

    dataframe = flowpaths
    
    # Don't need the string prefix anymore, drop it
    #mask = ~ dataframe['toid'].str.startswith("tnx") 
    mask = ~ dataframe['toid'].str.startswith("tex")
    dataframe = dataframe.apply(numeric_id, axis=1)
        
    # handle segment IDs that are also waterbody IDs. The fix here adds a large value
    # to the segmetn IDs, creating new, unique IDs. Otherwise our connections dictionary
    # will get confused because there will be repeat IDs...
    duplicate_wb_segments = supernetwork_parameters.get("duplicate_wb_segments", None)
    duplicate_wb_id_offset = supernetwork_parameters.get("duplicate_wb_id_offset", 9.99e11)
    if duplicate_wb_segments:
        # update the values of the duplicate segment IDs
        fix_idx = dataframe.id.isin(set(duplicate_wb_segments))
        dataframe.loc[fix_idx,"id"] = (dataframe[fix_idx].id + duplicate_wb_id_offset).astype("int64")

    # make the flowpath linkage, ignore the terminal nexus
    flowpath_dict = dict(zip(dataframe.loc[mask].toid, dataframe.loc[mask].id))
        
    # **********  need to be included in flowpath_attributes  *************
    dataframe['alt'] = 1.0 #FIXME get the right value for this... 

    cols = supernetwork_parameters.get('columns',None)
        
    if cols:
        ##self._dataframe = self.dataframe[list(cols.values())]
        ##self._dataframe = self.dataframe.rename(columns=reverse_dict(cols))
        dataframe.set_index("id", inplace=True)
        dataframe = dataframe.sort_index()

    # numeric code used to indicate network terminal segments
    terminal_code = supernetwork_parameters.get("terminal_code", 0)

    # There can be an externally determined terminal code -- that's this first value
    terminal_codes = set()
    terminal_codes.add(terminal_code)
    # ... but there may also be off-domain nodes that are not explicitly identified
    # but which are terminal (i.e., off-domain) as a result of a mask or some other
    # an interior domain truncation that results in a
    # otherwise valid node value being pointed to, but which is masked out or
    # being intentionally separated into another domain.
    terminal_codes = terminal_codes | set(
        dataframe[~dataframe["toid"].isin(dataframe.index)]["toid"].values
    )

    #This is NEARLY redundant to the self.terminal_codes property, but in this case
    #we actually need the mapping of what is upstream of that terminal node as well.
    #we also only want terminals that actually exist based on definition, not user input
    terminal_mask = ~dataframe["toid"].isin(dataframe.index)
    terminal = dataframe.loc[ terminal_mask ]["toid"]
    upstream_terminal = dict()
    for key, value in terminal.items():
        upstream_terminal.setdefault(value, set()).add(key)

    # build connections dictionary
    connections = extract_connections( dataframe, "toid", terminal_codes=terminal_codes )
  
    return dataframe,connections,terminal_mask,terminal_codes,mask,flowpath_dict,upstream_terminal
        

# Preprocessing of network, adapted from tRoute, 
# IN LINE WITH tRoute (same substitutions)
def preprocess_network_Troute(flowpaths, supernetwork_parameters):

    #
    # Arguments: 
    #
    #  flowpaths: vector layer from geopackage
    #
    #  supernetwork_parameters: imported from troute .yaml configuration files
    #
    # Returns:
    #
    #  Redundant returns at this point - will be reduced
    #   

    dataframe = flowpaths
    
    # Don't need the string prefix anymore, drop it
    #mask = ~ dataframe['toid'].str.startswith("tnx") 
    mask = ~ dataframe['toid'].str.startswith("tex")
    dataframe = dataframe.apply(numeric_id, axis=1)
        
    # handle segment IDs that are also waterbody IDs. The fix here adds a large value
    # to the segmetn IDs, creating new, unique IDs. Otherwise our connections dictionary
    # will get confused because there will be repeat IDs...
    duplicate_wb_segments = supernetwork_parameters.get("duplicate_wb_segments", None)
    duplicate_wb_id_offset = supernetwork_parameters.get("duplicate_wb_id_offset", 9.99e11)
    if duplicate_wb_segments:
        # update the values of the duplicate segment IDs
        fix_idx = dataframe.id.isin(set(duplicate_wb_segments))
        dataframe.loc[fix_idx,"id"] = (dataframe[fix_idx].id + duplicate_wb_id_offset).astype("int64")

    # make the flowpath linkage, ignore the terminal nexus
    flowpath_dict = dict(zip(dataframe.loc[mask].toid, dataframe.loc[mask].id))
        
    # **********  need to be included in flowpath_attributes  *************
    dataframe['alt'] = 1.0 #FIXME get the right value for this... 

    cols = supernetwork_parameters.get('columns',None)
        
    if cols:
        dataframe = dataframe[list(cols.values())]
        
        # Rename parameter columns to standard names: from route-link names
        #        key: "link"
        #        downstream: "to"
        #        dx: "Length"
        #        n: "n"  # TODO: rename to `manningn`
        #        ncc: "nCC"  # TODO: rename to `mannningncc`
        #        s0: "So"  # TODO: rename to `bedslope`
        #        bw: "BtmWdth"  # TODO: rename to `bottomwidth`
        #        waterbody: "NHDWaterbodyComID"
        #        gages: "gages"
        #        tw: "TopWdth"  # TODO: rename to `topwidth`
        #        twcc: "TopWdthCC"  # TODO: rename to `topwidthcc`
        #        alt: "alt"
        #        musk: "MusK"
        #        musx: "MusX"
        #        cs: "ChSlp"  # TODO: rename to `sideslope`
        
        print('cols',cols)
        print('reverse_dict(cols)',reverse_dict(cols))
        
        dataframe = dataframe.rename(columns=reverse_dict(cols))
        dataframe.set_index("key", inplace=True)
        dataframe = dataframe.sort_index()

    # numeric code used to indicate network terminal segments
    terminal_code = supernetwork_parameters.get("terminal_code", 0)

    # There can be an externally determined terminal code -- that's this first value
    terminal_codes = set()
    terminal_codes.add(terminal_code)
    # ... but there may also be off-domain nodes that are not explicitly identified
    # but which are terminal (i.e., off-domain) as a result of a mask or some other
    # an interior domain truncation that results in a
    # otherwise valid node value being pointed to, but which is masked out or
    # being intentionally separated into another domain.
    terminal_codes = terminal_codes | set(
        dataframe[~dataframe["downstream"].isin(dataframe.index)]["downstream"].values
    )

    #This is NEARLY redundant to the self.terminal_codes property, but in this case
    #we actually need the mapping of what is upstream of that terminal node as well.
    #we also only want terminals that actually exist based on definition, not user input
    terminal_mask = ~dataframe["downstream"].isin(dataframe.index)
    terminal = dataframe.loc[ terminal_mask ]["downstream"]
    _upstream_terminal = dict()
    for key, value in terminal.items():
        _upstream_terminal.setdefault(value, set()).add(key)

    # build connections dictionary
    connections = extract_connections( dataframe, "downstream", terminal_codes=terminal_codes )
  
    upstream_terminal = False
  
    return dataframe,connections,terminal_mask,terminal_codes,mask,flowpath_dict,upstream_terminal
  

# Preprocessing of waterbodies, adapted from tRoute, 
# BUT ALL ENTRIES LEFT INTACT (e.g., no renaming of attributes)
# FUNCTION WAS PORTED TO QGIS ENVIRONMENT FOR DEVELOPMENT PURPOSES,
# ONLY FOR EXPERT USERS AT THIS STAGE
def preprocess_waterbodies_maintain_df(dataframe, lakes, connections):
    
    #
    # Arguments: 
    #
    #  No preprocessing for waterbodis performed at this point: function is 
    #  for including future features
    #       
    
    # If waterbodies are being simulated, create waterbody dataframes and dictionaries
    if not lakes.empty:
        waterbody_df = (
            lakes[['hl_link','ifd','LkArea','LkMxE','OrificeA',
                'OrificeC','OrificeE','WeirC','WeirE','WeirL']]
            .rename(columns={'hl_link': 'lake_id'})
            )
        waterbody_df['lake_id'] = waterbody_df.lake_id.astype(float).astype(int)
        waterbody_df = waterbody_df.set_index('lake_id').drop_duplicates().sort_index()
        
        # Commented-out "bandaid" function
        '''
        # Create wbody_conn dictionary:
        #FIXME temp solution for missing waterbody info in hydrofabric
        bandaid()
        ''' 
    
        wbody_conn = dataframe[['rl_NHDWaterbodyComID']].dropna()
        wbody_conn = (
            wbody_conn['rl_NHDWaterbodyComID']
            .str.split(',',expand=True)
            .reset_index()
            .melt(id_vars='id')
            .drop('variable', axis=1)
            .dropna()
            .astype(int)
            )
            
        waterbody_connections = (
            wbody_conn[wbody_conn['value'].isin(waterbody_df.index)]
            .set_index('id')['value']
            .to_dict()
            )
            
        dataframe = dataframe.drop('rl_NHDWaterbodyComID', axis=1)

        # if waterbodies are being simulated, adjust the connections graph so that 
        # waterbodies are collapsed to single nodes. Also, build a mapping between 
        # waterbody outlet segments and lake ids
        
        break_network_at_waterbodies = waterbody_parameters.get("break_network_at_waterbodies", False)
        if break_network_at_waterbodies:
            connections_waterbodies, link_lake_crosswalk = replace_waterbodies_connections(
                connections, waterbody_connections
            )
        else:
            link_lake_crosswalk = None
            
        waterbody_types_df = pd.DataFrame(
            data = 1, 
            index = waterbody_df.index, 
            columns = ['reservoir_type']).sort_index()
            
        waterbody_type_specified = True
           
          
    else:
        
        waterbody_df = pd.DataFrame()
        waterbody_types_df = pd.DataFrame()
        waterbody_connections = {}
        waterbody_type_specified = False
        link_lake_crosswalk = None
    

    return waterbody_df, waterbody_connections, wbody_conn, connections_waterbodies, waterbody_type_specified
  
  
