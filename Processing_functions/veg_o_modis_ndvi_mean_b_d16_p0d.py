#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Aldo Tapia.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, see <http://www.gnu.org/licenses/>.
#
# Check https://nsidc.github.io/earthdata/ for earthdata python
# library installation

import os
import rioxarray as rioxr
from rioxarray.merge import merge_arrays
import re
from pathlib import Path
import subprocess
import time
from datetime import datetime
import csv
import geopandas as gpd
import hidrocl_paths as hcl
from math import ceil

# path to files
main_path = hcl.mod13q1_path # path with modis data
polys_path = hcl.hidrocl_sinusoidal # north polygons path
database_ndvi = hcl.veg_o_modis_ndvi_mean_b_d16_p0d  # CSV database
log_file = hcl.log_veg_o_modis_ndvi_mean # log file

# path for Rscript and R files
rscript_path = hcl.rscript_path
WeightedMeanExtraction = hcl.WeightedMeanExtraction
PreparingPackages = hcl.PreparingPackages

# set up
home = str(Path.home())
temporal_folder = os.path.join(home,'tempHidroCL')

# check is R libraries are installed
stats_file = subprocess.call([rscript_path, "--vanilla", PreparingPackages])

# Check or create temporal folder
if os.path.exists(temporal_folder):
    print(f'Checking temporary folder {temporal_folder}')
    temp_files = os.listdir(temporal_folder)
    print(f'Found {len(temp_files)} files')
else:
    os.makedirs(temporal_folder)
    print(f'Temporary folder {temporal_folder} not found, creating it')

polys = gpd.read_file(polys_path) # for getting gauge_id values
gauges = polys.gauge_id.tolist()

# Check or create ndvi database    
if os.path.exists(database_ndvi):
    print('NDVI database found, using ' + database_ndvi)
    with open(database_ndvi, 'r') as the_file:
        modis_in_db = [row[0] for row in csv.reader(the_file,delimiter=',')]
else:
    print('NDVI database not found, creating it for ' + database_ndvi)
    header_line = [str(s) for s in gauges]
    header_line.insert(0,'modis_id')
    header_line.insert(1,'date')
    header_line  = ','.join(header_line) + '\n'
    with open(database_ndvi,'w') as the_file:
        the_file.write(header_line)
    modis_in_db = ['modis_id']
    
raw_files = [value for value in os.listdir(main_path) if '.hdf' in value]
raw_ids = [value.split('.')[1] for value in raw_files]
if len(raw_files) >= 1:
    files_id = []
    for raw_id in raw_ids:
        if raw_id not in files_id:
            files_id.append(raw_id)
    files_id.sort()
    for file_id in files_id:
        if(file_id in modis_in_db):
            print(f'Scene ID {file_id} is already in database!')
        else:    
            r = re.compile('.*'+file_id+'.*')
            selected_files = list(filter(r.match, raw_files))
            if(len(selected_files)==9):
                start = time.time()
                file_date = datetime.strptime(file_id, 'A%Y%j').strftime('%Y-%m-%d')
                ndvi_single = []
                for selected_file in selected_files:
                    with rioxr.open_rasterio(os.path.join(main_path,selected_file)) as raster:
                        ndvi_single.append(getattr(raster,'250m 16 days NDVI'))

                ndvi_mosaic = merge_arrays(ndvi_single)

                ndvi_mosaic = (ndvi_mosaic * 0.1).astype('int16')

                temporal_raster_ndvi = os.path.join(temporal_folder,'ndvi_'+file_id+'.tif')
                
                ndvi_mosaic.rio.to_raster(temporal_raster_ndvi, compress='LZW')
                
                result_ndvi = os.path.join(temporal_folder,'ndvi_'+file_id+'.csv')
                
                subprocess.call([rscript_path,
                                 "--vanilla",
                                 WeightedMeanExtraction,
                                 polys_path,
                                 temporal_raster_ndvi,
                                 result_ndvi])

                with open(result_ndvi) as csv_file:
                    csvreader = csv.reader(csv_file, delimiter=',')
                    gauge_id_result = []
                    index_mean_result = []    
                    for row in csvreader:  
                            gauge_id_result.append(row[0])
                            index_mean_result.append(row[1])
                gauge_id_result = [int(value) for value in gauge_id_result[1:]]
                index_mean_result = [str(ceil(float(value))) for value in index_mean_result[1:]]
                
                if(gauges == gauge_id_result):
                    index_mean_result.insert(0,file_id)
                    index_mean_result.insert(1,file_date)
                    data_line  = ','.join(index_mean_result) + '\n'
                    with open(database_ndvi,'a') as the_file:
                                the_file.write(data_line)
                else:
                    print('Inconsistencies with gauge ids!')

                del data_line

                os.remove(result_ndvi)
                os.remove(temporal_raster_ndvi)
                end = time.time()
                time_dif = str(round(end - start))
                currenttime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                print(f'Time elapsed for {file_id}: {str(round(end - start))} seconds')
                with open(log_file, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Process time: {time_dif} s. Database: {database_ndvi}. \n')
            else:
                print(f'Scene ID {file_id} does not have enough files')
                with open(log_file, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Error, not enough files. Files: {len(selected_files)} \n')        