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

# path to files
main_path = 'path/to/MOD10A2/' # path with modis data
polys_path = 'path/to/HidroCL_boundaries_sinu.shp'# polygons path
database_path = "/Users/aldotapia/camels_test/MOD10A2/snow.csv" # CSV database

# path for Rscript and R files
rscript_path = "path/to/Rscript"
WeightedSumExtraction = "path/to/WeightedSumExtraction.R"
PreparingPackages = "path/to/PreparingPackages.R"

home = str(Path.home())
temporal_folder = os.path.join(home,'tempHidroCL')
stats_file = subprocess.call([rscript_path, "--vanilla", PreparingPackages])

# Check or create temporal folder
if os.path.exists(temporal_folder):
    print(f'Checking temporary folder {temporal_folder}')
    temp_files = os.listdir(temporal_folder)
    print(f'Found {len(temp_files)} files')
else:
    os.makedirs(temporal_folder)
    print(f'Temporary folder {temporal_folder} not found, creating it')

polys = gpd.read_file(polys_path)
gauges = polys.gauge_id.tolist()

# Check or create database    
if os.path.exists(database_path):
    print('Database found, using ' + database_path)
    with open(database_path, 'r') as the_file:
        modis_in_db = [row[0] for row in csv.reader(the_file,delimiter=',')]
else:
    print('Database not found, creating it for ' + database_path)
    header_line = [str(s) for s in gauges]
    header_line.insert(0,'modis_id')
    header_line.insert(1,'date')
    header_line  = ','.join(header_line) + '\n'
    with open(database_path,'w') as the_file:
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
                raster_single = []
                for selected_file in selected_files:
                    with rioxr.open_rasterio(os.path.join(main_path,selected_file)) as raster:
                        raster_single.append(getattr(raster,'Maximum_Snow_Extent'))
                raster_mosaic = merge_arrays(raster_single)
                raster_mosaic = (raster_mosaic.where(raster_mosaic == 200)/200).fillna(0).astype('int8')
                temporal_raster = os.path.join(temporal_folder,'snow_'+file_id+'.tif')
                raster_mosaic.rio.to_raster(temporal_raster, compress='LZW')
                result_file = os.path.join(temporal_folder,'snow_'+file_id+'.csv')
                subprocess.call([rscript_path,
                                 "--vanilla",
                                 WeightedSumExtraction,
                                 polys_path,
                                 temporal_raster,
                                 result_file])
                with open(result_file) as csv_file:
                    csvreader = csv.reader(csv_file, delimiter=',')
                    gauge_id_result = []
                    snow_sum_result = []    
                    for row in csvreader:  
                            gauge_id_result.append(row[0])
                            snow_sum_result.append(row[1])
                gauge_id_result = [int(value) for value in gauge_id_result[1:]]
                snow_sum_result = [str(round(float(value),2)) for value in snow_sum_result[1:]]
                
                if(gauges == gauge_id_result):
                    snow_sum_result.insert(0,file_id)
                    snow_sum_result.insert(1,file_date)
                    data_line  = ','.join(snow_sum_result) + '\n'
                    with open(database_path,'a') as the_file:
                                the_file.write(data_line)
                else:
                    print('Inconsistencies with gauge ids!')
                os.remove(temporal_raster)    
                end = time.time()
                print(f'Time elapsed for {file_id}: {str(round(end - start))} seconds')