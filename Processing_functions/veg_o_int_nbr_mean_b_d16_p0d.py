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
database_nbr = hcl.veg_o_int_nbr_mean_b_d16_p0d  # CSV database
log_file = hcl.log_veg_o_int_nbr_mean # log file

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

# Check or create nbr database    
if os.path.exists(database_nbr):
    print('NBR database found, using ' + database_nbr)
    with open(database_nbr, 'r') as the_file:
        modis_in_db = [row[0] for row in csv.reader(the_file,delimiter=',')]
else:
    print('NBR database not found, creating it for ' + database_nbr)
    header_line = [str(s) for s in gauges]
    header_line.insert(0,'modis_id')
    header_line.insert(1,'date')
    header_line  = ','.join(header_line) + '\n'
    with open(database_nbr,'w') as the_file:
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
                nbr_single = []
                for selected_file in selected_files:
                    with rioxr.open_rasterio(os.path.join(main_path,selected_file)) as raster:
                        NIR = getattr(raster,'250m 16 days NIR reflectance')
                        MIR = getattr(raster,'250m 16 days MIR reflectance')
                        nbr_single.append((NIR-MIR)/(NIR+MIR))

                nbr_mosaic = merge_arrays(nbr_single)

                nbr_mosaic = nbr_mosaic.where((nbr_mosaic <= 1) & (nbr_mosaic >= -1))

                nbr_mosaic = nbr_mosaic.where(nbr_mosaic != nbr_mosaic.rio.nodata)

                nbr_mosaic = (nbr_mosaic * 1000).astype('int16')

                temporal_raster_nbr = os.path.join(temporal_folder,'nbr_'+file_id+'.tif')
                
                nbr_mosaic.rio.to_raster(temporal_raster_nbr, compress='LZW')
                
                result_nbr = os.path.join(temporal_folder,'nbr_'+file_id+'.csv')
                
                subprocess.call([rscript_path,
                                 "--vanilla",
                                 WeightedMeanExtraction,
                                 polys_path,
                                 temporal_raster_nbr,
                                 result_nbr])

                with open(result_nbr) as csv_file:
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
                    with open(database_nbr,'a') as the_file:
                                the_file.write(data_line)
                else:
                    print('Inconsistencies with gauge ids!')

                del data_line

                os.remove(result_nbr)
                os.remove(temporal_raster_nbr)
                end = time.time()
                time_dif = str(round(end - start))
                currenttime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                print(f'Time elapsed for {file_id}: {str(round(end - start))} seconds')
                with open(log_file, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Process time: {time_dif} s. Database: {database_nbr}. \n')
            else:
                print(f'Scene ID {file_id} does not have enough files')
                with open(log_file, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Error, not enough files. Files: {len(selected_files)} \n')        