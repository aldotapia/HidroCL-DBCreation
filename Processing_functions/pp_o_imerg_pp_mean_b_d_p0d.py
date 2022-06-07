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
import subprocess
from pathlib import Path
import geopandas as gpd
import re
from datetime import datetime
import time
import csv
import hidrocl_paths as hcl
from math import ceil

# path to files
main_path = hcl.imerg_path # path with modis data
polys_path = hcl.hidrocl_wgs84 # polygons path
database_path = hcl.pp_o_imerg_pp_mean_b_d_pod # CSV database
log_file = hcl.log_pp_o_imerg_pp_mean_b_d_pod # log file

# path for Rscript and R files
rscript_path = hcl.rscript_path
imergDailyMean = hcl.imergDailyMean
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

# Check or create database    
if os.path.exists(database_path):
    print('Database found, using ' + database_path)
    with open(database_path, 'r') as the_file:
        imerg_in_db = [row[0] for row in csv.reader(the_file,delimiter=',')]
else:
    print('Database not found, creating it for ' + database_path)
    header_line = [str(s) for s in gauges]
    header_line.insert(0,'imerg_id')
    header_line.insert(1,'date')
    header_line  = ','.join(header_line) + '\n'
    with open(database_path,'w') as the_file:
        the_file.write(header_line)
    imerg_in_db = ['imerg_id']
    
raw_files = [value for value in os.listdir(main_path) if '.HDF5' in value]
raw_ids = [value.split('.')[4].split('-')[0] for value in raw_files]

if len(raw_files) >= 1:
    files_id = []
    for raw_id in raw_ids:
        if raw_id not in files_id:
            files_id.append(raw_id)
    files_id.sort()
    for file_id in files_id:
        if(file_id in imerg_in_db):
            print(f'Scene ID {file_id} is already in database!')
        else:
            r = re.compile('.*'+file_id+'.*')
            selected_files = list(filter(r.match, raw_files))
            if(len(selected_files)==48):
                start = time.time()
                file_date = datetime.strptime(file_id, '%Y%m%d').strftime('%Y-%m-%d')
                result_file = os.path.join(temporal_folder,'imerg_dailymean_'+file_id+'.csv')
                subprocess.call([rscript_path,
                                 "--vanilla",
                                 imergDailyMean,
                                 polys_path,
                                 main_path,
                                 file_id,
                                 result_file])

                with open(result_file) as csv_file:
                    csvreader = csv.reader(csv_file, delimiter=',')
                    gauge_id_result = []
                    imerg_mean_result = []    
                    for row in csvreader:  
                            gauge_id_result.append(row[0])
                            imerg_mean_result.append(row[1])
                gauge_id_result = [int(value) for value in gauge_id_result[1:]]
                imerg_mean_result = [str(ceil(float(value))) for value in imerg_mean_result[1:]]
                
                if(gauges == gauge_id_result):
                    imerg_mean_result.insert(0,file_id)
                    imerg_mean_result.insert(1,file_date)
                    data_line  = ','.join(imerg_mean_result) + '\n'
                    with open(database_path,'a') as the_file:
                                the_file.write(data_line)
                else:
                    print('Inconsistencies with gauge ids!')

                os.remove(result_file)
                end = time.time()
                time_dif = str(round(end - start))
                currenttime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                print(f'Time elapsed for {file_id}: {str(round(end - start))} seconds')
                with open(log_file, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Process time: {time_dif} s. Databases: {database_path}. \n')
            else:
                print(f'Scene ID {file_id} does not have enough files')
                currenttime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                with open(log_file, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Error, not enough files. Files: {len(selected_files)} \n')        