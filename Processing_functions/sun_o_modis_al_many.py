#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Aldo Tapia.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
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
from numpy import dtype
import rioxarray as rioxr
from rioxarray.merge import merge_arrays
import re
import subprocess
import time
from datetime import datetime
import geopandas as gpd

import hidrocl_paths as hcl
import hidrocl


# path to files
main_path = hcl.mcd43a3_path # path with modis data
polys_path = hcl.hidrocl_sinusoidal # north polygons path
database_albedomean = hcl.sun_o_modis_al_mean_b_d16_p0d  # CSV database
database_albedomedian = hcl.sun_o_modis_al_median_b_d16_p0d
database_albedop10 = hcl.sun_o_modis_al_p10_b_d16_p0d
database_albedop25 = hcl.sun_o_modis_al_p25_b_d16_p0d
database_albedop75 = hcl.sun_o_modis_al_p75_b_d16_p0d
database_albedop90 = hcl.sun_o_modis_al_p90_b_d16_p0d
log_mean = hcl.log_sun_o_modis_al_mean_b_d16_p0d  # CSV database
log_median = hcl.log_sun_o_modis_al_median_b_d16_p0d
log_p10 =  hcl.log_sun_o_modis_al_p10_b_d16_p0d
log_p25 =  hcl.log_sun_o_modis_al_p25_b_d16_p0d
log_p75 =  hcl.log_sun_o_modis_al_p75_b_d16_p0d
log_p90 =  hcl.log_sun_o_modis_al_p90_b_d16_p0d

# path for Rscript and R files
rscript_path = hcl.rscript_path
WeightedMeanExtraction = hcl.WeightedMeanExtraction
WeightedQuanExtraction = hcl.WeightedQuanExtraction
PreparingPackages = hcl.PreparingPackages

# check is R libraries are installed
stats_file = subprocess.call([rscript_path, "--vanilla", PreparingPackages])

# set temporal folder in user's home folder
temporal_folder = hidrocl.temp_folder()

polys = gpd.read_file(polys_path) # for getting gauge_id values
gauges = polys.gauge_id.tolist()

# Check or create database
hidrocl.database_check(db_path = database_albedomean,
    id_name = 'modis_id',
    catchment_names = gauges)

hidrocl.database_check(db_path = database_albedomedian,
    id_name = 'modis_id',
    catchment_names = gauges)    

hidrocl.database_check(db_path = database_albedop10,
    id_name = 'modis_id',
    catchment_names = gauges)

hidrocl.database_check(db_path = database_albedop25,
    id_name = 'modis_id',
    catchment_names = gauges)

hidrocl.database_check(db_path = database_albedop75,
    id_name = 'modis_id',
    catchment_names = gauges)

modis_in_db = hidrocl.database_check(db_path = database_albedop90,
    id_name = 'modis_id',
    catchment_names = gauges)
    
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
                albedo_single = []
                for selected_file in selected_files:
                    with rioxr.open_rasterio(os.path.join(main_path,selected_file),masked=True) as raster:
                        albedo_single.append(getattr(raster,'Albedo_BSA_vis'))

                albedo_mosaic = merge_arrays(albedo_single)

                albedo_mosaic = albedo_mosaic * 0.1

                temporal_raster_ndvi = os.path.join(temporal_folder,'albedo_'+file_id+'.tif')
                
                albedo_mosaic.rio.to_raster(temporal_raster_ndvi, compress='LZW')
                
                result_albedo = os.path.join(temporal_folder,'albedo_'+file_id+'.csv')
                
                subprocess.call([rscript_path,
                                 "--vanilla",
                                 WeightedMeanExtraction,
                                 polys_path,
                                 temporal_raster_ndvi,
                                 result_albedo])

                hidrocl.write_line(db_path = database_albedomean,
                    result = result_albedo,
                    catchment_names = gauges,
                    file_id = file_id,
                    file_date = file_date)

                os.remove(result_albedo)

                result_albedo = os.path.join(temporal_folder,'albedo_'+file_id+'.csv')
                
                subprocess.call([rscript_path,
                                 "--vanilla",
                                 WeightedQuanExtraction,
                                 polys_path,
                                 temporal_raster_ndvi,
                                 result_albedo])

                hidrocl.write_line(db_path = database_albedop10,
                    result = result_albedo,
                    catchment_names = gauges,
                    file_id = file_id,
                    file_date = file_date,
                    nrow = 1)

                hidrocl.write_line(db_path = database_albedop25,
                    result = result_albedo,
                    catchment_names = gauges,
                    file_id = file_id,
                    file_date = file_date,
                    nrow = 2)
                    
                hidrocl.write_line(db_path = database_albedomedian,
                    result = result_albedo,
                    catchment_names = gauges,
                    file_id = file_id,
                    file_date = file_date,
                    nrow = 3)

                hidrocl.write_line(db_path = database_albedop75,
                    result = result_albedo,
                    catchment_names = gauges,
                    file_id = file_id,
                    file_date = file_date,
                    nrow = 4)

                hidrocl.write_line(db_path = database_albedop90,
                    result = result_albedo,
                    catchment_names = gauges,
                    file_id = file_id,
                    file_date = file_date,
                    nrow = 5)

                os.remove(result_albedo)
                os.remove(temporal_raster_ndvi)
                end = time.time()
                time_dif = str(round(end - start))
                currenttime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                print(f'Time elapsed for {file_id}: {str(round(end - start))} seconds')
                with open(log_mean, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Process time: {time_dif} s. Database: {database_albedomean}. \n')
                with open(log_median, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Process time: {time_dif} s. Database: {database_albedomedian}. \n')
                with open(log_p10, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Process time: {time_dif} s. Database: {database_albedop10}. \n')
                with open(log_p25, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Process time: {time_dif} s. Database: {database_albedop25}. \n')
                with open(log_p75, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Process time: {time_dif} s. Database: {database_albedop75}. \n')
                with open(log_p90, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Process time: {time_dif} s. Database: {database_albedop90}. \n')
            else:
                print(f'Scene ID {file_id} does not have enough files')
                currenttime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                with open(log_mean, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Error, not enough files. Files: {len(selected_files)} \n')
                with open(log_median, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Error, not enough files. Files: {len(selected_files)} \n')
                with open(log_p10, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Error, not enough files. Files: {len(selected_files)} \n')
                with open(log_p25, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Error, not enough files. Files: {len(selected_files)} \n')
                with open(log_p75, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Error, not enough files. Files: {len(selected_files)} \n')
                with open(log_p90, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Error, not enough files. Files: {len(selected_files)} \n')