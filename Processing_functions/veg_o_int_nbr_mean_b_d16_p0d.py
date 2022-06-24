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
main_path = hcl.mod13q1_path # path with modis data
polys_path = hcl.hidrocl_sinusoidal # north polygons path
database_nbr = hcl.veg_o_int_nbr_mean_b_d16_p0d  # CSV database
log_file = hcl.log_veg_o_int_nbr_mean # log file

# path for Rscript and R files
rscript_path = hcl.rscript_path
WeightedMeanExtraction = hcl.WeightedMeanExtraction
PreparingPackages = hcl.PreparingPackages

# check is R libraries are installed
stats_file = subprocess.call([rscript_path, "--vanilla", PreparingPackages])

# set temporal folder in user's home folder
temporal_folder = hidrocl.temp_folder()

polys = gpd.read_file(polys_path) # for getting gauge_id values
gauges = polys.gauge_id.tolist()

# Check or create nbr database
modis_in_db = hidrocl.database_check(db_path = database_nbr,
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
                nbr_single = []
                for selected_file in selected_files:
                    with rioxr.open_rasterio(os.path.join(main_path,selected_file), masked=True) as raster:
                        NIR = getattr(raster,'250m 16 days NIR reflectance')
                        MIR = getattr(raster,'250m 16 days MIR reflectance')
                        nbr_single.append((NIR-MIR)/(NIR+MIR))

                nbr_mosaic = merge_arrays(nbr_single)
                nbr_mosaic = nbr_mosaic.where((nbr_mosaic <= 1) & (nbr_mosaic >= -1))
                nbr_mosaic = nbr_mosaic.where(nbr_mosaic != nbr_mosaic.rio.nodata)
                nbr_mosaic = nbr_mosaic * 1000

                temporal_raster_nbr = os.path.join(temporal_folder,'nbr_'+file_id+'.tif')
                nbr_mosaic.rio.to_raster(temporal_raster_nbr, compress='LZW')
                result_nbr = os.path.join(temporal_folder,'nbr_'+file_id+'.csv')
                
                subprocess.call([rscript_path,
                                 "--vanilla",
                                 WeightedMeanExtraction,
                                 polys_path,
                                 temporal_raster_nbr,
                                 result_nbr])

                hidrocl.write_line(db_path = database_nbr,
                    result = result_nbr,
                    catchment_names = gauges,
                    file_id = file_id,
                    file_date = file_date)

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
                currenttime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                with open(log_file, 'a') as txt_file:
                    txt_file.write(f'ID {file_id}. Date: {currenttime}. Error, not enough files. Files: {len(selected_files)} \n')        