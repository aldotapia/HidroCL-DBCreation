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

# path with files
main_path = 'path/to/MOD10A2/' # path with modis data
polys_path = 'path/to/HidroCL_boundaries_sinu.shp'# polygons path

# path for Rscript and R files
rscript_path = "path/to/Rscript"
WeightedSumExtraction = "path/to/WeightedSumExtraction.R"
PreparingPackages = "path/to/PreparingPackages.R"

home = str(Path.home())
temporal_folder = os.path.join(home,'tempHidroCL')
stats_file = subprocess.call([rscript_path, "--vanilla", PreparingPackages])

if os.path.exists(temporal_folder):
    print(f'Checking temporary folder {temporal_folder}')
    temp_files = os.listdir(temporal_folder)
    print(f'Found {len(temp_files)} files')
else:
    os.makedirs(temporal_folder)
    print(f'Temporary folder {temporal_folder} not found, creating it')

raw_files = [value for value in os.listdir(main_path) if '.hdf' in value]
raw_ids = [value.split('.')[1] for value in raw_files]
if len(raw_files) >= 1:
    files_id = []
    for raw_id in raw_ids:
        if raw_id not in files_id:
            files_id.append(raw_id)
    files_id.sort()
    for file_id in files_id:
        r = re.compile('.*'+file_id+'.*')
        selected_files = list(filter(r.match, raw_files))
        if(len(selected_files)==9):
            start = time.time()
            raster_single = []
            for selected_file in selected_files:
                with rioxr.open_rasterio(os.path.join(main_path,selected_file)) as raster:
                    raster_single.append(getattr(raster,'Maximum_Snow_Extent'))
            raster_mosaic = merge_arrays(raster_single)
            raster_mosaic = (raster_mosaic.where(raster_mosaic == 200)/200).fillna(0).astype('int8')
            temporal_raster = os.path.join(temporal_folder,'snow_'+file_id+'.tif')
            raster_mosaic.rio.to_raster(temporal_raster, compress='LZW')
            subprocess.call([rscript_path,
                             #"--vanilla",
                             WeightedSumExtraction,
                             polys_path,
                             temporal_raster,
                             os.path.join(temporal_folder,'snow_'+file_id+'.csv')])
            os.remove(temporal_raster)    
            end = time.time()
            print(f'Time elapsed for {file_id} {str(round(end - start))} seconds')