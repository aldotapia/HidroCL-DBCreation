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

from pathlib import Path
import os
import csv
from math import ceil


def temp_folder():
    """Set temporary folder for files"""
    home = str(Path.home()) # get user's home path
    temporal_folder = os.path.join(home,'tempHidroCL')

    # Check or create temporal folder
    if os.path.exists(temporal_folder):
        print(f'Checking temporary folder {temporal_folder}')
        temp_files = os.listdir(temporal_folder)
        print(f'Found {len(temp_files)} files')
    else:
        os.makedirs(temporal_folder)
        print(f'Temporary folder {temporal_folder} not found, creating it')

    return temporal_folder


def database_check(db_path, id_name, catchment_names):
    """Check or create database. Then pull IDs"""

    if os.path.exists(db_path): # check if db exists
        print('Database found, using ' + db_path)
        with open(db_path, 'r') as the_file:
            ids_in_db = [row[0] for row in csv.reader(the_file,delimiter=',')]
    else: # create db
        print('Database not found, creating it for ' + db_path)
        header_line = [str(s) for s in catchment_names]
        header_line.insert(0,id_name)
        header_line.insert(1,'date')
        header_line  = ','.join(header_line) + '\n'
        with open(db_path,'w') as the_file:
            the_file.write(header_line)
        ids_in_db = [id_name]

    return(ids_in_db)

def write_line(db_path, result, catchment_names, file_id, file_date, nrow = 1):
    """Write line in dabatabase"""

    with open(result) as csv_file:
        csvreader = csv.reader(csv_file, delimiter=',')
        gauge_id_result = []
        value_result = []
        for row in csvreader:
                gauge_id_result.append(row[0])
                value_result.append(row[nrow])
    gauge_id_result = [int(value) for value in gauge_id_result[1:]]
    value_result = [str(ceil(float(value))) if value.replace('.','',1).isdigit() else 'NA' for value in value_result[1:] if value]
    
    if(catchment_names == gauge_id_result):
        value_result.insert(0,file_id)
        value_result.insert(1,file_date)
        data_line  = ','.join(value_result) + '\n'
        with open(db_path,'a') as the_file:
                    the_file.write(data_line)
    else:
        print('Inconsistencies with gauge ids!')