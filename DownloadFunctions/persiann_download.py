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

import wget
import ftplib
import os
import gzip, shutil
import xarray as xr

import hidrocl_paths

product_path = hidrocl_paths.persiann

ftp_server = 'persiann.eng.uci.edu'
ftp_path = 'CHRSdata/PCCSCDR/daily'

ftp = ftplib.FTP(ftp_server)
ftp.login()
ftp.cwd(ftp_path)
dir_list = []
ftp.dir(dir_list.append)
files_list = [value.split(' ')[-1] for value in dir_list if 'bin' in value]
files_list = [value for value in files_list if value.split('.gz')[0] not in os.listdir(product_path)]

for file in files_list:
    file_link = os.path.join('ftp://',ftp_server,ftp_path,files_list[1])
    file_path = os.path.join(product_path,file)
    print(f'Downloading {file}')
    wget.download(file_link, file_path)
    with gzip.open(file_path, 'r') as f_in, open(file_path.split('.gz')[0], 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(file_path)
ftp.close()
