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
import time

import hidrocl_paths

product_path = hidrocl_paths.persiann

ftp_server = 'persiann.eng.uci.edu'
ftp_path = 'CHRSdata/PERSIANN-CCS/daily'

while True:
    try:
        ftp = ftplib.FTP(ftp_server)
        ftp.login()
        ftp.cwd(ftp_path)
        break
    except:
        print('FTP connection failed. Trying again in 5 seconds...')
        time.sleep(5)
        continue

dir_list = []
ftp.dir(dir_list.append)
files_list = [value.split(' ')[-1] for value in dir_list if 'bin' in value]
files_list = [value for value in files_list if value.split('.gz')[0] not in os.listdir(product_path)]

while True:
    try:
        for file_name in files_list:
            print(f'Downloading {file_name}')
            wget.download(f'ftp://{ftp_server}/{ftp_path}/{file_name}', out=product_path)
            print(f'Unzipping {file_name}')
            with gzip.open(f'{product_path}/{file_name}', 'rb') as f_in:
                with open(f'{product_path}/{file_name.split(".gz")[0]}', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(f'{product_path}/{file_name}')
        break
    except:
        print('FTP connection failed. Trying again in 5 seconds...')
        ftp.close()
        time.sleep(5)
        ftp = ftplib.FTP(ftp_server)
        ftp.login()
        ftp.cwd(ftp_path)
        continue
ftp.close()