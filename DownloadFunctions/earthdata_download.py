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

from earthdata import Auth, DataGranules, DataCollections, Store
from datetime import date
import os
from datetime import datetime
import argparse

grids = ['h13v14','h14v14','h12v13','h13v13','h11v12',
    'h12v12','h11v11','h12v11','h11v10']

earthdata_products = {
    'vegetation':'MOD13Q1',
    'albedo':'MCD43A3',
    'lulc':'MCD12Q1',
    'et0':'MOD16A2',
    'snow':'MOD10A2',
    'precipitation':'GPM_3IMERGHH',
    'landdata':'GLDAS_NOAH025_3H',
}

earthdata_platform = {
    'vegetation':'modis',
    'albedo':'modis',
    'lulc':'modis',
    'et0':'modis',
    'snow':'modis',
    'precipitation':'mixed',
    'landdata':'model',
}

earthdata_version = {
    'vegetation':'061',
    'albedo':'061',
    'lulc':'006',
    'et0':'061',
    'snow':'61',
    'precipitation':'06',
    'landdata':'2.1',
}

earthdata_file_extension = {
    'vegetation':'.hdf',
    'albedo':'.hdf',
    'lulc':'.hdf',
    'et0':'.hdf',
    'snow':'.hdf',
    'precipitation':'.hdf5',
    'landdata':'.nc4',
}

# https://github.com/podaac/data-subscriber#step-2-setup-your-earthdata-login
# about netrc (.netrc):
# machine urs.earthdata.nasa.gov
#     login <your username>
#     password <your password>

auth = Auth().login(strategy="netrc")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="""
    Script for downloading based on earthdata library,
    (for searching and accessing NASA datasets
    using NASA CMR and EDL APIs)
    Use CRTL + C for interrunpt with keyboard.
    """, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--type',help = 'Product type',
        choices = ['vegetation','albedo','lulc','et0',
        'snow','precipitation','landdata'])
    parser.add_argument('--path',
        help = 'Path for downloaded products. Do not include product name')
    parser.add_argument('--startdate',
        help = 'Starting date in YYYY-MM-DD format',
        default = None)

    args = parser.parse_args()

    ed_opt = args.type
    path = args.path
    start_date = args.startdate

    product = earthdata_products.get(ed_opt)
    platform = earthdata_platform.get(ed_opt)
    version = earthdata_version.get(ed_opt)
    file_extension = earthdata_file_extension.get(ed_opt)

    database_path = os.path.join(path,product)
    end_date = date.today().strftime('%Y-%m-%d')

    if os.path.exists(database_path):
        print(f'Checking folder {database_path}')
        files = os.listdir(database_path)
        if(ed_opt in ['landdata','precipitation']):
            dates = [datetime.strptime(value.lower().split('.')[1], 'a%Y%m%d') for value in files if file_extension in value.lower()]
        else:    
            dates = [datetime.strptime(value.lower().split('.')[1], 'a%Y%j') for value in files if file_extension in value.lower()]
        if len(dates) >= 1:
            recent_date = max(dates).strftime('%Y-%m-%d')
            print(f'Found {len(dates)} files. The most recent date is {recent_date}')
        else:
            recent_date = '2000-01-01'
            print(f'Found {len(dates)} files.')
        if start_date is None:
            start_date = recent_date
        print(f'Setting period from {start_date} to {end_date}')
    else:
        print(f'Folder {database_path} not found, creating it')
        os.makedirs(database_path)
        start_date = '2000-01-01'

    Query = (DataGranules(auth)
        .short_name(product)
        .bounding_box(-73.73,-55.01,-67.05,-17.63)
        .downloadable(True).version(version)
        .temporal(start_date,end_date))
    granules = Query.get_all()

    data_links = [granule.data_links(access="onprem") for granule in granules]
    if platform == 'modis':
        download_links = [value[0] for value in data_links if any(substring in value[0] for substring in grids)]
    else:
        download_links = [value[0] for value in data_links]

    # because for nsidc many threads fail
    if(ed_opt in ['snow','landdata','precipitation']):
        threads = 1
    else:
        threads = 8

    store = Store(auth)

    while True:
        try:
            store.get(download_links, database_path, threads = threads)
        except:
            continue
        break