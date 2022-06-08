#!C:/Program Files/Python310/python.exe
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

import requests # http files
from bs4 import BeautifulSoup # for pulling html data
# library html5lib should be installed for BeautifulSoup function
import re # reguar expressions in python
import json # for json data management
import subprocess # needed for downloading modis data
# wget should be installed, not the python library
from functools import reduce # url path join
from os.path import exists # check if file exists
from os import listdir # for listing directories in a path
import argparse # for arguments into the function

def join_slash(a, b):
    return a.rstrip('/') + '/' + b.lstrip('/')
def urljoin(*args):
    return reduce(join_slash, args) if args else ''

token = '--header=Authorization: Bearer YWxkbDummyTokenDummyTokenDummyTokenDummyTokenDummyTokenDummyTokenDummyTokenDummyTokenDummyTokenDummyTokenDummyTokenDummyToken!'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="""
    Script for downloading MODIS data from LAAD DAAC
    (Level-1 and Atmosphere Archive & Distribution
    System Distributed Active Archive Center).
    Use CRTL + C for interrunpt with keyboard.

    Example:
    python get_modis.py vegetation /Users/testuser/Modis_download
    """, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('type',help = 'Product type',
        choices = ['vegetation','albedo','lulc','et0'])
    parser.add_argument('path',
        help = 'Path to check products and download')

    args = parser.parse_args()

    mod_opt = args.type
    path = args.path

    baseurl = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData'

    modis_products = {
        'vegetation':'MOD13Q1',
        'albedo':'MCD43A3',
        'lulc':'MCD12Q1',
        'et0':'MOD16A2'
    }

    modis_url = {
        'vegetation':baseurl+'/61/MOD13Q1',
        'albedo':baseurl+'/61/MCD43A3',
        'lulc':baseurl+'/6/MCD12Q1',
        'et0':baseurl+'/6/MOD16A2/'
    }

    grids = ['h13v14','h14v14','h12v13','h13v13','h11v12',
    'h12v12','h11v11','h12v11','h11v10']

    def get_modis(mod_opt, path):
        url = modis_url[mod_opt]
        response = requests.get(url)
        if response.ok:
            response_text = response.text
        else:
            return response.raise_for_status()
        soup = BeautifulSoup(response_text, 'html5lib')
        json_data = soup.find('script', text = re.compile('window.laadsArchiveFiles'))
        years_data = str(json_data)[str(json_data).find('window.laadsArchiveFiles = '):str(json_data).find(';')]
        years_data = json.loads(years_data.replace('window.laadsArchiveFiles = ', ''))
        years = []
        for i in years_data:
            years.append(i['name'])
        years_numeric = [int(x) for x in years]
        test_path = join_slash(path,modis_products[mod_opt])
        try:
            dirs = listdir(test_path)
            r = re.compile("2[0-9]")
            years_downloaded = list(filter(r.match,dirs))
            years_downloaded_numeric = [int(x) for x in years_downloaded]
            years_numeric = list(filter(lambda x: x >= max(years_downloaded_numeric), years_numeric))
            years = [str(x) for x in years_numeric]
        except:
            years_downloaded = []   
        for i in years:
            urlparts = [url,i]
            url2 = urljoin(*urlparts)
            response = requests.get(url2)
            if response.ok:
                response_text = response.text
            else:
                return response.raise_for_status()
            soup = BeautifulSoup(response_text, 'html5lib')
            json_data = soup.find('script', text = re.compile('window.laadsArchiveFiles'))
            weeks_data = str(json_data)[str(json_data).find('window.laadsArchiveFiles = '):str(json_data).find(';')]
            weeks_data = json.loads(weeks_data.replace('window.laadsArchiveFiles = ', ''))
            weeks = []
            for j in weeks_data:
                weeks.append(j['name'])
            weeks_numeric = [int(x) for x in weeks]
            test_path2 = join_slash(test_path,i)
            try:
                dirs = listdir(test_path2)
                r = re.compile("[0-9][0-9][0-9]")
                weeks_downloaded = list(filter(r.match,dirs))
                weeks_downloaded_numeric = [int(x) for x in weeks_downloaded]
                weeks_numeric = list(filter(lambda x: x >= max(weeks_downloaded_numeric), weeks_numeric))
                weeks = [str(x).zfill(3) for x in weeks_numeric]    
            except:
                weeks_downloaded = []
            for j in weeks:
                urlparts = [url2,j]
                url3 = urljoin(*urlparts)
                response = requests.get(url3)
                if response.ok:
                    response_text = response.text
                else:
                    return response.raise_for_status()
                soup = BeautifulSoup(response_text, 'html5lib')
                json_data = soup.find('script', text = re.compile('window.laadsArchiveFiles'))
                modis_data = str(json_data)[str(json_data).find('window.laadsArchiveFiles = '):str(json_data).find(';')]
                modis_data = json.loads(modis_data.replace('window.laadsArchiveFiles = ', ''))
                modis = []
                for l in modis_data:
                    modis.append(l['name'])
                    if any(substring in l['name'] for substring in grids):
                        path_temp = [path,modis_products[mod_opt], i, j, l['name']]
                        file_temp = urljoin(*path_temp)
                        urlparts = [url3,l['name']]
                        url4 = urljoin(*urlparts)
                        if exists(file_temp):
                            print('File '+file_temp+' is already downloaded')
                        else:
                            command = ['wget.exe','-e robots=off','-m','-np','-R','.html,.tmp','-nH','--cut-dirs=3',
                                              url4,token,'-P',
                                              path]
                            p = subprocess.Popen(command, stdout=subprocess.PIPE)
                            stdout, stderr = p.communicate()
                            p.wait()
            print('Download completed!')
    try:
        get_modis(mod_opt, path)
    except KeyboardInterrupt:
        print('Keyboard interruption')
