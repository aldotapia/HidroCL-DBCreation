'''
this is hidrocl abstract base classes for testing purposes
what does an hidroclabc class do?
The class should:
 - is necessary to download data or can I mess things up? Maybe split classes for processing and download
 - create a database / Done
 - load database / Done
 - check current observations in the database / Done
 - process data stored on nas / Done
 - write data to database / Done
 - Products included:
   - MOD13Q1
 - to do:
    - Database maintener (delete wrong observations)
'''

# import collections
import re
import os
import gc
import csv
import time
import subprocess
import pandas as pd
from math import ceil
from pathlib import Path
import rioxarray as rioxr
from datetime import datetime
from rioxarray.merge import merge_arrays

import hidrocl_paths as hcl

#hcl_object = collections.namedtuple('HCLObs',['name','date','value'])

class HidroCLVariable:
    '''A class to hold information about a hidrocl variable

    Parameters:
    name (str): name of the variable
    database (str): path to the database'''

    def __init__(self,name,database):
        self.name = name
        self.database = database
        self.indatabase =  ''
        self.observations =  None
        self.catchment_names = None
        self.checkdatabase()

    def __repr__(self):
        return f'Variable: {self.name}. Records: {len(self.indatabase)}'

    def __str__(self):
        return f'''
Variable {self.name}.
Records: {len(self.indatabase)}.
Database path: {self.database}.
        '''

    def add_catchment_names(self,catchment_names_list):
        '''add catchment names to the variable'''
        if(self.catchment_names is None):
            self.catchment_names = catchment_names_list
            print('Catchment names added. I recommend you to check the database')
        else:
            print('Catchment names already added!')

    def checkdatabase(self):
        '''check database'''
        if os.path.exists(self.database): # check if db exists
            print('Database found, using ' + self.database)
            self.observations = pd.read_csv(self.database)
            self.observations.date=pd.to_datetime(self.observations.date, format = '%Y-%m-%d')
            self.observations.set_index(['date'], inplace = True)
            self.indatabase = self.observations[self.observations.columns[0]].values.tolist()
            self.catchment_names = self.observations.columns[1:].tolist()
            print('Observations and catchment names added!')
        else: # create db
            if self.catchment_names is None:
                print('Database not found. Please, add catchment names before creating the database')
            else:    
                print('Database not found, creating it for ' + self.database)
                header_line = [str(s) for s in self.catchment_names]
                header_line.insert(0,'name_id')
                header_line.insert(1,'date')
                header_line  = ','.join(header_line) + '\n'
                with open(self.database,'w') as the_file:
                    the_file.write(header_line)
                print('Database created!')

def mosaic_raster(raster_list,layer):
    '''function to mosaic files with rioxarray library'''
    raster_single = []

    for raster in raster_list:
        with rioxr.open_rasterio(raster, masked = True) as src:
            raster_single.append(getattr(src, layer))

    raster_mosaic = merge_arrays(raster_single)
    return raster_mosaic

def mosaic_nd_raster(raster_list,layer1, layer2):
    '''function to compute normalized difference and mosaic files with rioxarray library'''
    raster_single = []

    for raster in raster_list:
        with rioxr.open_rasterio(raster, masked = True) as src:
            lyr1 = getattr(src,layer1)
            lyr2 = getattr(src,layer2)
            nd = 1000 * (lyr1 - lyr2) / (lyr1 + lyr2)
            nd.rio.set_nodata(-32768)
            raster_single.append(nd)
    raster_mosaic = merge_arrays(raster_single)
    raster_mosaic = raster_mosaic.where((raster_mosaic <= 1000) & (raster_mosaic >= -1000))
    raster_mosaic = raster_mosaic.where(raster_mosaic != raster_mosaic.rio.nodata)
    return raster_mosaic

def temp_folder():
    '''set temporary folder for paths'''
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

def run_WeightedMeanExtraction(temporal_raster,result_file):
    '''run WeightedMeanExtraction'''
    subprocess.call([hcl.rscript_path,
                     "--vanilla",
                     hcl.WeightedMeanExtraction,
                     hcl.hidrocl_sinusoidal,
                     temporal_raster,
                     result_file])

def run_WeightedSumExtractionNorth(temporal_raster,result_file):
    '''run WeightedSumExtraction for north face'''
    subprocess.call([hcl.rscript_path,
                     "--vanilla",
                     hcl.WeightedSumExtraction,
                     hcl.hidrocl_north,
                     temporal_raster,
                     result_file])

def run_WeightedSumExtractionSouth(temporal_raster,result_file):
    '''run WeightedSumExtraction for South face'''
    subprocess.call([hcl.rscript_path,
                     "--vanilla",
                     hcl.WeightedSumExtraction,
                     hcl.hidrocl_south,
                     temporal_raster,
                     result_file])


def write_line(database, result, catchment_names, file_id, file_date, nrow = 1):
    """Write line in dabatabase"""
    with open(result) as csv_file:
        csvreader = csv.reader(csv_file, delimiter=',')
        gauge_id_result = []
        value_result = []
        for row in csvreader:
                gauge_id_result.append(row[0])
                value_result.append(row[nrow])
    gauge_id_result = [value for value in gauge_id_result[1:]]
    value_result = [str(ceil(float(value))) if value.replace('.','',1).isdigit() else 'NA' for value in value_result[1:] if value]

    if(catchment_names == gauge_id_result):
        value_result.insert(0,file_id)
        value_result.insert(1,file_date)
        data_line  = ','.join(value_result) + '\n'
        with open(database,'a') as the_file:
                    the_file.write(data_line)
    else:
        print('Inconsistencies with gauge ids!')

def write_log(log_file,file_id,currenttime,time_dif,database):
    '''write log file'''
    with open(log_file, 'a') as txt_file:
        txt_file.write(f'ID {file_id}. Date: {currenttime}. Process time: {time_dif} s. Database: {database}. \n')

def write_log_double(log_file,file_id,currenttime,time_dif,database1,database2):
    '''write log file for two databases'''
    with open(log_file, 'a') as txt_file:
        txt_file.write(f'ID {file_id}. Date: {currenttime}. Process time: {time_dif} s. Databases: {database1}/{database2}. \n')

class mod13q1extractor:
    '''class to extract MOD13Q1 to hidrocl variables

    Parameters:
    ndvi (HidroCLVariable): ndvi variable
    evi (HidroCLVariable): evi variable
    nbr (HidroCLVariable): nbr variable'''

    def __init__(self,ndvi,evi,nbr):
        if isinstance(ndvi,HidroCLVariable) & isinstance(evi,HidroCLVariable) & isinstance(nbr,HidroCLVariable):
            self.ndvi = ndvi
            self.evi = evi
            self.nbr = nbr
            self.productname = 'MODIS MOD13Q1 Version 0.61'
            self.productpath = hcl.mod13q1_path
            self.common_elements = self.compare_indatabase()
            self.product_files = self.read_product_files()
            self.product_ids = self.get_product_ids()
            self.all_scenes = self.check_product_files()
            self.scenes_occurrences = self.count_scenes_occurrences()
            self.incomplete_scenes = self.get_incomplete_scenes()
            self.overpopulated_scenes = self.get_overpopulated_scenes()
            self.complete_scenes = self.get_complete_scenes()
            self.scenes_to_process = self.get_scenes_out_of_db()
        else:
            raise TypeError('ndvi, evi and nbr must be HidroCLVariable objects')

    def __repr__(self):
        return f'Class to extract {self.productname}'
    def __str__(self):
        return f'''
Product: {self.productname}

NDVI records: {len(self.ndvi.indatabase)}.
NDVI database path: {self.ndvi.database}

EVI records: {len(self.evi.indatabase)}.
EVI database path: {self.evi.database}

NBR records: {len(self.nbr.indatabase)}.
NBR database path: {self.nbr.database}
        '''

    def order_indatabase(self):
        '''order indatabase attributes'''
        if len(self.ndvi.indatabase) > 0:
            self.ndvi.indatabase.sort()
        if len(self.evi.indatabase) > 0:
            self.evi.indatabase.sort()
        if len(self.nbr.indatabase) > 0:
            self.nbr.indatabase.sort()

    def compare_indatabase(self):
        '''compare indatabase and return elements that are equal'''
        self.order_indatabase()
        if len(self.ndvi.indatabase) > 0 or len(self.evi.indatabase) > 0 or len(self.nbr.indatabase) > 0:
            common_elements = list(set(self.ndvi.indatabase) & set(self.evi.indatabase) & set(self.nbr.indatabase))
        else:
            common_elements = []
        return common_elements

    def read_product_files(self):
        '''read product files'''
        return [value for value in os.listdir(self.productpath) if '.hdf' in value]

    def get_product_ids(self):
        '''get product ids'''
        product_ids = [value.split('.')[1] for value in self.product_files]
        return product_ids

    def check_product_files(self):
        '''extract product ids from product files'''
        files_id = []
        for product_id in self.product_ids:
            if product_id not in files_id:
                files_id.append(product_id)
        files_id.sort()
        return files_id

    def count_scenes_occurrences(self):
        '''count self.all_scenes in self.product_ids returning a dictionary'''
        count_scenes = {}
        for scene in self.all_scenes:
            count_scenes[scene] = self.product_ids.count(scene)
        return count_scenes

    def get_overpopulated_scenes(self):
        '''get scenes with more than 9 items from self.scenes_occurrences'''
        overpopulated_scenes = []
        for scene,occurrences in self.scenes_occurrences.items():
            if occurrences > 9:
                overpopulated_scenes.append(scene)
        return overpopulated_scenes

    def get_incomplete_scenes(self):
        '''get scenes with less than 9 items from self.scenes_occurrences'''
        incomplete_scenes = []
        for scene,occurrences in self.scenes_occurrences.items():
            if occurrences < 9:
                incomplete_scenes.append(scene)
        return incomplete_scenes

    def get_complete_scenes(self):
        '''get scenes with 9 items from self.scenes_occurrences'''
        complete_scenes = []
        for scene,occurrences in self.scenes_occurrences.items():
            if occurrences == 9:
                complete_scenes.append(scene)
        return complete_scenes

    def get_scenes_out_of_db(self):
        '''compare
        self.ndvi.indatabase and
        self.evi.indatabase and
        self.nbr.indatabase and
        return scenes that are not in the database'''
        scenes_out_of_db = []
        for scene in self.all_scenes:
            if scene not in self.common_elements:
                scenes_out_of_db.append(scene)
        return scenes_out_of_db

    def run_extraction(self, limit = None):
        '''run scenes to process'''

        while True:
            try:
                self.ndvi.checkdatabase()
                self.evi.checkdatabase()
                self.nbr.checkdatabase()

                self.common_elements = self.compare_indatabase()
                self.scenes_to_process = self.get_scenes_out_of_db()

                tempfolder = temp_folder()

                scenes_path = [os.path.join(self.productpath,value) for value in self.product_files]

                if limit is not None:
                    scenes_to_process = self.scenes_to_process[:limit]
                else:
                    scenes_to_process = self.scenes_to_process

                for scene in scenes_to_process:
                    if scene not in self.ndvi.indatabase:
                        print(f'Processing scene {scene} for ndvi')
                        r = re.compile('.*'+scene+'.*')
                        selected_files = list(filter(r.match, scenes_path))
                        start = time.time()
                        file_date = datetime.strptime(scene, 'A%Y%j').strftime('%Y-%m-%d')
                        mos = mosaic_raster(selected_files,'250m 16 days NDVI')
                        mos = mos * 0.1
                        temporal_raster = os.path.join(tempfolder,'ndvi_'+scene+'.tif')
                        result_file = os.path.join(tempfolder,'ndvi_'+scene+'.csv')
                        mos.rio.to_raster(temporal_raster, compress='LZW')
                        run_WeightedMeanExtraction(temporal_raster,result_file)
                        write_line(self.ndvi.database, result_file, self.ndvi.catchment_names, scene, file_date, nrow = 1)
                        end = time.time()
                        time_dif = str(round(end - start))
                        currenttime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        print(f'Time elapsed for {scene}: {str(round(end - start))} seconds')
                        write_log(hcl.log_veg_o_modis_ndvi_mean,scene,currenttime,time_dif,self.ndvi.database)
                        os.remove(temporal_raster)
                        os.remove(result_file)
                        gc.collect()
                    if scene not in self.evi.indatabase:
                        print(f'Processing scene {scene} for evi')
                        r = re.compile('.*'+scene+'.*')
                        selected_files = list(filter(r.match, scenes_path))
                        start = time.time()
                        file_date = datetime.strptime(scene, 'A%Y%j').strftime('%Y-%m-%d')
                        mos = mosaic_raster(selected_files,'250m 16 days EVI')
                        mos = mos * 0.1
                        temporal_raster = os.path.join(tempfolder,'evi_'+scene+'.tif')
                        result_file = os.path.join(tempfolder,'evi_'+scene+'.csv')
                        mos.rio.to_raster(temporal_raster, compress='LZW')
                        run_WeightedMeanExtraction(temporal_raster,result_file)
                        write_line(self.evi.database, result_file, self.evi.catchment_names, scene, file_date, nrow = 1)
                        end = time.time()
                        time_dif = str(round(end - start))
                        currenttime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        print(f'Time elapsed for {scene}: {str(round(end - start))} seconds')
                        write_log(hcl.log_veg_o_modis_evi_mean,scene,currenttime,time_dif,self.evi.database)
                        os.remove(temporal_raster)
                        os.remove(result_file)
                        gc.collect()
                    if scene not in self.nbr.indatabase:
                        print(f'Processing scene {scene} for nbr')
                        r = re.compile('.*'+scene+'.*')
                        selected_files = list(filter(r.match, scenes_path))
                        start = time.time()
                        file_date = datetime.strptime(scene, 'A%Y%j').strftime('%Y-%m-%d')
                        mos = mosaic_nd_raster(selected_files,'250m 16 days NIR reflectance', '250m 16 days MIR reflectance')
                        temporal_raster = os.path.join(tempfolder,'nbr_'+scene+'.tif')
                        result_file = os.path.join(tempfolder,'nbr_'+scene+'.csv')
                        mos.rio.to_raster(temporal_raster, compress='LZW', dtype = 'int16')
                        run_WeightedMeanExtraction(temporal_raster,result_file)
                        write_line(self.nbr.database, result_file, self.nbr.catchment_names, scene, file_date, nrow = 1)
                        end = time.time()
                        time_dif = str(round(end - start))
                        currenttime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        print(f'Time elapsed for {scene}: {str(round(end - start))} seconds')
                        write_log(hcl.log_veg_o_int_nbr_mean,scene,currenttime,time_dif,self.nbr.database)
                        os.remove(temporal_raster)
                        os.remove(result_file)
                        gc.collect()

            except:
                continue
            break

class mod10a2extractor:
    '''class to extract MOD10A2 to hidrocl variables

    Parameters:
    nsnow (HidroCLVariable): north face snow
    ssnow (HidroCLVariable): south face snow'''

    def __init__(self,nsnow,ssnow):
        if isinstance(nsnow,HidroCLVariable) & isinstance(ssnow,HidroCLVariable):
            self.nsnow = nsnow
            self.ssnow = ssnow
            self.productname = 'MODIS MOD10A2 Version 0.61'
            self.productpath = hcl.mod10a2_path
            self.common_elements = self.compare_indatabase()
            self.product_files = self.read_product_files()
            self.product_ids = self.get_product_ids()
            self.all_scenes = self.check_product_files()
            self.scenes_occurrences = self.count_scenes_occurrences()
            self.incomplete_scenes = self.get_incomplete_scenes()
            self.overpopulated_scenes = self.get_overpopulated_scenes()
            self.complete_scenes = self.get_complete_scenes()
            self.scenes_to_process = self.get_scenes_out_of_db()
        else:
            raise TypeError('nsnow and snow must be HidroCLVariable objects')

    def __repr__(self):
        return f'Class to extract {self.productname}'
    def __str__(self):
        return f'''
Product: {self.productname}

North face snow records: {len(self.nsnow.indatabase)}.
North face snow path: {self.nsnow.database}

South face snow records: {len(self.ssnow.indatabase)}.
South face snow database path: {self.ssnow.database}
        '''

    def order_indatabase(self):
        '''order indatabase attributes'''
        if len(self.nsnow.indatabase) > 0:
            self.nsnow.indatabase.sort()
        if len(self.ssnow.indatabase) > 0:
            self.ssnow.indatabase.sort()

    def compare_indatabase(self):
        '''compare indatabase and return elements that are equal'''
        self.order_indatabase()
        if len(self.nsnow.indatabase) > 0 or len(self.snow.indatabase) > 0:
            common_elements = list(set(self.nsnow.indatabase) & set(self.ssnow.indatabase))
        else:
            common_elements = []
        return common_elements

    def read_product_files(self):
        '''read product files'''
        return [value for value in os.listdir(self.productpath) if '.hdf' in value]

    def get_product_ids(self):
        '''get product ids'''
        product_ids = [value.split('.')[1] for value in self.product_files]
        return product_ids

    def check_product_files(self):
        '''extract product ids from product files'''
        files_id = []
        for product_id in self.product_ids:
            if product_id not in files_id:
                files_id.append(product_id)
        files_id.sort()
        return files_id

    def count_scenes_occurrences(self):
        '''count self.all_scenes in self.product_ids returning a dictionary'''
        count_scenes = {}
        for scene in self.all_scenes:
            count_scenes[scene] = self.product_ids.count(scene)
        return count_scenes

    def get_overpopulated_scenes(self):
        '''get scenes with more than 9 items from self.scenes_occurrences'''
        overpopulated_scenes = []
        for scene,occurrences in self.scenes_occurrences.items():
            if occurrences > 9:
                overpopulated_scenes.append(scene)
        return overpopulated_scenes

    def get_incomplete_scenes(self):
        '''get scenes with less than 9 items from self.scenes_occurrences'''
        incomplete_scenes = []
        for scene,occurrences in self.scenes_occurrences.items():
            if occurrences < 9:
                incomplete_scenes.append(scene)
        return incomplete_scenes

    def get_complete_scenes(self):
        '''get scenes with 9 items from self.scenes_occurrences'''
        complete_scenes = []
        for scene,occurrences in self.scenes_occurrences.items():
            if occurrences == 9:
                complete_scenes.append(scene)
        return complete_scenes

    def get_scenes_out_of_db(self):
        '''compare
        self.nsnow.indatabase and
        self.ssnow.indatabase and
        return scenes that are not in the database'''
        scenes_out_of_db = []
        for scene in self.all_scenes:
            if scene not in self.common_elements:
                scenes_out_of_db.append(scene)
        return scenes_out_of_db

    def run_extraction(self, limit = None):
        '''run scenes to process'''

        while True:
            try:
                self.ndvi.checkdatabase()
                self.evi.checkdatabase()
                self.nbr.checkdatabase()

                self.common_elements = self.compare_indatabase()
                self.scenes_to_process = self.get_scenes_out_of_db()

                tempfolder = temp_folder()

                scenes_path = [os.path.join(self.productpath,value) for value in self.product_files]

                if limit is not None:
                    scenes_to_process = self.scenes_to_process[:limit]
                else:
                    scenes_to_process = self.scenes_to_process

                for scene in scenes_to_process:

                    tempfolder = temp_folder()

                    scenes_path = [os.path.join(self.productpath,value) for value in self.product_files]

                    if limit is not None:
                        scenes_to_process = self.scenes_to_process[:limit]
                    else:
                        scenes_to_process = self.scenes_to_process

                    for scene in scenes_to_process:
                        if scene not in self.nsnow.indatabase or scene not in self.ssnow.indatabase:
                            print(f'Processing scene {scene} for snow processing')
                            r = re.compile('.*'+scene+'.*')
                            selected_files = list(filter(r.match, scenes_path))
                            start = time.time()
                            file_date = datetime.strptime(scene, 'A%Y%j').strftime('%Y-%m-%d')
                            mos = mosaic_raster(selected_files,'Maximum_Snow_Extent')
                            mos = (mos.where(mos == 200)/200).fillna(0)
                            temporal_raster = os.path.join(tempfolder,'snow_'+scene+'.tif')
                            mos.rio.to_raster(temporal_raster, compress='LZW')

                            if scene not in self.nsnow.indatabase:
                                result_file = os.path.join(tempfolder,'nsnow_'+scene+'.csv')
                                run_WeightedSumExtractionNorth(temporal_raster,result_file)
                                write_line(self.nsnow.database, result_file, self.nsnow.catchment_names, scene, file_date, nrow = 1)
                                os.remove(result_file)

                            if scene not in self.ssnow.indatabase:
                                result_file = os.path.join(tempfolder,'ssnow_'+scene+'.csv')
                                run_WeightedSumExtractionSouth(temporal_raster,result_file)
                                write_line(self.ssnow.database, result_file, self.ssnow.catchment_names, scene, file_date, nrow = 1)
                                os.remove(result_file)

                            end = time.time()
                            time_dif = str(round(end - start))
                            currenttime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            print(f'Time elapsed for {scene}: {str(round(end - start))} seconds')
                            write_log_double(hcl.log_snw_o_modis_sca_cum,scene,currenttime,time_dif,self.nsnow.database,self.ssnow.database)
                            os.remove(temporal_raster)

            except:
                continue
            break