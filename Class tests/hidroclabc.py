'''
this is hidrocl abstract base classes for testing purposes
what does an hidroclabc class do?
The class should:
 - is necessary to download data or can I mess things up? Maybe split classes for processing and download
 - create a database / Done
 - load database / Done
 - check current observations in the database / Done
 - process data stored on nas
 - write data to database
'''

# import collections
import os
import pandas as pd
import rioxarray as rioxr
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
        self.indatabase =  None
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
            self.catchment_names = self.observations.columns[2:].tolist()
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
    '''function to mosaic ndvi files with rioxarray library'''
    raster_single = []

    for raster in raster_list:
        with rioxr.open(raster, masked = True) as src:
            raster_single.append(getattr(src, layer))

    raster_mosaic = merge_arrays(raster_single)
    return raster_mosaic

class mod10a2extractor:
    '''class to extract mod10a2 to hidrocl variables
    
    Parameters:
    ndvi (HidroCLVariable): ndvi variable
    evi (HidroCLVariable): evi variable
    nbr (HidroCLVariable): nbr variable'''

    def __init__(self,ndvi,evi,nbr):
        if isinstance(ndvi,HidroCLVariable) & isinstance(evi,HidroCLVariable) & isinstance(nbr,HidroCLVariable):
            self.ndvi = ndvi
            self.evi = evi
            self.nbr = nbr
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
        self.ndvi.indatabase.sort()
        self.evi.indatabase.sort()
        self.nbr.indatabase.sort()

    def compare_indatabase(self):
        '''compare indatabase and return elements that are equal'''
        self.order_indatabase()
        common_elements = set(self.ndvi.indatabase) & set(self.evi.indatabase) & set(self.nbr.indatabase)
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