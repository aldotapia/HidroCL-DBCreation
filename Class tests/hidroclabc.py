'''
this is hidrocl abstract base classes for testing purposes
what does an hidroclabc class do?
The class should:
 - is necessary to download data or can I mess things up? Maybe split classes for processing and download
 - create a database
 - load database
 - check current observations in the database
 - process data stored on nas
 - write data to database
'''

import collections
import os
import pandas as pd

import hidrocl_paths as hcl


hcl_object = collections.namedtuple('HCLObs',['name','date','value'])

class HidroCLVariable:
    '''A class to hold information about a hidrocl variable'''
    def __init__(self,name,database,product):
        self.name = name
        self.database = database
        self.product = product
        self.indatabase =  None
        self.observations =  None
        self.catchment_names = None
        self.checkdatabase()

    def __repr__(self):
        return f'Variable: {self.name}. Records: {len(self.indatabase)}'
    
    def __str__(self):
        return f'''
Variable {self.name}.
Product : {self.product}.
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

# for testing
hh = HidroCLVariable('snow', hcl.snw_o_modis_sca_cum_n_d8_p0d, hcl.mod10a2_path)