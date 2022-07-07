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

hcl_object = collections.namedtuple('HCLObs',['name','date','value'])

class HidroCLVariable:
    '''A class to hold information about a hidrocl variable'''
    def __init__(self,name,database,product):
        self.name = name
        self.database = database
        self.product = product
    
    def __str__(self):
        return f'Variable {name}. Records:'