#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 13 11:43:39 2018

@author: Rachel
"""

import urllib.request
from bson import json_util # added in 2/11
import json
import dml
import prov.model
import datetime
import uuid

class getTrees(dml.Algorithm):
    contributor = 'rmak_rsc3'
    reads = []
    writes = ['rmak_rsc3.getTrees'] #CHANGE

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rmak_rsc3', 'rmak_rsc3') 
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/ce863d38db284efe83555caf8a832e2a_1.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        r = json_util.loads(response)['features']
        
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("getTrees") 
        repo.createCollection("getTrees")
        #repo['rmak_rsc3.trees'].insert_many(r)
        
        coordinates = {}
        for x in r:
            coordinates[x['properties']['OBJECTID']] = x['geometry']['coordinates']
            
        treecordFinal = [{'treeNums': k, 'coordinates': (coordinates[k][0], coordinates [k][1])} for k in coordinates]
        
        repo['rmak_rsc3.getTrees'].insert_many(treecordFinal)
    
        repo['rmak_rsc3.trees'].metadata({'complete':True})
 
        

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
   
        
    @staticmethod
    
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rmak_rsc3', 'rmak_rsc3')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:rmak_rsc3#getTrees', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource = doc.entity('bdp:ce863d38db284efe83555caf8a832e2a_1', {'prov:label':'Trees', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        getTrees = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
 
        doc.wasAssociatedWith(getTrees, this_script)

        doc.usage(getTrees, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        

        tree = doc.entity('dat:rmak_rsc3#trees', {prov.model.PROV_LABEL:'trees', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(tree, this_script)
        doc.wasGeneratedBy(tree, getTrees, endTime)
        doc.wasDerivedFrom(tree, resource, getTrees, getTrees, getTrees)
        
        

        repo.logout()
                  
        return doc
    '''
print('get_fireHydrant.execute()')
getUniversities.execute()
print('doc = get_fireHydrant.provenance()')
doc = getUniversities.provenance()
print('doc.get_provn()')
print(doc.get_provn())
print('json.dumps(json.loads(doc.serialize()), indent=4')
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
