# -*- coding: utf-8 -*-
"""
Created on Sun Feb 11 16:32:25 2018

@author: Alexander
"""


import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from time import sleep
import sample

class getBostonYelpRestaurantData(dml.Algorithm):
    
    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.getBostonYelpRestaurantData']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')

        key = dml.auth['services']['yelp']['key']        
        
        collection = repo.bstc_semina.getBostonRestaurantLicenseData
        cursor = collection.find({})
        repo.dropCollection('getBostonYelpRestaurantData')
        repo.createCollection('getBostonYelpRestaurantData')
        for i in cursor:
            name = i["businessName"]
            address = i["Address"] + " " + i["CITY"]
            r = sample.search(key,name,address)
            repo['bstc_semina.getBostonYelpRestaurantData'].insert_one(r)
        #url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?query=restaurants+in+boston&key=' + key
        #response = urllib.request.urlopen(url).read().decode()
#        response = response.replace("]", "")
#        response = response.replace("[", "")
#        response = "[" + response + "]"
        #r = json.loads(response)
        #pagetoken = r["next_page_token"]
        #print(len(r))
        #r = r["results"]
        #s = json.dumps(r, sort_keys=True, indent=2)
        #print(type(repo['bstc_semina.ApiTest']))
        repo['bstc_semina.getBostonYelpRestaurantData'].metadata({'complete':True})
        #print(repo['bstc_semina.googleTest'].metadata())
        
        repo.logout()
        
        endTime = datetime.datetime.now()
        
        return ({'start':startTime, 'end':endTime})

    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/bstc_semina') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/bstc_semina') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://api.yelp.com/v3/businesses/search')

        this_script = doc.agent('alg:bstc_semina#getBostonYelpRestaurantData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:address city', {'prov:label':'Reviews', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_rate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_rate, this_script)
        doc.usage(get_rate, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Yelp+Reviews&$select=_id,businesses,total,region'
                  }
                  )

        rate = doc.entity('dat:bstc_semina#getBostonYelpRestaurantData', {prov.model.PROV_LABEL:'Yelp Ratings', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(rate, this_script)
        doc.wasGeneratedBy(rate, get_rate, endTime)
        doc.wasDerivedFrom(rate, resource, get_rate, get_rate, get_rate)


        repo.logout()
                  
        return doc
    
getBostonYelpRestaurantData.execute()
doc = getBostonYelpRestaurantData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
