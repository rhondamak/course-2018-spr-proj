# -*- coding: utf-8 -*-
"""
Created on Sun Feb 11 23:50:02 2018

@author: Alexander
"""



import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getNYCRestaurantData(dml.Algorithm):
    
    contributor = "bstc_semina"
    reads = []
    writes = ['bstc_semina.getNYCRestaurantData']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_semina', 'bstc_semina')
        
        app_token = dml.auth['services']['cityofnewyork']['token']        
        
        url ='https://data.cityofnewyork.us/resource/mphz-k8gq.json?' + app_token
        response = urllib.request.urlopen(url).read().decode()
        #response = response.replace("]", "")
        #response = response.replace("[", "")
        #response = "[" + response + "]"
        r = json.loads(response)
        #print(len(r))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection('getNYCRestaurantData')
        repo.createCollection('getNYCRestaurantData')
        repo['bstc_semina.getNYCRestaurantData'].insert_many(r)
        #print(type(repo['bstc_semina.ApiTest']))
        for i in range(5):
            url ='https://data.cityofnewyork.us/resource/mphz-k8gq.json?' + app_token + '&$offset=' + str(50000 * i)
            response = urllib.request.urlopen(url).read().decode()
            r = json.loads(response)
            repo['bstc_semina.getNYCRestaurantData'].insert_many(r)
        repo['bstc_semina.getNYCRestaurantData'].metadata({'complete':True})
        #print(repo['bstc_semina.getNYCRestaurantData'].metadata())

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
        doc.add_namespace('bdp', 'https://data.cityofnewyork.us/resource/')

        this_script = doc.agent('alg:bstc_semina#getNYCRestaurantData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:mphz-k8gq', {'prov:label':'Restaurants', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_rest = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_rest, this_script)
        doc.usage(get_rest, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=NYC+Restaurants&$select=_id,license_permit_holder,building_id,permit_status_date,permit_subtype_description,permit_expiration_date,address,city,license_permit_number,source,borough,zip_code,longitude_wgs84,permit_type_description,street,business_description,latitude_wgs84,borough_block_lot,permit_issuance_date,license_permit_holder_name'
                  }
                  )


        restaurant = doc.entity('dat:bstc_semina#getNYCRestaurantData', {prov.model.PROV_LABEL:'Restaurants', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(restaurant, this_script)
        doc.wasGeneratedBy(restaurant, get_rest, endTime)
        doc.wasDerivedFrom(restaurant, resource, get_rest, get_rest, get_rest)


        repo.logout()
                  
        return doc
    
getNYCRestaurantData.execute()
doc = getNYCRestaurantData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))