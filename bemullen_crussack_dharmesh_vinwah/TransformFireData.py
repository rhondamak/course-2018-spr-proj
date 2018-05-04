# Filename: TransformFireData.py
# Author: Claire Russack <crussack@bu.edu>
# Description: K-Means clustering.

import urllib.request
from urllib.request import quote 
import json
import dml
import prov.model
import datetime
import uuid
import prequest
import geocoder
from sklearn.cluster import KMeans
import pandas as pd
import numpy as np
import ast

class TransformFireData(dml.Algorithm):
    contributor = "bemullen_crussack_dharmesh_vinwah"
    reads = ["bemullen_crussack_dharmesh_vinwah.fires"]
    writes = ["bemullen_crussack_dharmesh_vinwah.fires_monthly_centroids"]

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')

        fires = repo.get_collection('bemullen_crussack_dharmesh_vinwah.fires')

        may = []; dec = []; sept = []

        if fires is not None:
            fires_cursor = fires.find()
            if fires_cursor is not None:
                if fires_cursor.count() == 1:
                    coll_vals = fires_cursor.next()
                    obj_id = coll_vals.pop('_id', None)
                    for val in coll_vals.values():
                        if val[1] == None or val[1] == 'null':
                            val = ""
                        elif val[0] == 'may':
                            may.append(val[1])
                        elif val[0] == 'september':
                            sept.append(val[1])
                        elif val[0] == 'december':
                            dec.append(val[1])
                    # separated the data by month            
                    df_may = pd.DataFrame.from_dict(may)
                    df_sept = pd.DataFrame.from_dict(sept)
                    df_dec = pd.DataFrame.from_dict(dec)

                    fires_monthly_centroids = {}


                    f1 = df_may[0].values
                    f2 = df_may[1].values
                    y = list(zip(f1,f2))
                    X=np.matrix(y)
                    kmeans = KMeans(n_clusters=2)
                    kmeans.fit(X)
                    fires_monthly_centroids["may"] = list(kmeans.cluster_centers_[0])

                    f3 = df_sept[0].values
                    f4 = df_sept[1].values
                    x = list(zip(f3,f4)) 
                    W=np.matrix(x)
                    kmeans2 = KMeans(n_clusters=2)
                    kmeans2.fit(W)
                    fires_monthly_centroids["september"] = list(kmeans2.cluster_centers_[0])

                    f5 = df_dec[0].values
                    f6 = df_dec[1].values
                    w = list(zip(f5,f6))
                    Z=np.matrix(w)
                    kmeans3 = KMeans(n_clusters=2)
                    kmeans3.fit(Z)
                    fires_monthly_centroids["december"] = list(kmeans3.cluster_centers_[0])

                    write_key = "bemullen_crussack_dharmesh_vinwah.fires_monthly_centroids"

                    # NB: This is only being done to conform to Claire's standards.
                    #     Ideally, we'd seraliase this differently.
                    repo.dropCollection(write_key)
                    repo.createCollection(write_key)
                    print(fires_monthly_centroids)
                    repo[write_key].insert_one((fires_monthly_centroids))

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

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bemullen_crussack_dharmesh_vinwah', 'bemullen_crussack_dharmesh_vinwah')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') 
        doc.add_namespace('dat', 'http://datamechanics.io/data/') 
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bdpr', 'https://data.boston.gov/api/3/action/datastore_search_sql')
        doc.add_namespace('bdpm', 'https://data.boston.gov/datastore/odata3.0/')
        doc.add_namespace('csdt', 'https://cs-people.bu.edu/dharmesh/cs591/591data/')
        doc.add_namespace('datp', 'http://datamechanics.io/data/bemullen_crussack_dharmesh_vinwah/data/')


        this_script = doc.agent('alg:bemullen_crussack_dharmesh_vinwah#TransformFireData',\
            {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        fires = doc.entity('dat:bemullen_crussack_dharmesh_vinwah#fires',\
            {prov.model.PROV_LABEL:'Fire Data for Boston city',\
            prov.model.PROV_TYPE:'ont:DataSet'})        
        get_fires = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime,
            {'prov:label':'Fire Data for Boston City'})        
        doc.wasAssociatedWith(get_fires, this_script)
        doc.used(get_fires, fires, startTime)
        doc.wasAttributedTo(get_fires, this_script)
        doc.wasGeneratedBy(fires, get_fires, endTime)        
        
        repo.logout()
        return doc