import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class requestCityScore(dml.Algorithm):
    contributor = 'lliu_saragl'
    reads = []
    writes = ['lliu_saragl.scores']

    @staticmethod
    def execute(trial = False):
        '''Retrieve data sets'''
        startTime = datetime.datetime.now()

        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')

        url = 'https://data.boston.gov/export/5bc/e8e/5bce8e71-5192-48c0-ab13-8faff8fef4d7.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        response = response.replace("]", "")
        response += "]"
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("CityScore")
        repo.createPermanent("CityScore")
        repo['lliu_saragl.CityScore'].insert_many(r)
        repo['lliu_saragl.CityScore'].metadata({'complete':True})
        print(repo['lliu_saragl.CityScore'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening in this script.
        Each run of the script will generate a new document describing that invocation event.
        '''

        # Set up the database connection 
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('lliu_saragl', 'lliu_saragl')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        #additional resource
        doc.add_namespace('anb', 'https://data.boston.gov/')

        this_script = doc.agent('alg:lliu_saragl#requestCityScore', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('anb:5bce8e71-5192-48c0-ab13-8faff8fef4d7', {'prov:label': 'CityScore, Service Requests', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        get_city = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_city, this_script)
        doc.usage(get_city, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        city_score = doc.entity('dat:lliu_saragl#CityScore',{prov.model.PROV_LABEL:'CityScore', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(city_score, this_script)
        doc.wasGeneratedBy(city_score, get_city, endTime)
        doc.wasDerivedFrom(city_score, get_city, get_city, get_city)

        #repo.record(doc.serialize())
        repo.logout()

        return doc

requestCityScore.execute()
doc = requestCityScore.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
