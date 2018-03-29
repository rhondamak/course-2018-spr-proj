import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class property_data(dml.Algorithm):
    contributor = 'keyanv'
    reads = []
    writes = ['keyanv.properties']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('keyanv', 'keyanv')

        url = 'https://data.boston.gov/export/062/fc6/062fc6fa-b5ff-4270-86cf-202225e40858.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        # preprocessing to fix the mistakes of the ignorant soul who provided this data without verifying if it is valid json
        response = response.replace(']', "")
        response += ']'

        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("properties")
        repo.createCollection("properties")
        repo['keyanv.properties'].insert_many(r)
        repo['keyanv.properties'].metadata({'complete':True})
        print(repo['keyanv.properties'].metadata())

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
        repo.authenticate('keyanv', 'keyanv')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/export/')

        this_script = doc.agent('alg:keyanv#property_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:062/fc6/062fc6fa-b5ff-4270-86cf-202225e40858', {'prov:label':'Property Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_properties = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_properties, this_script)
        doc.usage(get_properties, resource, startTime)
        properties = doc.entity('dat:keyanv#properties', {prov.model.PROV_LABEL:'Property Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(properties, this_script)
        doc.wasGeneratedBy(properties, get_properties, endTime)
        doc.wasDerivedFrom(properties, resource, get_properties, get_properties, get_properties)

        repo.logout()
                  
        return doc

property_data.execute()
doc = property_data.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
