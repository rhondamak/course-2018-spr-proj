import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class nonpublicschools(dml.Algorithm):
    contributor = 'ashleyyu_bzwtong'
    reads = []
    writes = ['ashleyyu_bzwtong.nonpublicschools']

    @staticmethod
    def execute(trial = False):
        '''Nonpublic Schools in the City of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/0046426a3e4340a6b025ad52b41be70a_1.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        schools_json = [json.loads(response)]
        repo.dropCollection("nonpublicschools")
        repo.createCollection("nonpublicschools")
        repo['ashleyyu_bzwtong.nonpublicschools'].insert_many(schools_json)
        repo['ashleyyu_bzwtong.nonpublicschools'].metadata({'complete':True})
        print(repo['ashleyyu_bzwtong.nonpublicschools'].metadata())

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
        repo.authenticate('ashleyyu_bzwtong', 'ashleyyu_bzwtong')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ashleyyu_bzwtong') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/ashleyyu_bzwtong') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ashleyyu_bzwtong#schools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:Non Public Schools in Boston',
                              {'prov:label':'Non Public School Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_schools, this_script)
        doc.usage(get_schools, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )

        schools = doc.entity('dat:ashleyyu_bzwtong#schools', {prov.model.PROV_LABEL:'Nonpublic Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)
        doc.wasDerivedFrom(schools, resource, get_schools, get_schools, get_schools)

        repo.logout()
                  
        return doc

#nonpublicschools.execute()
#doc = nonpublicschools.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
