import dml
import prov.model
import urllib.request
import datetime
import json
import uuid
import re

class boston_crimes(dml.Algorithm):
	contributor = 'agoncharova_lmckone'
	reads = []
	writes = ['agoncharova_lmckone.boston_crimes']
	repo_name = "agoncharova_lmckone.boston_crimes"

	@staticmethod
	def get_crime_data():
		print("about to get boston crime data")
		print("NOTE: we are requesting it from the Boston data portal and it takes A WHILE")
		print("even in trial mode :(")
		url = "https://data.boston.gov/export/12c/b38/12cb3883-56f5-47de-afa5-3b1cf61b257b.json"
		response = urllib.request.urlopen(url).read()
		response = response.decode("utf-8").replace(']', "") + "]"
		data = json.loads(response)
		return data

	@staticmethod
	def execute(trial = False):
		'''Retrieve Boston Crime dataset for 2015-now'''
		startTime = datetime.datetime.now()


		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("agoncharova_lmckone", "agoncharova_lmckone")
		repo.dropCollection("boston_crimes")
		repo.createCollection("boston_crimes")
		print("finished setuprepo")
		
		# get data
		data = boston_crimes.get_crime_data()

		# filter out rows that dont have valid lat long points
		data = [x for x in data if re.match("^4", x['Lat'])]
		data = [x for x in data if re.match("^-7",x['Long'])]

		#use only 2016 data for now bc of size
		data = [x for x in data if x['YEAR']=="2016"]



		for item in data:
			item['Long'] = float(item['Long'])
			item['Lat'] = float(item['Lat'])
		if(trial):
			data = data[:1000]
		# save data
		repo['agoncharova_lmckone.boston_crimes'].insert_many(data)
		repo.logout()

		print("got all Boston crime data and saved it to " + boston_crimes.repo_name)
		
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}		
	
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		# Set up the database connection.
		
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')
		# TODO: should this be removed?
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		doc.add_namespace('bdp', 'https://data.boston.gov/export/')

		this_script = doc.agent('alg:agoncharova_lmckone#boston_crimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:a26ac664-2a36-48cf-9ed2-e1c0752a2534', {'prov:label':'Boston: Crime Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_boston_crimes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_boston_crimes, this_script)

		doc.usage(get_boston_crimes, resource, startTime, None, { prov.model.PROV_TYPE:'ont:Retrieval' })

		boston_crimes = doc.entity('dat:agoncharova_lmckone#boston_crimes', {prov.model.PROV_LABEL:'Boston: Crimes', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(boston_crimes, this_script)
		doc.wasGeneratedBy(boston_crimes, get_boston_crimes, endTime)
		doc.wasDerivedFrom(boston_crimes, resource, get_boston_crimes, get_boston_crimes, get_boston_crimes)
		repo.logout()

		return doc

# boston_crimes.execute()
# doc = boston_crimes.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof