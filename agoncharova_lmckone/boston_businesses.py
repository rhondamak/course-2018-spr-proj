import urllib.request
import json
import pprint
import dml
import prov.model
import datetime
import uuid

class boston_businesses(dml.Algorithm):
	pp = pprint.PrettyPrinter(indent=2)
	contributor = 'agoncharova_lmckone'
	reads = []
	writes = ['agoncharova_lmckone.boston_businesses']

	# request data
	CLIENT_SECRET = 'G2WWI5LGU3EYQFOIQ3WAWXDZGFY24XEZZHGZSI3U45R0E5O5'
	CLIENT_ID = 'F2I1TARKWE0GTDZD0KUAA22TUU0FQ0UMMHDL5U4XREALH1PJ'
	CATEGORY_ID = '4bf58dd8d48988d124941735' # 'Offices' category
	api_version = '20180201' # use Feb 1, 2018
	radius = 1500 # in meters 
	# url string to be later `format`ted
	url_string = (
		"https://api.foursquare.com/v2/venues/search"
		"?client_id={0}"
		"&client_secret={1}"
		"&categoryId={2}"
		"&ll={3}"
		"&v={4}"
		"&radius={5}"
		"&limit=100"
	)

	@staticmethod
	def get_coords(city):
		'''
		Returns a set of long and lat coords for use with 
		the Foursquare API, since there is a 50 item limit per query.
		Used by get_data_by_city that actually queries the API.
		'''
		coords = []
		if(city == 'SF'):
			coords = [[37.80, -122.51], [37.70, -122.51], [37.80, -122.40], [37.70, -122.40]]
			for lon in range(0, 12):	
				for lat in range(0, 11):			
					y = 37.70 + (lat/100.0)
					x = -122.51 + (lon/100.0)
					coords.append([float("{0:.2f}".format(y)), float("{0:.2f}".format(x))])
		if(city == 'Boston'):
			coords = [[42.40, -71.19], [42.30, -71.19], [42.40, -71.02], [42.30, -71.02]]
			for lon in range(0, 11):	
				for lat in range(0, 18):			
					y = 42.30 + (lat/100.0)
					x = -71.19 + (lon/100.0)
					coords.append([float("{0:.2f}".format(y)), float("{0:.2f}".format(x))])
		return coords

	@staticmethod
	def construct_set_of_queries(city):
		'''
		Returns an arrary of string URL queries, where
		the only difference are the coordinates
		''' 
		fr = boston_businesses
		coords = fr.get_coords(city)
		set_of_urls = []
		print("constructing set of URLs for: " + city)
		for coord in coords: 
			coords = '{0},{1}'.format(coord[0], coord[1])
			url = boston_businesses.url_string.format(
				boston_businesses.CLIENT_ID, 
				boston_businesses.CLIENT_SECRET, 
				boston_businesses.CATEGORY_ID, 
				coords, 
				boston_businesses.api_version, 
				boston_businesses.radius)
			set_of_urls.append(url)
		return set_of_urls


	@staticmethod
	def get_data_by_city(city):
		''' 
		Constructs and issues a request depending on the 
		`city` param passed in. 
		'''
		fr = boston_businesses
		pp = fr.pp				
		all_data = {}
		# get a set of url queries
		set_of_queries = fr.construct_set_of_queries(city)
		print("about to request business data")
		print("NOTE: this also takes a while")
		for query in set_of_queries:
			# make the request
			response = urllib.request.urlopen(query)
			data = json.load(response)['response']['venues']
			for place in data: 
				all_data[place['id']] = place	
		# do this to format the data
		for_mongo = []
		for item in all_data: 
			for_mongo.append(all_data[item])
		print("finished constructing results from request")
		return for_mongo

	@staticmethod
	def execute(trial = False):
		''' 
		Retrives business data using the Foursquare API for Boston and SF
		and saves to a database
		'''
		pp = boston_businesses.pp
		# names of db and collections 
		db_name = 'agoncharova_lmckone'
		#sf_coll = 'agoncharova_lmckone.sf_businesses'
		#boston_coll = 'agoncharova_lmckone.boston_businesses'
		
		# setup
		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')		

		response = boston_businesses.get_data_by_city("Boston")
		print("Got the following number of businesses in Boston:")		
		print(len(response))
		repo.dropCollection('agoncharova_lmckone.boston_businesses')
		repo.createCollection('agoncharova_lmckone.boston_businesses')
		repo['agoncharova_lmckone.boston_businesses'].insert_many(response)
		repo['agoncharova_lmckone.boston_businesses'].metadata( {'complete':True} )
		print("Saved Boston data")
		print(repo['agoncharova_lmckone.boston_businesses'].metadata())
		

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		'''
		Create the provenance document describing everything happening
		in this script. Each run of the script will generate a new
		document describing that invocation event.
		'''
		# shorten class name
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('agoncharova_lmckone', 'agoncharova_lmckone')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		# custom data sources
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
		doc.add_namespace('4sq', 'https://data.cityofboston.gov/resource/')
		doc.add_namespace('sfdp', 'https://datasf.org/opendata/')

		this_script = doc.agent('alg:agoncharova_lmckone#boston_businesses', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		# TODO: Is the value after bdp below a random id?
		resource = doc.entity('4sq:40e2-897e', {'prov:label':'Foursquare, Office Data for Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

		#	separate by SF and Boston data

		get_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_boston, this_script)
		# TODO: How do we format the complex set of queries to the API?

		boston_businesses = doc.entity('dat:agoncharova_lmckone#boston_businesses', {prov.model.PROV_LABEL:'Boston Businesses', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(boston_businesses, this_script)
		doc.wasGeneratedBy(boston_businesses, get_boston, endTime)
		doc.wasDerivedFrom(boston_businesses, resource, get_boston, get_boston, get_boston)

		repo.logout()
		        
		return doc

# boston_businesses.execute()
# boston_businesses.provenance()
