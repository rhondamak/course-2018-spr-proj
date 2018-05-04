import urllib.request
import json
import geojson
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import shapely 
from shapely.geometry import shape, Point, Polygon
import random
#import geopandas as geo
"""
Finds average point (lat, long) for each street in each district where crimes existed.
This is for finding the "middle" of the street - used in findCrimeStats.

Yields the form {DISTRICT: {"street1": {Lat: #, Long: #}, "street2": {Lat: #, Long: #} ...}, DISTRICT2:...}

- Filters out empty entries

"""

def merge_dicts(x, y):
    ''' Helper function to merge 2 dictionaries '''
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z


class sortNeighborhood(dml.Algorithm):
    contributor = 'janellc_rstiffel_yash'
    reads = ['janellc_rstiffel_yash.neighborhoods','janellc_rstiffel_yash.crimesData'] #NOTE: ALSO READS FROM ferrys.streetlights
    writes = ['janellc_rstiffel_yash.sortedNeighborhoods'] 


    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        # Get Crime data and neighborhood data
        # street light data borrowed from ferrys

        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate('janellc_rstiffel_yash', 'janellc_rstiffel_yash')
        crimesData = list(repo.janellc_rstiffel_yash.crimesData.find())
        neighborhoods = list(repo['janellc_rstiffel_yash.neighborhoods'].find())
        streetLights = list(repo.ferrys.streetlights.find())

        if trial:
            #print("you are gere")
            streetLights = random.sample(streetLights, 2000)
            #print(len(streetLights))
        n_count = {}
        

        # @staticmethod

        def count(data, name, N, figure):
            counter = 0
            for item in data:
                # Filters out empty rows
                if (item['Long'] == None or item['Lat'] == None):
                    continue
                point = Point(float(item['Long']), float(item['Lat']))
                if figure.contains(point):
                    counter += 1
            return(counter)

        ## def getCrime(data, N, figure):
        ##     for crime in crimesData:
        ##         # Filters out empty rows
        ##         if (crime['Long'] == None or crime['Lat'] == None):
        ##             continue
        ##         point = Point(float(crime['Long']), float(crime['Lat']))
        ##         if figure.contains(point):
        ##             # n_count[N] = {'crimes': 1}
        ##             if N not in n_count:
        ##                 n_count[N] = {'crimes': 1}
        ##             else:
        ##                 n_count[N]['crimes'] += 1
        
        ## # @staticmethod
        ## def getLights(data, N, figure):
        ##     for light in streetLights:
        ##         if (light['Long'] == None or light['Lat'] == None):
        ##             continue
        ##         point = Point(float(light['Long']), float(light['Lat']))
        ##         if figure.contains(point):
        ##             if N not in n_count:
        ##                 n_count[N] = {'lights': 1}
        ##             else:
        ##                 n_count[N]['lights'] += 1


        ##new = sortNeighborhoods.aggregate(streetLights, lambda t: [counter(x) for x in streetLights])
        ## print(new)
        n_count = {}
        for polygon in neighborhoods:
            figure = shape(polygon['Polygon'])
            N = polygon['Neighborhood']

            lightcount = count(streetLights, 'lights', N, figure)
            crimecount = count(crimesData, 'crimes', N, figure)
            n_count[N] = {'neighborhood': N, 'lights': lightcount, 'crimes': crimecount}
            # print(n_count)


        # Store in DB
        repo.dropCollection("sortedNeighborhoods")
        repo.createCollection("sortedNeighborhoods")

        for key,value in n_count.items():
             r = {'data':value}
             repo['janellc_rstiffel_yash.sortedNeighborhoods'].insert(r)
        repo['janellc_rstiffel_yash.sortedNeighborhoods'].metadata({'complete':True})
        print(repo['janellc_rstiffel_yash.sortedNeighborhoods'].metadata())


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
        repo.authenticate('janellc_rstiffel_yash', 'janellc_rstiffel_yash')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        
        # Agent, entity, activity
        this_script = doc.agent('alg:janellc_rstiffel_yash#sortNeighborhood', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # Resource = crimesData
        resource1 = doc.entity('dat:janellc_rstiffel_yash#crimesData', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        # Resource = crimesData
        resource2 = doc.entity('dat:ferrys#streetlights', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        # Resource = crimesData
        resource3 = doc.entity('dat:ferrys#neighborhoods', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})


        #Activity
        sort_neighborhoods = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(sort_neighborhoods, this_script)

        doc.usage(sort_neighborhoods, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )
        doc.usage(sort_neighborhoods, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )
        doc.usage(sort_neighborhoods, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Calculation',
                  'ont:Query':''
                  }
                  )


        sorted_neighborhoods = doc.entity('dat:janellc_rstiffel_yash#crimesDistrict', {prov.model.PROV_LABEL:'Counts streetlights and crimes per neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(sorted_neighborhoods, this_script)
        doc.wasGeneratedBy(sorted_neighborhoods, sort_neighborhoods, endTime)
        doc.wasDerivedFrom(sorted_neighborhoods, resource1, resource2, resource3, sort_neighborhoods)

        repo.logout()
                  
        return doc

#sortNeighborhood.execute(True)
#doc = sortNeighborhood.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
