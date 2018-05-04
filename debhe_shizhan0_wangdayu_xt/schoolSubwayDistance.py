import urllib.request
import json
import math
import dml
import prov.model
import datetime
import uuid
import copy

'''
This is the file we are going to read two collections
We are going to read the allschool collection and hubway collection
For every school, it will calculate the distance to the closest hubway station
and save it into the data base
'''
class schoolSubwayDistance(dml.Algorithm):
    contributor = 'debhe_shizhan0_wangdayu_xt'
    reads = ['debhe_shizhan0_wangdayu_xt.allSchool', 'debhe_shizhan0_wangdayu_xt.subwayStop']
    writes = ['debhe_shizhan0_wangdayu_xt.schoolSubwayDistance']

    #reads = ['debhe_shizhan0_wangdayu_xt.allSchool', 'debhe_shizhan0_wangdayu_xt.subwayStopTrial']
    #writes = ['debhe_shizhan0_wangdayu_xt.schoolSubwayDistanceTrial']

    @staticmethod
    def execute(trial = False):
        ''' Merging data sets
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')

        schools = []
        subwayStop = []
        # Loads the collection
        schools = repo['debhe_shizhan0_wangdayu_xt.allSchool'].find()
        subwayStop = repo['debhe_shizhan0_wangdayu_xt.subwayStop'].find()

        # finding the distance between each school and its closest hubway bike station
        # using product, projection, and selection
        minDistance = []

        # Need to make the deepcopy of record for further use
        schools_1 = copy.deepcopy(schools)
        schools_2 = copy.deepcopy(schools)
        allDistance = []
        for row_1 in schools_1:
            subwayStop_temp = copy.deepcopy(subwayStop)
            for row_2 in subwayStop_temp:
                if(row_2['X'] != '""' and row_2['Y'] != '""' and row_1['X'] != '""' and row_1['Y'] != '""'):
                    s = (row_1['schoolName'], row_2['stopName'], (float(row_1['X']) - float(row_2['X']))**2 + ( float(row_1['Y']) - float(row_2['Y']) )**2, row_2['id'], row_1['X'], row_1['Y'], row_2['X'], row_2['Y'])
                    allDistance.append(s)
        
        # Need to school name for all the school as the key for further transformation
        keys = []
        for row in schools_2:
            keys.append(row['schoolName'])
        
        for key in keys:
            minD = float('inf')
            school = key
            station = ''
            schoolX = ''
            schoolY = ''

            for (k, b, v, d, x, y,sb_x, sb_y) in allDistance:
                if(key == k):
                    if(v < minD):
                        station = b
                        minD = v
                        identity = d
                        schoolX = x
                        schoolY = y
                        subwayX = sb_x
                        subwayY = sb_y
            minDistance.append({'schoolName': school , 'subwayStation': station, 'Distance': math.sqrt(minD), 'subwayId' : identity, "schoolX" : schoolX, 'schoolY' : schoolY, 'subwayX': subwayX, 'subwayY':subwayY})

        # save the information to the database
        repo.dropCollection("schoolSubwayDistance")
        repo.createCollection("schoolSubwayDistance")

        repo['debhe_shizhan0_wangdayu_xt.schoolSubwayDistance'].insert_many(minDistance)
        repo['debhe_shizhan0_wangdayu_xt.schoolSubwayDistance'].metadata({'complete': True})
        print("Saved schoolSubwayDistance", repo['debhe_shizhan0_wangdayu_xt.schoolSubwayDistance'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('debhe_shizhan0_wangdayu_xt', 'debhe_shizhan0_wangdayu_xt')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bpd',
                          'http://bostonopendata-boston.opendata.arcgis.com/datasets/')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        

        this_script = doc.agent('alg:#schoolSubwayDistance',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bpd:1d9509a8b2fd485d9ad471ba2fdb1f90_0',
                                             {'prov:label': 'minimum distance between school and Subway',
                                              prov.model.PROV_TYPE: 'ont:DataSet', 'ont:Extension':'csv'})

        get_schoolSubwayDistance = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_schoolSubwayDistance, this_script)
        doc.usage(get_schoolSubwayDistance, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation', 'ont:Query':'?type=delay+time$select=id, time'})

        schoolSubwayDistance = doc.entity('dat:debhe_shizhan0_wangdayu_xt#schoolSubwayDistance',
                          {prov.model.PROV_LABEL: 'minmium distance between school and Subway',
                           prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(schoolSubwayDistance, this_script)
        doc.wasGeneratedBy(schoolSubwayDistance, get_schoolSubwayDistance, endTime)
        doc.wasDerivedFrom(schoolSubwayDistance, resource, get_schoolSubwayDistance, get_schoolSubwayDistance, get_schoolSubwayDistance)

        repo.logout()

        return doc
