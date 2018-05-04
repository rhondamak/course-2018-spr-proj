import dml
import prov.model
import datetime
import uuid
import prequest as requests
from pyspark.sql import SparkSession


class get_mbta_performance_data(dml.Algorithm):
    contributor = 'fjansen'
    reads = []
    writes = ['fjansen.MBTAPerformance', 'fjansen.householdincome', 'fjansen.povertyrates', 'fjansen.commuting']

    @staticmethod
    def execute(trial=False):
        start_time = datetime.datetime.now()

        print('Fetching MBTAPerformance data...')
        data_url = 'http://datamechanics.io/data/nathansw_rooday_sbajwa_shreyap/MBTAPerformance.json'
        response = requests.get(data_url).json()
        print('MBTAPerformance fetched!')

        count = 0
        obj1 = {}
        obj2 = {}
        obj3 = {}
        for key in response.keys():
            if count % 3 == 0:
                obj1[key] = response[key]
            elif count % 3 == 1:
                obj2[key] = response[key]
            elif count % 3 == 2:
                obj3[key] = response[key]
            count += 1

        final = [obj1, obj2, obj3]

        print('Saving MBTAPerformance data...')
        spark = SparkSession.builder.appName('save-mbta-performance').getOrCreate()
        df = spark.createDataFrame(final)
        df.write.json('hdfs://project/hariri/cs591/mbta-performance.json')
        spark.stop()

        print('Done!')
        end_time = datetime.datetime.now()
        return {'start': start_time, 'end': end_time}

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
        repo.authenticate('fjansen', 'fjansen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        # Since the urls have a lot more information about the resource itself, we are treating everything apart from the actual document suffix as the namespace.
        doc.add_namespace('MBTAPerformance', 'https://data.boston.gov/api/action/datastore_search_sql')

        this_script = doc.agent('alg:#get_mbta_performance_data',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('MBTAPerformance:?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22',
                              {'prov:label': 'MBTAPerformance Data', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_MBTAPerformance_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_MBTAPerformance_data, this_script)
        doc.usage(get_MBTAPerformance_data, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22'
                   }
                  )
        MBTAPerformance = doc.entity('dat:#MBTAPerformance', {prov.model.PROV_LABEL: 'MBTAPerformance Data',
                                                              prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(MBTAPerformance, this_script)
        doc.wasGeneratedBy(MBTAPerformance, get_MBTAPerformance_data, endTime)
        doc.wasDerivedFrom(MBTAPerformance, resource, get_MBTAPerformance_data, get_MBTAPerformance_data,
                           get_MBTAPerformance_data)

        repo.logout()
        return doc
