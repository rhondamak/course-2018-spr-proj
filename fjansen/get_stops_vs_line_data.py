import dml
import prov.model
import datetime
import uuid
import prequest as requests
from pyspark.sql import SparkSession


class get_stops_vs_line_data(dml.Algorithm):
    contributor = 'fjansen'
    reads = []
    writes = ['fjansen.stopsVsLines']

    @staticmethod
    def execute(trial=False):
        start_time = datetime.datetime.now()

        print('Fetching stopsVsLines data...')
        data_url = 'http://datamechanics.io/data/nathansw_rooday_sbajwa_shreyap/stop_vs_lines.json'
        response = requests.get(data_url).json()
        print('stopsVsLines data fetched!')

        print('Saving stopsVsLines data...')
        spark = SparkSession.builder.appName('save-stops-vs-lines').getOrCreate()
        df = spark.createDataFrame(response)
        df.write.json('hdfs://project/hariri/cs591/stops-vs-lines.json')
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
        doc.add_namespace('stopsVsLines', 'https://data.boston.gov/api/action/datastore_search_sql')

        this_script = doc.agent('alg:fjansen#getstopsVsLinesData',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('stopsVsLines:?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22',
                              {'prov:label': 'stopsVsLines Data', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_stopsVsLines_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stopsVsLines_data, this_script)
        doc.usage(get_stopsVsLines_data, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22'
                   }
                  )
        stopsVsLines = doc.entity('dat:fjansen#stopsVsLines',
                                  {prov.model.PROV_LABEL: 'stopsVsLines Data', prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(stopsVsLines, this_script)
        doc.wasGeneratedBy(stopsVsLines, get_stopsVsLines_data, endTime)
        doc.wasDerivedFrom(stopsVsLines, resource, get_stopsVsLines_data, get_stopsVsLines_data, get_stopsVsLines_data)

        repo.logout()
        return doc
