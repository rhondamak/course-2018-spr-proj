import dml
import prov.model
import datetime
import uuid
import prequest as requests
from pyspark.sql import SparkSession


class get_demographics(dml.Algorithm):
    contributor = 'fjansen'
    reads = []
    writes = ['fjansen.race', 'fjansen.householdincome', 'fjansen.povertyrates', 'fjansen.commuting']

    @staticmethod
    def execute(trial=False):

        start_time = datetime.datetime.now()

        spark = SparkSession.builder.appName('save-demographics').getOrCreate()

        # opens 'Race.json' file from datamechanics.io

        url = 'http://datamechanics.io/data/nathansw_sbajwa/Race.json'
        response = requests.get(url).json()

        df = spark.createDataFrame(response)
        df.write.json('hdfs://project/hariri/cs591/race.json')

        # opens 'MeansOfCommuting.json' file from datamechanics.io

        url = 'http://datamechanics.io/data/nathansw_sbajwa/MeansOfCommuting.json'
        response = requests.get(url).json()
        df = spark.createDataFrame(response)
        df.write.json('hdfs://project/hariri/cs591/commuting.json')

        # opens 'PovertyRates.json' file from datamechanics.io

        url = 'http://datamechanics.io/data/nathansw_sbajwa/PovertyRates.json'
        response = requests.get(url).json()
        df = spark.createDataFrame(response)
        df.write.json('hdfs://project/hariri/cs591/poverty-rates.json')

        # opens 'HouseholdIncome.json' file from datamechanics.io

        url = 'http://datamechanics.io/data/nathansw_sbajwa/HouseholdIncome.json'
        response = requests.get(url).json()

        # removes $ from all of the nested keys within the JSON file (char forbidden by mongodb)
        # TODO Is this necessary for Spark?
        for town in response.keys():
            # Preps variables to alter dict with
            toReplace = {}
            toDelete = []
            for old_key in response[town]:
                # ex: '$25,000-34,999' -> '25,000-34,999'
                new_key = old_key.replace('$', '')
                # only continue if the original key had a $ that needed to be removed
                if new_key != old_key:
                    # puts new key in separate dict
                    toReplace[new_key] = response[town][old_key]
                    # adds old key to list of keys to be deleted
                    toDelete += [old_key]
            # merges two dicts i.e. r[town] contains both old and new keys ($ and no $)
            response[town].update(toReplace)
            # deletes old keys from r[town] leaving only kys with no $
            for key in toDelete:
                del response[town][key]

        df = spark.createDataFrame(response)
        df.write.json('hdfs://project/hariri/cs591/household-income.json')

        # logs out of db
        spark.stop()

        end_time = datetime.datetime.now()

        return {"start": start_time, "end": end_time}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("fjansen", "fjansen")

        ## Namespaces
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts in / format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets in / format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log#')  # The event log.

        ## Agents
        this_script = doc.agent('alg:fjansen#demographics',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        ## Activities
        get_race = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_povertyrates = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_householdincome = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_commuting = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        ## Entities
        # Data Sources
        resource1 = doc.entity('dat: Race.json',
                               {'prov:label': 'Race by Neighborhood', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        resource2 = doc.entity('dat: PovertyRates.json',
                               {'prov:label': 'Poverty Rates by Neighborhood', prov.model.PROV_TYPE: 'ont:DataResource',
                                'ont:Extension': 'json'})
        resource3 = doc.entity('dat: HouseholdIncome.json', {'prov:label': 'Household Income by Neighborhood',
                                                             prov.model.PROV_TYPE: 'ont:DataResource',
                                                             'ont:Extension': 'json'})
        resource4 = doc.entity('dat: MeansOfCommuting.json', {'prov:label': 'Means of Commuting by Neighborhood',
                                                              prov.model.PROV_TYPE: 'ont:DataResource',
                                                              'ont:Extension': 'json'})

        # Data Generated
        race = doc.entity('dat:fjansen#race',
                          {prov.model.PROV_LABEL: 'Race by Neighborhood', prov.model.PROV_TYPE: 'ont:DataSet'})
        povertyrates = doc.entity('dat:fjansen#povertyrates', {prov.model.PROV_LABEL: 'Poverty by Neighborhood',
                                                               prov.model.PROV_TYPE: 'ont:DataSet'})
        commuting = doc.entity('dat:fjansen#commuting',
                               {prov.model.PROV_LABEL: 'Means of Commuting by Neighborhood',
                                prov.model.PROV_TYPE: 'ont:DataSet'})
        householdincome = doc.entity('dat:fjansen#householdincome',
                                     {prov.model.PROV_LABEL: 'Household Income by Neighborhood',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})

        ## wasAssociatedWith
        doc.wasAssociatedWith(get_race, this_script)
        doc.wasAssociatedWith(get_povertyrates, this_script)
        doc.wasAssociatedWith(get_householdincome, this_script)
        doc.wasAssociatedWith(get_commuting, this_script)

        ## used
        doc.usage(get_race, resource1, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_povertyrates, resource2, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_householdincome, resource3, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_commuting, resource4, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        ## wasGeneratedBy
        doc.wasGeneratedBy(race, get_race, endTime)
        doc.wasGeneratedBy(povertyrates, get_povertyrates, endTime)
        doc.wasGeneratedBy(householdincome, get_householdincome, endTime)
        doc.wasGeneratedBy(commuting, get_commuting, endTime)

        ## wasAttributedTo
        doc.wasAttributedTo(race, this_script)
        doc.wasAttributedTo(povertyrates, this_script)
        doc.wasAttributedTo(householdincome, this_script)
        doc.wasAttributedTo(commuting, this_script)

        ## wasDerivedFrom
        doc.wasDerivedFrom(race, resource1, get_race, get_race, get_race)
        doc.wasDerivedFrom(povertyrates, resource2, get_povertyrates, get_povertyrates, get_povertyrates)
        doc.wasDerivedFrom(householdincome, resource3, get_householdincome, get_householdincome, get_householdincome)
        doc.wasDerivedFrom(commuting, resource4, get_commuting, get_commuting, get_commuting)

        repo.logout()

        return doc
