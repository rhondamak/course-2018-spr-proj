import dml
import prov.model
import datetime
import uuid
import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as colors

class BostonScoringEval(dml.Algorithm):

    contributor = "bstc_csuksan_semina_tedkong"
    reads = []
    writes = ['bstc_csuksan_semina_tedkong.BostonScoringEval']

    """

    RUN AFTER BOSTON SCORING MAP

    """

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        """

        Read in json data of edge weights
        TODO: Read in from mongo collection after inserting it in BostonScoring_Map.py

        """
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_csuksan_semina_tedkong', 'bstc_csuksan_semina_tedkong')

        collection_map = repo.bstc_csuksan_semina_tedkong.BostonScoring_Map
        cursor_map = collection_map.find({})

        file = pd.DataFrame(list(cursor_map))
        #file = pd.read_json("BostonScoring_Map.json", lines=True)
        if trial == True:
            splitted = np.array_split(file, 3)
            file = splitted[0]

        """

        create array from dataframe
        sort dataframe

        """
        file = file.sort_values(by='name')
        names = np.array(file)[:,3]
        arr = np.array(file)
        top_connections = []
        arr = arr[:,1:]
        #print(arr)
        """

        This for loops finds the 6 min value connections for each restaurant

        """


        for i,row in enumerate(arr):
            if(names[i] != row[2]):
                print('oh god')
            top = {'name': names[i]}
            top.update({'latitude': row[0]})
            top.update({'longitude': row[1]})
            t = []
            score = row[3]
            top.update({'score': score})
            for j,row2 in enumerate(arr):
                if(names[i] == names[j] and row[3] == row2[3]):
                    continue
                else:
                    sco = abs(score - row2[3])
                    dis = np.sqrt(np.power(row[0] - row2[0],2) + np.power(row[1] - row2[1],2))
                    sco = sco + dis
                for ind, val in enumerate(t):
                    #print(val)
                    sc = abs(score - val[0])
                    dis = np.sqrt(np.power(row[0] - val[2],2) + np.power(row[1] - val[3],2))
                    sc = sc + dis
                    if (sc > sco):
                        t[ind] = (row2[3], names[j], row2[0], row2[1], dis)
                        break
                if (len(t) != 6):
                    t += [(row2[3], names[j], row2[0], row2[1], dis)]
            for ind,tu in enumerate(t):
                top.update({'Closest Score ' + str(ind): tu})
            top_connections += [top]
        print(top_connections[0])


        #file2 = pd.read_json("merged_datasets/RestaurantRatingsAndHealthViolations_Boston.json", lines=True)
        #arr2 = file2[['ave_violation_severity', 'rating','latitude','longitude']].copy()
        #cmap=plt.get_cmap('gist_earth')
        colors = ['y', 'orange', 'purple', 'teal', 'cyan', 'r', 'g', 'b', 'brown', 'black']

        plt.figure(1)
        for i in top_connections:

            lon = i['longitude']
            lat = i['latitude']
            plt.plot(lon, lat, 'o', color = "Red", markersize  = 6)
            for j in range(6):
                dis = i['Closest Score ' + str(j)][4]
                pos = int(round(dis*100))
                #print(pos)
                temp_i = [lon, i['Closest Score ' + str(j)][3]]
                temp_j = [lat, i['Closest Score ' + str(j)][2]]
                plt.plot(temp_i, temp_j, 'b--', lw = 0.5, color=colors[pos])
        plt.show()

        plt.figure(2)
        for i in top_connections:

            lon = i['longitude']
            lat = i['latitude']
            plt.plot(lon, lat, 'o', color = "Red")
        plt.show()

#        repo.logout()

        endTime = datetime.datetime.now()

        return ({'start':startTime, 'end':endTime})



    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bstc_csuksan_semina_tedkong', 'bstc_csuksan_semina_tedkong')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:bstc_csuksan_semina_tedkong#BostonScoringEval', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:bstc_csuksan_semina_tedkong#BostonScoringMap', {'prov:label':'Visualization', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_rate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_rate, this_script)
        doc.usage(get_rate, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Scoring+Map&$select=name,ave_violation_severity,rating,latitude,longitude'
                  }
                  )

        rate = doc.entity('dat:bstc_csuksan_semina_tedkong#BostonScoringEval', {prov.model.PROV_LABEL:'Scoring Evaluations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(rate, this_script)
        doc.wasGeneratedBy(rate, get_rate, endTime)
        doc.wasDerivedFrom(rate, resource, get_rate, get_rate, get_rate)


        repo.logout()

        return doc

BostonScoringEval.execute()
doc = BostonScoringEval.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
