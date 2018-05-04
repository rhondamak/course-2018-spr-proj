# -*- coding: utf-8 -*-
"""
Created on Sun Apr 22 00:28:19 2018

@author: Alexander
"""

import pandas as pd
import numpy as np
import json
import random
import requests

def locate(lat, long, rate, yelp, lim):
    urls = 'http://datamechanics.io/data/BostonScoring_Map.json'
    with requests.get(urls) as url:
        data = url.text
        data = json.dumps(data)
    temp = json.loads(data)
    file = pd.read_json(temp, lines=True)
    #print(file)
    
    # TEMPORARY INPUT
    #new = {"latitude":42.3777464032,"longitude":-71.0518522561,"name":"149 Eat Street","rate":0.4698795181,"yelp":0.5}
    new = {"latitude":lat,"longitude":long}
    check = [-99999999, new['latitude'],new['longitude']]
    data = np.array(file[['name','latitude','longitude', 'rate', 'yelp']])
    
    limit = lim
    ret = []
    rate_level = rate
    yelp_level = yelp
    #print(yelp)
    yelp_level = random.uniform((yelp_level), 5) 
    rate_level = random.uniform(1, 3 - ((rate_level) - 1))
    check += [rate_level / 3, yelp_level / 5]
    
    for ind,val in enumerate(data):
        x = (val[1] - check[1])**2
        y = (val[2] - check[2])**2
        x2 = ((val[3] - check[3])**2)
        y2 = ((val[4] - check[4])**2)
        distance1 = np.sqrt(x+y)
        distance2 = np.sqrt(x2+y2)
        distance = ((distance1 * 20) + distance2)/21
        if len(ret) < limit:
            ret += [(ind, distance)]
        else:
            for i,v in enumerate(ret):
                if v[1] > distance:
                    ret[i] = (ind, distance)
                    break
    
    local = []
    for i in ret:
        # remove scaling from yelp rating and health score
        data[i[0]][3] =  data[i[0]][3] * 3
        data[i[0]][4] =  round(data[i[0]][4] * 5, 1)
        local += [data[i[0]]]
    
    local = np.array(local)
    return local


