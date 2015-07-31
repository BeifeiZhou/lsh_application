#!/usr/bin/python
import operator
import simplejson as json
import random
import os
import numpy as np
import time
import re
import sys
from pyspark import SparkContext, SparkConf

#p1 = re.compile("'(.+)'")
#cmd = "awk '{print $1}' /Users/nali/Beifei/ximalaya2015/code_ximalaya/code_LSH/TarsosLSH/Cluster/data.txt > albums.txt"
#os.system(cmd)

conf = SparkConf()
sc = SparkContext(appName = "LSH")
id_module = sc.textFile('/Users/nali/Beifei/ximalaya2015/code_ximalaya/code_LSH/TarsosLSH/Cluster/data.txt')\
        .map(lambda x: x.split(' '))\
        .map(lambda x: (x[0].replace("'", ""), map(lambda xx: float(xx), x[1:])))\
        .map(lambda x: (x[0], np.sqrt(np.sum(np.array(x[1])**2))))\
        .collect()
id_module = dict(id_module)

id_title = open('/Users/nali/Beifei/ximalaya2015/code_ximalaya/code_LSH/TarsosLSH/Cluster/albumIdTitleCatigory.csv','r').readlines()
id_title = map(lambda x: x.strip().split(','), id_title)
id_title = map(lambda x: (x[0], x[4]), id_title)
id_title = dict(id_title)

albums_v = map(lambda x: x.strip(), open('/Users/nali/Beifei/ximalaya2015/data/user_history/albums.txt','r').readlines())
f = file('/Users/nali/Beifei/ximalaya2015/data/user_history/yue.json')
source = f.read()
userhistory = json.JSONDecoder().decode(source)
albums = []
ids = []
for line in userhistory:
#    if float(line['total_play_percent']) > 0.3 and float(line['total_play_percent']) < 1.0 and line['album_id'] in albums_v and line['album_id'] in id_title.keys() and line['album_id'] not in ids:
    if line['album_id'] in albums_v and line['album_id'] in id_title.keys() and line['album_id'] not in ids:
        albums.append(line)
        ids.append(line['album_id'])

if len(albums) < 2:
    sys.exit('The user listened to less than two albums')
else:
    def random_notsameid(x):
        num_random = random.sample(range(len(x)),2)
        albums_random = [albums[i] for i in num_random]
        album1 = albums_random[0]
        album2 = albums_random[1]
        if album1['album_id'] == album2['album_id']:
            random_notsameid(x)
        else:
            return num_random
    num_random = random_notsameid(albums)
    print num_random
    num_random = random.sample(range(len(albums)),2)
    albums = [albums[i] for i in num_random]

    def getTitle(x):
        if x in id_title.keys():
            return id_title[x]
        else:
            return x

    jar_p= "/Users/nali/Beifei/ximalaya2015/code_ximalaya/code_LSH/TarsosLSH/TarsosLSH_test/build/TarsosLSH-0.7.jar"
    dataset_p = "/Users/nali/Beifei/ximalaya2015/code_ximalaya/code_LSH/TarsosLSH/Cluster/data.txt"
    result_p = "/Users/nali/Beifei/ximalaya2015/data/user_history/result.txt"
    query_p = "/Users/nali/Beifei/ximalaya2015/data/user_history/query.txt"
    num_sim = 50*len(albums)
    cmd_rm = "rm *.bin"
    cmd_result = 'grep "^\'" /Users/nali/Beifei/ximalaya2015/data/user_history/result.txt > result_query.txt'
    output = open("out.txt",'w')

    queries = []
    modules = []
    sim_albums = []

    for n in range(len(albums)):
        eachalbum = albums[n]
        cmd = "grep "+'"'+"'"+eachalbum['album_id']+"'"+'"'+" /Users/nali/Beifei/ximalaya2015/code_ximalaya/code_LSH/TarsosLSH/Cluster/data.txt > query"+str(n)+".txt"
        cmd_java_query = "java -jar "+jar_p+" -d "+dataset_p+" -q query"+str(n)+".txt -f cos -n 50 > "+result_p
        os.system(cmd)
        os.system(cmd_java_query)
        os.system(cmd_rm)
        os.system(cmd_result)
        result = open('/Users/nali/Beifei/ximalaya2015/data/user_history/result_query.txt','r').readlines()[0]
        result_query = result.strip().replace("'","").split(";")[1:][:-1]
        result_query_dict = {k: id_module[k] for k in result_query}
        result_query = list(np.array(sorted(result_query_dict.items(), key = operator.itemgetter(1), reverse = True))[:,0][:25])
        sim_albums = sim_albums + result_query
        result_query_replace = ','.join(map(lambda x: getTitle(x), result_query))
        album = getTitle(eachalbum['album_id'])+','+eachalbum['total_play_percent']+'\n'
        output.write(album)
        output.write(result_query_replace+'\n') 
        output.write('---------------------\n')
        query = open('/Users/nali/Beifei/ximalaya2015/data/user_history/query'+str(n)+'.txt','r').readlines()[0].split(' ')
        query = np.array(map(lambda x: float(x), query[1:])) 
        module = np.sqrt(sum(query**2))
        query = [query, module]
        queries.append(query)

    sim_albums = list(set(sim_albums))
    queries = map(lambda x: x[0]/x[1], queries)
    queries = np.array(queries)
    query = np.sum(queries, axis = 0)
    query = "'query' "+' '.join(map(lambda x: str(x), query))
    open('/Users/nali/Beifei/ximalaya2015/data/user_history/query.txt','w').write(query)
    cmd_java_query = "java -jar "+jar_p+" -d "+dataset_p+" -q query"+str(n)+".txt -f cos -n "+str(num_sim)+" > "+result_p
    os.system(cmd_java_query)
    os.system(cmd_rm)
    os.system(cmd_result)
    result = open('/Users/nali/Beifei/ximalaya2015/data/user_history/result_query.txt','r').readlines()[0]
    result_query = result.strip().replace("'","").split(";")[1:][:-1]
    result_query_dict = {k: id_module[k] for k in result_query}
    result_query = list(np.array(sorted(result_query_dict.items(), key = operator.itemgetter(1), reverse = True))[:,0][:25*len(albums)])
    sim_all = result_query
    result_query_replace = ','.join(map(lambda x: getTitle(x), result_query))
    p = len(set(sim_all).intersection(set(sim_albums)))/float(num_sim)
    output.write("query"+","+str(p)+'\n')
    output.write(result_query_replace+'\n')
    output.write('---------------------\n')
    output.write('---------------------\n')
    output.close()
    cmd_output = "cat out.txt >> output.txt"
    os.system(cmd_output)
