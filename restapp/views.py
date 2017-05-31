from django.shortcuts import render
from django.http import HttpResponse, HttpResponseNotAllowed, HttpResponseBadRequest
import re
from elasticsearch_dsl.query import MultiMatch, Match

import os
import logging
import json
from datetime import datetime, date, time
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search


client = Elasticsearch()



# Create your views here.
logger = logging.getLogger('restapp')




def createproject(request):
    ''' it handles the json request and save the json file to Project.txt'''
    print(request.content_type)
    if  request.content_type == 'application/json':
        logger.info("in post method of createproject")
        try:
            data = json.loads(request.body.decode("utf-8"))
        except ValueError:
            logger.info("invalid request. bad json sent")
            return HttpResponseBadRequest('invalid request.. ')


        id = data['id']
        try:
            id= int(id)
        except ValueError:
            logger.info("invalid request. project id should be of type int")
            return HttpResponseBadRequest('invalid request')
        projectName = data['projectName']
        projectCost = data['projectCost']
        projectUrl = data['projectUrl']
        creationDate = data['creationDate']

        expiryDate = data['expiryDate ']
        targetCountries = data['targetCountries']
        targetKeys = data['targetKeys']
        for target in targetKeys:
            try:
                int(target['number'])
            except ValueError:
                logger.info("invalid request. project id should be of type int")
                return HttpResponseBadRequest('invalid request')
        logger.info("Complete ->checking parameter")
        pwd = os.path.dirname(__file__)
        with open(pwd + '/Project.txt', 'a') as data_file: # appending data to the file
            data_file.write(json.dumps(data))
            data_file.write("\n")
            results.append(data)
            logger.info("Complete -> Writing value to file")






        logger.info("campaign is successfully created")

        return HttpResponse("campaign is successfully created")
    else:
        if request.GET:
            logger.info("invalid request. using get type")
        else:
            if request.content_type != 'application/json':
                logger.info("invalid request. request.content type is not json")


        return HttpResponseNotAllowed('Only POST method with content-type json allowed here')

def requestproject(request):
    ''' sends the project based on the parameter it receives'''
    logger.info("inside requestproject")
    documentAddFromFile()
    if len(request.GET) == 0:
        s = Search(using=client, index="restapp").sort('-projectCost')
        response = s.execute()
        return elasticResponseGeneratePrintOne(response)



    if  True :
        for k,v in request.GET.items():
            print (k)
            print (v)
            s = Search(using=client, index="restapp")
            if k == 'id':

                s = s.filter("term", id= v)
                response = s.execute()
                return elasticResponseGenerate(response)
            elif k== 'country':
                s = s.query("match", targetCountries=v)
                response = s.execute()
                return elasticResponseGenerate(response)
            elif k== 'number':
                args = {
                    "targetKeys.number": v
                }
                s.filter("term", **args)
                response = s.execute()
                return elasticResponseGenerate(response)



    return HttpResponse("no project found")

def documentAddFromFile():
    resultList = []
    pwd = os.path.dirname(__file__)
    with open(pwd + '/Project.txt', 'r') as data_file:
        logger.info("Complete -> Writing value to file")
        #client.bulk(index='restapp', body = data_file, refresh = True)
        for line in data_file:
            #client.index(index='restapp', doc_type= 'project', body = line)
            record = json.loads(line)
            resultList.append(record)
    return resultList

def elasticResponseGenerate(response):
    ''' sends the response generated from the elastic search'''

    if response['hits']['total'] > 0:
        records = (response['hits']['hits'])
        print(len(records))
        res = ''


        for d in records:
            print(d['_source']['expiryDate '])
            print(d['_source'].to_dict())
            currentdate = datetime.now()
            dateofrecord = datetime.strptime( d['_source']['expiryDate '], "%m%d%Y %H:%M:%S")

            if len((d['_source']['projectUrl']).strip()) == 0:
                continue;
            if dateofrecord < currentdate:
                continue;
            res = res + json.dumps(d['_source'].to_dict(), indent = 7) + '\n'

        if len(res.strip()) == 0:
            return HttpResponse("no project found")

        return HttpResponse(res, content_type="application/json")

    else:
        return HttpResponse("no project found")


def elasticResponseGeneratePrintOne(response):
    if response['hits']['total'] > 0:
        d = (response['hits']['hits'][0]['_source'])
        currentdate = datetime.now()
        dateofrecord = datetime.strptime( d['expiryDate '], "%m%d%Y %H:%M:%S")
        if len((d['projectUrl']).strip()) == 0:
            return HttpResponse("no project found")
        if dateofrecord < currentdate:
            return HttpResponse("no project found")
        return HttpResponse(json.dumps(d.to_dict(), indent = 7), content_type="application/json")
    else:
        return HttpResponse("no project found")




def getproject(request):
    ''' implementation using regular datastructure'''
    list = results
    print("------------")
    print(len(list))

    if len(request.GET) == 0:  # when no parameters are passed then return the project with highest cost
        map ={}
        logger.info("request with no parameter.")

        max = 0
        temp = {}
        for l in list:
            if l['projectCost'] > max:
                temp = l

        return HttpResponse(json.dumps(temp, indent = 7), content_type="application/json")





    if 'id' in request.GET:      # if id is included then just find the corresponding id and return it
        for l in list:
            val = int(l['id'])
            logger.info("request with id parameter.")

            parameterid = int(request.GET['id'])
            if val == parameterid:
                return HttpResponse(json.dumps(l, indent = 7), content_type="application/json")

        return HttpResponse("no project found")




    res = ''
    for l in list:                  # if id is not present then find the project based on all the parameters passed
        count = 0
        if 'country' in request.GET:
            if request.GET['country'] in l['targetCountries']:
                if checkvalid(l):
                    count = count + 1
        if 'number' in request.GET:
            for key in  l['targetKeys']:
                 if int(key['number']) == int(request.GET['number']):
                     if checkvalid(l):
                        count = count + 1
                        break
        if 'keyword' in request.GET:
            for key in l['targetKeys']:
                if key['keyword'] == request.GET['keyword']:
                    if checkvalid(l):
                        count = count + 1
                        break
        if count == len(request.GET):    # checks if the record satisfies all the parameters or not
            res = res + json.dumps(l, indent = 7)

    if len(res.strip()) != 0:
        return HttpResponse(res, content_type="application/json")

    return HttpResponse("no project found")



def checkvalid(l):       # utility function that checks expiryDate and projectUrl is valid or not
    currentdate = datetime.now()
    dateofrecord = datetime.strptime( l['expiryDate '], "%m%d%Y %H:%M:%S")
    if len((l['projectUrl']).strip()) == 0:
        return False
    if dateofrecord < currentdate:
        return False
    return True





results = documentAddFromFile()    # this is executed only once. It reads all the values from the file once .










































