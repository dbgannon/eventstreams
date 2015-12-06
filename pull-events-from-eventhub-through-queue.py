
#This is the basic service that pull events from the eventhub via 
#the queue and invokes the azureml sevice and stores the response in
#the table service

from azure.servicebus import ServiceBusService, Message, Queue
import time
import json
import sys
import azure
import socket
import urllib2
import json
import pandas as pd
import numpy as np
import urllib
import json
import pickle
import unicodedata
import nltk
#nltk.download('stopwords')
from nltk.corpus import stopwords
from azure.storage import TableService, Entity, BlobService

# This function sends a request to the azureml service


def sendrequest(datalist, url, api_key):
    #datalist is a list ["class", "document", "title"]
    data =  {
    "Inputs": {

            "input1":
            {
                "ColumnNames": ["class", "document", "title"],
                "Values": [ datalist  ]
            },        },
        "GlobalParameters": { }
    }
    body = str.encode(json.dumps(data))
    headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}
    req = urllib2.Request(url, body, headers) 
    try:
        response = urllib2.urlopen(req)
        result = response.read()
        y = json.loads(result)
        return(y["Results"]["output1"]["value"]["Values"] ) 
    except urllib2.HTTPError, error:
        print("The request failed with status code: " + str(error.code))
       # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
        print(error.info())
        print(json.loads(error.read()))
        return "failed call"                 
   


#this sets up the connection to the queue from the service bus



def processevents(table_service, hostname, bus_service, url, api_key):
    while True:
        timst = str(int(time.time())%10000000)
        try:
            msg = bus_service.receive_queue_message('tasks', peek_lock=False)
            t = msg.body
        except:
            print "queue error"
            t = None
        #print "pulled from queue"
        if t != None:
            #print "got something"
            start =t.find("{")
            if start > 0:
                t = t[start:]  
                x = len(t)
                tt = t[:x-1]
                try:
                    jt = json.loads(tt)
                    title = jt["title"].encode("ascii","ignore")
                    doc = jt["doc"].encode("ascii","ignore")
                    tclass = jt["class"].encode("ascii","ignore")
                    evtime = jt["EventEnqueuedUtcTime"].encode("ascii", "ignore")
                    datalist = [tclass, doc, title]
                    #print "sending data to service " + str(datalist)
                    try:
                        x = sendrequest(datalist, url, api_key)
                    except:
                        x = "failed call"
                    if x == "failed call":
                        print "failed call"
                    else:
                        best = x[0][1]
                        second = x[0][2]
                        rk = hash(title)
                        #print best, second, rk
                        partition = hostname
                        item = {'PartitionKey': partition, 'RowKey': str(rk), 'class': tclass, 'bestguess':best, 'secondguess':second, 
                                    'title': title, 'timestamp': timst, 'enqueued': evtime}
                        #print item
                        try:
                                table_service.insert_entity('scimlevents', item)
                        except:
                                s= "table service error ... likely duplicate"
                                print s
                except:
                    print "bad json object:"+tt
            else:
                print "no json object:"+t

import sys
import getopt

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
#we created four azureML service endpoints to maximize parallelism.
#the program is invoked with one parameter: a number between 0 and 3 that select the
#end point that is used.
#you need to create at least one endpoint
#you also need your stream namespace to listen for events
#and you need a table service to store the results
    endpoints=[]
    url = 'the original service endpoint url'
    api_key = 'the original service key'
    endpoints.extend([[url, api_key]])
    url2 = 'a second enpoint url'
    api_key2 = 'the key for the second'
    endpoints.extend([[url2, api_key2]])
    url3 = 'the third endpoint url'
    api_key3 = 'the key for the third'
    endpoints.extend([[url3, api_key3]])
    url4 = 'the fourth endpint url'
    api_key4 = 'the key for the fourth'
    endpoints.extend([[url4, api_key4]])
        
    if argv is None: 
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "h", ["help"])
        except getopt.error, msg:
            raise Usage(msg)
        bus_service = ServiceBusService(
            service_namespace='your stream',
            shared_access_key_name='listenpolicy',
            shared_access_key_value='your access key')
            
        #next set up the table service
        table_service = TableService(account_name='your table account', account_key='account key')
        table_service.create_table('scimlevents')
        hostname = socket.gethostname()      
        
        if len(args)< 1:
            print "need the endpoint number"
            return 2
        
        endpointid = int(args[0])
        url = endpoints[endpointid][0]
        api_key = endpoints[endpointid][1]
        print url
        print api_key
        print "starting with endpoint "+str(endpointid)
        while True:
            print "running"            
            processevents(table_service, hostname, bus_service, url, api_key)
        print "all done."
    except Usage, err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"
        return 2

if __name__ == "__main__":
    sys.exit(main())