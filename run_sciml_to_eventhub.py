
# coding: utf-8

# To use this you must create an eventhub service, get the key and plug that into the appropriate places below.
# It load the configuration files and all the output of the main topic classifications from the previous set of experiments.
#

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
from ast import literal_eval
from azure.servicebus import (
  _service_bus_error_handler
  )

from azure.servicebus.servicebusservice import (
  ServiceBusService,
  ServiceBusSASAuthentication
  )
from azure.http import (
  HTTPRequest,
  HTTPError
  )

from azure.http.httpclient import _HTTPClient


# In[2]:


def read_config_azure_blob(subj, base):
    #base is the url for the azure blob store
    docpath =base+ "/config_"+subj+".json"
    f = urllib.urlopen(docpath)
    doc = f.read()
    z =json.loads(doc)
    subject = z['subject']
    loadset = z['loadset']
    subtopics = []
    for w in z['supertopics']:
        subtopics.extend([(w[0], w[1])])
    return subject, loadset, subtopics

def load_data(name):
    base = "http://esciencegroup.blob.core.windows.net/scimlpublic"
    docpath =base+ "/"+name+".p"
    f = urllib.urlopen(docpath)
    z = f.read()
    print len(z)
    lst = pickle.loads(str(z))
    titles = []
    sitenames = []
    abstracts = []
    for i in range(0, len(lst)):
        titles.extend([lst[i][0]])
        sitenames.extend([lst[i][1]])
        abstracts.extend([clean(lst[i][2])])

    print "done loading "+name
    return abstracts, sitenames, titles

def load_data2(readtopics, dataset):
    titles, sitenames, disp_title = load_data(dataset)
    bio = []
    sitenames2 = []
    disp_tit2 = []
    ls = [str(x) for x in readtopics]
    siteset = set(ls)
    for i in range(0, len(titles)):
        if sitenames[i] in siteset:
            bio.extend([titles[i]])
            sitenames2.extend([sitenames[i]])
            disp_tit2.extend([disp_title[i]+" ["+sitenames[i]+"]"])

    print len(bio)
    titles = bio
    sitenames = sitenames2
    disp_title = disp_tit2
    return titles, sitenames, disp_title

def make_pandas_set(titles,disp_titles, supertopics):
    #want a table with columns "document", "title", "class"
    dclass = []
    for i in range(0,len(disp_titles)):
        title = disp_titles[i]
        for topic in supertopics:
            for j in topic[1]:
                x = title.find(j)
                if x > 0:
                    dclass.extend([topic[0].encode('ascii','ignore')])
                    title = ''
    dt = []
    for i in range(0,len(disp_titles)):
        dt.extend([disp_titles[i].encode('ascii','ignore')])

    data = {'class': dclass,
        'document': titles,
        'title': dt}
    df = pd.DataFrame(data, columns=['class', 'document', 'title'])             
    #print df
    return df


# We first load all the needed files



def clean(doc):
    st = ""
    sciencestopwords = set([u'model','according', 'data', u'models', 'function', 'properties', 'approach', 'parameters',
                'systems', 'number', 'order', u'data', 'analysis', u'information', u'journal',
                'results','using','research', 'consumers', 'scientists', 'model', 'models', 'journal',
                'researchers','paper','new','study','time','case', 'simulation', u'simulation', 'equation',
                'based','years','better', 'theory', 'particular','many','due','much','set', 'studies', 'systems',
                'simple', 'example','work','non','experiments', 'large', 'small', 'experiment', u'experiments',
                'provide', 'analysis', 'problem', 'method', 'used', 'methods'])

    stopwrds = set(stopwords.words("english")) | sciencestopwords

    wordl = doc.lower().split()
    s = ""
    for word in wordl:
        if word not in stopwrds:
            s = s+" "+word
    return s


# Load all the data


base = "http://esciencegroup.blob.core.windows.net/scimlpublic"
subject, loadset, supertopics =read_config_azure_blob("all4", base)
titles, sitenames, disp_title = load_data2(loadset, "arxiv-11-1-15")
dataframe1 = make_pandas_set(titles, disp_title, supertopics)
tclass = dataframe1["class"]
tdoc = dataframe1["document"]
ttitle = dataframe1["title"]
bigtable = []
for i in range(0, len(tclass)):
    doc = tdoc[i].replace("'","")
    doc = doc.replace('"',"")
    doc = doc.replace('\\x7f','')
    doc = doc.replace('\\','')
    title = ttitle[i].replace("'",'')
    title2 = str(title)
    title = title2.replace('"',"")
    title2 = title.replace('\\x7f','')
    title2 = title2.replace('\\','')
    title = title2.replace("\o", '')
    if title.find("\x7f")>0:
        #print title
        title2 = title.replace('\x7f','')
        print title2
        title = title2
    dictitem = {'class': tclass[i], 'doc': doc, 'title': title}
    bigtable.extend([dictitem])


# In[64]:

jsonlist = []
for item in bigtable:
    x = str(item)
    x = x.replace("'",'"')
    jsonitem = json.loads(x)
    jsonlist.extend([jsonitem])


#The following is the class for sending the message to the eventHub.
#this is where you need to identify
# the eventHubHost,
# your stream service key
# and the eventhub path.

class EventHubClient(object):

  def sendMessage(self,body,partition):
    eventHubHost = "yourstreamservice.servicebus.windows.net"

    httpclient = _HTTPClient(service_instance=self)
    sasKeyName = "SendPolicy"
    sasKeyValue = "your stream service key"

    authentication = ServiceBusSASAuthentication(sasKeyName,sasKeyValue)

    request = HTTPRequest()
    request.method = "POST"
    request.host = eventHubHost
    request.protocol_override = "https"
    request.path = "/youreventhub/publishers/" + partition + "/messages?api-version=2014-05"
    request.body = body
    request.headers.append(('Content-Type', 'application/atom+xml;type=entry;charset=utf-8'))

    authentication.sign_request(request, httpclient)

    request.headers.append(('Content-Length', str(len(request.body))))

    status = 0

    try:
        resp = httpclient.perform_request(request)
        status = resp.status
        #print resp
    except HTTPError as ex:
        print "oops! error"
        status = ex.status

    return status


hubClient = EventHubClient()
hostname = socket.gethostname()

#the following sends 400 messages.
for i in range(100, 500):
    body = str(jsonlist[i])
    body = body.replace("u'","'")
    body = body.replace("'",'"')
    hubStatus = hubClient.sendMessage(body,hostname)

print hubStatus

