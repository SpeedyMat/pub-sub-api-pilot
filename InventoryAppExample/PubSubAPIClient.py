from __future__ import print_function
import grpc
import requests
import threading
import io
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc

import avro.schema
import avro.io
import time
import certifi
import xml.etree.ElementTree as ET

semaphore = threading.Semaphore(1)

latest_replay_id = None

with open(certifi.where(), 'rb') as f: creds = grpc.ssl_channel_credentials(f.read())
#with grpc.secure_channel('eventbusapi-core1.sfdc-ypmv18.svc.sfdcfc.net:7443',creds) as channel:
with grpc.secure_channel('https://api.pilot.pubsub.salesforce.com:7443',creds) as channel:

    username = "Matthew.Hallworth_dev@synectics-solutions.com"
    password = "Password goes here"
    url = 'https://login.salesforce.com/services/Soap/u/52.0/'
    headers = {'content-type': 'text/xml', 'SOAPAction': 'login'}
    xml = "<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body> <urn:login><urn:username><![CDATA[" + username + "]]></urn:username><urn:password><![CDATA[" + password + "]]></urn:password></urn:login></soapenv:Body></soapenv:Envelope>"
    #https://2.python-requests.org/en/master/
    res = requests.post(url, data=xml, headers=headers, verify=False)
#Optionally, print the content field returned
#print(res.content)
#print('here')
print(res.text)



#testXML = '<?xml version="1.0"?><actors xmlns:fictional="http://characters.example.com" xmlns="http://people.example.com"><actor><name>John Cleese</name><fictional:character>Lancelot</fictional:character><fictional:character>Archie Leach</fictional:character></actor><actor><name>Eric Idle</name><fictional:character>Sir Robin</fictional:character><fictional:character>Gunther</fictional:character><fictional:character>Commander Clement</fictional:character></actor></actors>'
#testXMLRoot = ET.fromstring(testXML)
#responsecontentstring = res.content.decode('UTF-8')
#responseXmlRoot = ET.fromstring(res.text)

'''
ns = {'soapenv':'http://schemas.xmlsoap.org/soap/envelope/', 'xsi':'http://www.w3.org/2001/XMLSchema-instance', 'urn':'partner.soap.sforce.com'}
print('here')
for env in responseXmlRoot.findall('result', ns):
    loginResponseEl = env.find('loginResponse', ns)
    print(loginResponseEl.text)
'''


res_xmlRoot = ET.fromstring(res.content.decode('utf-8'))[0][0][0]
session_id = res_xmlRoot[4].text
instance_URL = 'https://synecticssolutionsltd4-dev-ed.my.salesforce.com'
org_id = '00D4L000000qL6HUAU'

#session ID, instance URL, org ID
authmetadata = (session_id, instance_URL, org_id)

stub = pb2_grpc.PubSubStub(channel)



semaphore = threading.Semaphore(1)

'''
This is a Generator function to make a FetchRequest stream
In this FetchRequest, num_requested is the maximum number of events that the server can send to the
client at once. The specified num_requested of events can be sent in one or more
FetchResponses, with each FetchResponse containing a batch of events. In this case,
we set it to 1 but it can be any number, depending on how many events we want to process.
'''
def fetchReqStream(topic):
     while True:
                semaphore.acquire()
                yield pb2.FetchRequest(topic_name = topic,replay_preset = pb2.ReplayPreset.LATEST,num_requested = 10)

# A decoding function to decode the payloads of received event messages
def decode(schema, payload):
        schema = avro.schema.Parse(schema)
        buf = io.BytesIO(payload)
        decoder = avro.io.BinaryDecoder(buf)
        reader = avro.io.DatumReader(writer_schema=schema)
        ret = reader.read(decoder)
        return ret

'''
The topic format is:
○ For a custom platform event: /event/EventName__e
○ For a standard platform event: /event/EventName
○ For a change data capture channel that captures events for all selected entities:
/data/ChangeEvents
○ For a change data capture single-entity channel for a standard object:
/data/<StandardObjectName>ChangeEvent
○ For a change data capture single-entity channel for a custom object:
/data/<CustomObjectName>__ChangeEvent
○ For a change data capture custom channel:
/data/CustomChannelName__chn
'''

print('about to subscribe')

mysubtopic = "/data/OpportunityChangeEvent"
#mysubtopic = "/data/ChangeEvents"
substream = stub.Subscribe(fetchReqStream(mysubtopic),metadata=authmetadata)
print('subscribed')
for event in substream:
	semaphore.release()
	if event.events:
		payloadbytes = event.events[0].event.payload
		schemaid = event.events[0].event.schema_id
		schema = stub.GetSchema(pb2.SchemaRequest(schema_id=schemaid),metadata=authmetadata).schema_json
		decoded = decode(schema, payloadbytes)
		print("Got an event!", decoded)
	else:
		print("[", time.strftime('%b %d, %Y %l:%M%p %Z'),"] The subscription is active.")
	latest_replay_id = event.latest_replay_id


#sessionid = res._content.
#instanceurl = <the server URL you just got from the XML
#ending in .com>
#tenantid = <org ID>

#authmetadata = (('accesstoken', sessionid),
#('instanceurl', instanceurl),
#('tenantid', tenantid))