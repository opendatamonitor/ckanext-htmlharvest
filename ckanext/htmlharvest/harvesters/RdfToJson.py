import xmltodict
import json
from bs4 import BeautifulSoup
import urllib2
import urllib
import requests
import pymongo
import uuid
import configparser
import logging.config
import GetHarvestRules
import AddLinkToJson
import datetime

log = logging.getLogger(__name__)

# create a file handler
handler = logging.FileHandler('/var/local/ckan/default/pyenv/src/ckanext-htmlharvest/ckanext/htmlharvest/htmlharvest.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
# add the handlers to the logger
log.addHandler(handler)
assert not log.disabled

##read from development.ini file all the required parameters
config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
log_path=config['ckan:odm_extensions']['log_path']
mongoclient=config['ckan:odm_extensions']['mongoclient']
mongoport=config['ckan:odm_extensions']['mongoport']

client = pymongo.MongoClient(str(mongoclient), int(mongoport))

##function that searches for  rdf link and harvests that rdf
def harvest_rdf(url2,rules):
  
  
  db = client.odm
  db_fetch_temp=db.fetch_temp
  db_jobs=db.jobs
  try:
	db1=db.odm

  except AttributeError as e:
	log.warn('error: {0}', e)
	
  #document=db1.aggregate([{ "$group" :{"_id" : "$id", "elements" : { "$sum" : 1}}},
       # {"$match": {"elements": {"$gt":0}}},
        #{"$sort":{"elements":-1}}])
  #j=0
  #ids=[]
  
  #print("dataset url: "+str(url2))
  
  #while j<len(document['result']):
	#ids.append(document['result'][j]['_id'])
	#j+=1
  

  ## reading the rules
  commands=rules['commands']
  links=rules['links']
  ckantags=rules['tags']
  cat_url=rules['cat_url']
  rdf=rules['rdf']
  ckanExtrasLanguage=rules['language']

  
  if 'http://' in cat_url:
	mainurl1=cat_url[cat_url.find('http://')+7:]
	mainurl='http://'+mainurl1[0:mainurl1.find('/')]
  if 'https://' in cat_url:
	mainurl1=cat_url[cat_url.find('https://')+8:]
	mainurl='https://'+mainurl1[0:mainurl1.find('/')]

  try:
	r2  = requests.get(url2)
	data2 = r2.text
  except: data2=''

  soup2 = BeautifulSoup(data2)
  soup3=soup2.findAll(text=True)
  counterl=0
  
  j=0
  k=0

  while j<len(links):
	  while k<len(soup3):
		  if links[j] in soup3[k]:

			  links[j]=soup3[k]
		  k=k+1
	  k=0
	  j=j+1
  j=0
  rdf_url=""

##Searching for rdf link in soup
  while j<len(links):
	format_temp=[]
	format_temp[:]=[]
	formats=[]
	formats[:]=[]
	
	if links[j] in soup3:

	  for x in range(0,len(soup3)):

		if soup3[x]==links[j]:

			parent=str(soup3[x].parent.parent)
			parent1=BeautifulSoup(parent)
			parent2=parent1.find_all(text=True)
			parent3=parent1.find_all('a', {'href': True})
			
			# for links
			counter=0
			while counter<len(parent3):
				url_temp=str(parent3[counter]['href'].encode('utf-8').lstrip().lower())
				if 'rdf' in url_temp:
				  if 'http' not in url_temp:
					  if url_temp[0]=='/':
						  url_temp=mainurl+url_temp
					  else:
						  url_temp=mainurl+'/'+url_temp				  
				  rdf_url=url_temp
				counter+=1
	j+=1
	
  #print(rdf_url)
  
  final_json={}
  
  if rdf_url!='':
	xml_str = urllib.urlopen(rdf_url).read()
	#print(xml_str)

	##convert rdf to json
	xmlparse = xmltodict.parse(xml_str)

	#convert json to string
	str_json=json.dumps(xmlparse)


	##DCAT CASE:

	#Basic fields
	final_json.update({'url':url2})
	final_json.update({'title':xmlparse['rdf:RDF']['dcat:Dataset']['dct:title']})
	final_json.update({'notes':xmlparse['rdf:RDF']['dcat:Dataset']['dct:description'].replace(':','')})
	final_json.update({'license_id':xmlparse['rdf:RDF']['dcat:Dataset']['dct:licence']})
	final_json.update({'author':xmlparse['rdf:RDF']['dcat:Dataset']['dct:creator']})
	final_json.update({'organization':{'title':xmlparse['rdf:RDF']['dcat:Dataset']['dct:publisher'],"type" : "organization"}})
	final_json.update({'metadata_created':str(datetime.datetime.now())})
	final_json.update({'catalogue_url':mainurl})
	final_json.update({'platform':'html'})


	#Extras
	extras=[]
	extras.append({'key':'category','value':xmlparse['rdf:RDF']['dcat:Dataset']['dcat:theme']})
	extras.append({'key':'date_released','value':xmlparse['rdf:RDF']['dcat:Dataset']['dct:created']})
	extras.append({'key':'date_updated','value':xmlparse['rdf:RDF']['dcat:Dataset']['dct:modified']})
	extras.append({'key':'temporal_coverage','value':xmlparse['rdf:RDF']['dcat:Dataset']['dct:temporal']})
	extras.append({'key':'language','value':ckanExtrasLanguage})
	final_json.update({'extras':extras})


	#Tags
	tags=xmlparse['rdf:RDF']['dcat:Dataset']['dcat:keywords']
	if tags!=None and tags!="":
	  if ',' in tags:
		tags=tags.split(',')
		i=0
		while i<len(tags):
		  tags[i]=tags[i].replace(" ","")
		  i+=1
	final_json.update({'tags':tags})


	#Resources
	try:
		resources_list=xmlparse['rdf:RDF']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']
		resources=[]
		i=0
		while i<len(resources_list):
		  resource={}
		  try:
			resource.update({'url':str(xmlparse['rdf:RDF']['dcat:Dataset']['dcat:distribution']['dcat:Distribution'][i]['dcat:accessURL'])})
			resource.update({'format':str(xmlparse['rdf:RDF']['dcat:Dataset']['dcat:distribution']['dcat:Distribution'][i]['dct:format']).lower()})
			resources.append(resource)
		  except KeyError:pass
		  
		  i+=1
		final_json.update({'resources':resources})
	except:pass
	temp_id=str(uuid.uuid3(uuid.NAMESPACE_OID, str(url2)))
	final_json.update({'id':str(temp_id)})
	# check if id exists
	document=db1.find_one({"id":temp_id,"catalogue_url":mainurl})	
	if document==None:
	#if temp_id not in ids:
		try:
		  db1.save(final_json)
		  log.info('Metadata stored succesfully to MongoDb.')
		  fetch_document=db_fetch_temp.find_one()
		  if fetch_document==None:
			fetch_document={}
			fetch_document.update({"cat_url":mainurl})
			fetch_document.update({"new":1})
			fetch_document.update({"updated":0})
			db_fetch_temp.save(fetch_document)
		  else:
			if mainurl==fetch_document['cat_url']:
			  new_count=fetch_document['new']
			  new_count+=1
			  fetch_document.update({"new":new_count})
			  db_fetch_temp.save(fetch_document)
			else:
			  last_cat_url=fetch_document['cat_url']
			  doc=db_jobs.find_one({'cat_url':fetch_document['cat_url']})
			  if 'new' in fetch_document.keys():
				new=fetch_document['new']
				if 'new' in doc.keys():
				  last_new=doc['new']
				  doc.update({"last_new":last_new})
				doc.update({"new":new})
				db_jobs.save(doc)
			  if 'updated' in fetch_document.keys():
				updated=fetch_document['updated']
				if 'updated' in doc.keys():
				  last_updated=doc['updated']
				  doc.update({"last_updated":last_updated})
				doc.update({"updated":updated})
				db_jobs.save(doc)
			  fetch_document.update({"cat_url":mainurl})
			  fetch_document.update({"new":1})
			  fetch_document.update({"updated":0})
			  db_fetch_temp.save(fetch_document)
		except: pass
	else:
		if len(final_json.keys())>1:
		  #document=db1.find_one({"id":temp_id})
		  met_created=document['metadata_created']
		  if 'copied' in document.keys():
			final_json.update({'copied':document['copied']})
		  final_json.update({'updated_dataset':True})
		  final_json.update({'metadata_created':met_created})
		  final_json.update({'metadata_updated':str(datetime.datetime.now())})
		  #db1.remove({"id":temp_id})
		  temp_id=document['_id']
		  final_json.update({"_id":temp_id})
		  db1.save(final_json)
		  log.info('Metadata updated succesfully to MongoDb.')
		  fetch_document=db_fetch_temp.find_one()
		  #print(fetch_document)
		  if fetch_document==None:
			fetch_document={}
			fetch_document.update({"cat_url":mainurl})
			fetch_document.update({"updated":1})
			fetch_document.update({"new":0})
			db_fetch_temp.save(fetch_document)
		  else:
			if mainurl==fetch_document['cat_url']:
			  updated_count=fetch_document['updated']
			  updated_count+=1
			  fetch_document.update({"updated":updated_count})
			  db_fetch_temp.save(fetch_document)
			else:
			  last_cat_url=fetch_document['cat_url']
			  #print(fetch_document['cat_url'])
			  doc=db_jobs.find_one({'cat_url':fetch_document['cat_url']})
			  if 'new' in fetch_document.keys():
				new=fetch_document['new']
				if 'new' in doc.keys():
				  last_new=doc['new']
				  doc.update({"last_new":last_new})
				doc.update({"new":new})
				db_jobs.save(doc)
			  if 'updated' in fetch_document.keys():
				updated=fetch_document['updated']
				if 'updated' in doc.keys():
				  last_updated=doc['updated']
				  doc.update({"last_updated":last_updated})
				doc.update({"updated":updated})
				db_jobs.save(doc)
			  fetch_document.update({"cat_url":mainurl})
			  fetch_document.update({"updated":1})
			  fetch_document.update({"new":0})
			  db_fetch_temp.save(fetch_document)

	#print(final_json)

	try:
	  del final_json['_id']
	except: pass
  return json.dumps(final_json)



