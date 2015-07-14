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
  try:
	db1=db.odm

  except AttributeError as e:
	log.warn('error: {0}', e)

  
  print("dataset url: "+str(url2))
  
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
	temp_id=str(uuid.uuid3(uuid.NAMESPACE_OID, str(url2)))
	final_json.update({'id':str(temp_id)})
	# check if id exists
	document=db1.find_one({"id":temp_id,"catalogue_url":mainurl})	
	if document==None:
		try:
		  db1.save(final_json)
		  log.info('Metadata stored succesfully to MongoDb.')
		except: pass
	else:
		if len(final_json.keys())>1:
		  met_created=document['metadata_created']
		  final_json.update({'updated_dataset':True})
		  final_json.update({'metadata_created':met_created})
		  final_json.update({'metadata_updated':str(datetime.datetime.now())})
		  db1.remove({"id":temp_id,"catalogue_url":mainurl})
		  db1.save(final_json)
		  log.info('Metadata updated succesfully to MongoDb.')

	print(final_json)

	try:
	  del final_json['_id']
	except: pass
  return json.dumps(final_json)



