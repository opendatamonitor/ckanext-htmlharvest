from bs4 import BeautifulSoup
import urllib2
import urllib
import pprint
import requests
import time
from xml.dom.minidom import parseString
import re
import difflib
import json
import bson
import logging
import SaveLabels
import pymongo
import uuid
import hashlib
import AddNoLabelToJson
import AddLabelToJson
import AddLinkToJson
import HarvestProcedure
import configparser



##read from development.ini file all the required parameters
config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
log_path=config['ckan:odm_extensions']['log_path']
html_harvester_log_file_path=str(log_path)+'ckanext-htmlharvest/ckanext/htmlharvest/harvesters/html1.log'
mongoclient=config['ckan:odm_extensions']['mongoclient']
mongoport=config['ckan:odm_extensions']['mongoport']


text_file = open(str(html_harvester_log_file_path), "a")



ahref=""

#--list of non label data
nonlabel=[]
a_link=[]
# list of label data
label=[]
ids=[]
ids1=[]
links=[]
all_ids=[]
parent_attrs_list=[]
parent_name_list=[]
parent_name_list1=[]
parent_attrs_list1=[]
commands=[]


next=""
endpoint=""
url=""
step=""
mainurl=""
dataset_keyword=""
dataset_keyword1=""

#-- after id is for sites that use a pattern like data.gouv.fr/id=1/something else  (afterid==something else)
afterid=""


#-- list for label elements
labels=[]
labelshigher=[]
labelshighest=[]
ids=[]

log = logging.getLogger(__name__)
client = pymongo.MongoClient(str(mongoclient), int(mongoport))


def StartHarvestProcedure(commands,label,links,url,step,afterid,endpoint,dataset_keyword,dataset_keyword1,ckannotes,ckanlicense,ckanresource,ckantags,ckanauthor_email,ckanauthor,ckantitle,ckandate_updated,ckanExtrasCategory
	,ckanExtrasFrequency,ckanExtrasLanguage,ckanMaintainer,ckandate_released,ckancountry,ckantemporalcoverage,backupi,ckanorganization,ckanmaintainer_email,ckanstate,ckancity):

	#text_file.write('this is non label :'+str(nonlabel))
	if backupi>0:
	  i=backupi
	else:
	  i=0
	j=0
	k=0
	l=0
	type1=""
	mainurl1=url[url.find('http://')+7:]
	mainurl='http://'+mainurl1[0:mainurl1.find('/')]
	jason="{"
	ckanjason='{'
	dbname=mainurl1[0:mainurl1.find('/')]
	dbname1=dbname.replace('.','_')
	print(str(dbname))
	soup1=endpoint
	db = client.odm


	try:
	  db1=db.odm
	 

	except AttributeError as e:
	        log.warn('error: {0}', e)

	#document=db1.aggregate([{ "$group" :{"_id" : "$id", "elements" : { "$sum" : 1}}},
        #{"$match": {"elements": {"$gt":0}}},
        #{"$sort":{"elements":-1}}])
	#j=0
	#while j<len(document['result']):
		#ids.append(document['result'][j]['_id'])
		#j+=1


# call harvest procedure function:

	dataset_urls=HarvestProcedure.ProcedureWithNext(soup1,dataset_keyword,dataset_keyword1,mainurl,text_file,ckanjason,commands,ckannotes,ckanlicense,ckanresource,ckantags,ckanauthor_email,ckanauthor,j,label,k,a_link,
			  links,type1,jason,db1,endpoint,url,i,afterid,step,ckantitle,ckandate_updated,ckanExtrasCategory,ckanExtrasFrequency,ckanExtrasLanguage,ckanMaintainer,ckandate_released
			  ,ckancountry,ckantemporalcoverage,ckanorganization,ckanmaintainer_email,ckanstate,ckancity)
	return dataset_urls


def read_data(temp_id,backupi):

	db2 = client.odm
	collection=db2.html_jobs
	post_id=bson.ObjectId(temp_id)
	job=collection.find_one({"_id":post_id})


	url1=str(job['url'])
	url=str(job['cat_url'].encode('utf-8'))
	
	id1=str(job['step'])
	afterid=str(job['afterurl'])
	


	dataset_keyword2=str(job['identifier'])
	if '@/@xpath' in dataset_keyword2:
		dataset_keyword=dataset_keyword2
		dataset_keyword1="nothingatall"
	elif ',' in dataset_keyword2 and '@/@xpath' not in dataset_keyword2:
		dataset_keyword3=dataset_keyword2.split(',')
		dataset_keyword=dataset_keyword3[0]
		dataset_keyword1=dataset_keyword3[1]
		
	else:
		dataset_keyword=dataset_keyword2
		dataset_keyword1="nothingatall"

	
	title=job['title']
	title1=title.split('@/@')
	if title1[0]!="":
	  commands.append(title1[0])
	ckantitle=title1[0]

	notes=job['notes']
	notes1=notes.split('@/@')
	if notes1[1]=='value' and notes1[0]!='':
		commands.append(notes1[0])
	if notes1[1]=='label' and notes1[0]!='':
		label.append(notes1[0])
	if notes1[1]=='link' and notes1[0]!='':
		links.append(notes1[0])
	ckannotes=notes1[0]

	author=job['author']
	author1=author.split('@/@')
	if author1[1]=='value' and author1[0]!='':
		commands.append(author1[0])
	if author1[1]=='label' and author1[0]!='':
		label.append(author1[0])
	if author1[1]=='link' and author1[0]!='':
		links.append(author1[0])
	ckanauthor=author1[0]

	country=job['country']
	country1=country.split('@/@')
	if country1[1]=='value' and country1[0]!='':
		commands.append(country1[0])
	if country1[1]=='label' and country1[0]!='':
		label.append(country1[0])
	if country1[1]=='link' and country1[0]!='':
		links.append(country1[0])
	ckancountry=country1[0]

	temporal_coverage=job['temporal_coverage']
	temporal_coverage1=temporal_coverage.split('@/@')
	if temporal_coverage1[1]=='value' and temporal_coverage1[0]!='':
		commands.append(temporal_coverage1[0])
	if temporal_coverage1[1]=='label' and temporal_coverage1[0]!='':
		label.append(temporal_coverage1[0])
	if temporal_coverage1[1]=='link' and temporal_coverage1[0]!='':
		links.append(temporal_coverage1[0])
	ckantemporalcoverage=temporal_coverage1[0]

	date_released=job['date_released']
	date_released1=date_released.split('@/@')
	if date_released1[1]=='value' and date_released1[0]!='':
		commands.append(date_released1[0])
	if date_released1[1]=='label' and date_released1[0]!='':
		label.append(date_released1[0])
	if date_released1[1]=='link' and date_released1[0]!='':
		links.append(date_released1[0])
	ckandate_released=date_released1[0]

	author_email=job['author_email']
	author_email1=author_email.split('@/@')
	if author_email1[1]=='value' and author_email1[0]!='':
		commands.append(author_email1[0])
	if author_email1[1]=='label' and author_email1[0]!='':
		label.append(author_email1[0])
	if author_email1[1]=='link' and author_email1[0]!='':
		links.append(author_email1[0])
	ckanauthor_email=author_email1[0]

	tags=job['tags']
	tags1=tags.split('@/@')
	if tags1[1]=='value' and tags1[0]!='':
		commands.append(tags1[0])
	if tags1[1]=='label' and tags1[0]!='':
		label.append(tags1[0])
	if tags1[1]=='link' and tags1[0]!='':
		links.append(tags1[0])
	ckantags=tags1[0]

	resource=job['resource']
	resource1=resource.split('@/@')
	if resource1[1]=='value' and resource1[0]!='':
		commands.append(resource1[0])
	if resource1[1]=='label' and resource1[0]!='':
		label.append(resource1[0])
	if resource1[1]=='link' and resource1[0]!='':
		links.append(resource1[0])
	ckanresource=resource1[0]


	licence=job['license']
	licence1=licence.split('@/@')
	if licence1[1]=='label' and licence1[0]!='':
		label.append(licence1[0])
	if licence1[1]=='value' and licence1[0]!='':
		commands.append(licence1[0])
	if licence1[1]=='link' and licence1[0]!='':
		links.append(licence1[0])
	ckanlicense=licence1[0]

	date_updated=job['date_updated']
	date_updated1=date_updated.split('@/@')
	if date_updated1[1]=='value' and date_updated1[0]!='':
		commands.append(date_updated1[0])
	if date_updated1[1]=='label' and date_updated1[0]!='':
		label.append(date_updated1[0])
	if date_updated1[1]=='link' and date_updated1[0]!='':
		links.append(date_updated1[0])
	ckandate_updated=date_updated1[0]


	organization=job['organization']
	organization1=organization.split('@/@')
	if organization1[1]=='value' and organization1[0]!='':
		commands.append(organization1[0])
	if organization1[1]=='label' and organization1[0]!='':
		label.append(organization1[0])
	if organization1[1]=='link' and organization1[0]!='':
		links.append(organization1[0])
	ckanorganization=organization1[0]	



	maintainer_email=job['maintainer_email']
	maintainer_email1=maintainer_email.split('@/@')
	if maintainer_email1[1]=='value' and maintainer_email1[0]!='':
		commands.append(maintainer_email1[0])
	if maintainer_email1[1]=='label' and maintainer_email1[0]!='':
		label.append(maintainer_email1[0])
	if maintainer_email1[1]=='link' and maintainer_email1[0]!='':
		links.append(maintainer_email1[0])
	ckanmaintainer_email=maintainer_email1[0]


	state=job['state']
	state1=state.split('@/@')
	if state1[1]=='value' and state1[0]!='':
		commands.append(state1[0])
	if state1[1]=='label' and state1[0]!='':
		label.append(state1[0])
	if state1[1]=='link' and state1[0]!='':
		links.append(state1[0])
	ckanstate=state1[0]	


	city=job['city']
	city1=city.split('@/@')
	if city1[1]=='value' and city1[0]!='':
		commands.append(city1[0])
	if city1[1]=='label' and city1[0]!='':
		label.append(city1[0])
	if city1[1]=='link' and city1[0]!='':
		links.append(city1[0])
	ckancity=city1[0]	


	category=job['category']
	category1=category.split('@/@')
	if category1[1]=='value' and category1[0]!='':
		commands.append(category1[0])
	if category1[1]=='label' and category1[0]!='':
		label.append(category1[0])
	if category1[1]=='link' and category1[0]!='':
		links.append(category1[0])
	ckanExtrasCategory=category1[0]


	frequency=job['frequency']
	frequency1=frequency.split('@/@')
	if frequency1[1]=='value' and frequency1[0]!='':
		commands.append(frequency1[0])
	if frequency1[1]=='label' and frequency1[0]!='':
		label.append(frequency1[0])
	if frequency1[1]=='link' and frequency1[0]!='':
		links.append(frequency1[0])
	ckanExtrasFrequency=frequency1[0]


	language=job['language']
	ckanExtrasLanguage=language


	maintainer=job['maintainer']
	maintainer1=maintainer.split('@/@')
	if maintainer1[1]=='value' and maintainer1[0]!='':
		commands.append(maintainer1[0])
	if maintainer1[1]=='label' and maintainer1[0]!='':
		label.append(maintainer1[0])
	if maintainer1[1]=='link' and maintainer1[0]!='':
		links.append(maintainer1[0])
	ckanMaintainer=maintainer1[0]
	try:
	  step=int(id1)
	except ValueError:
	  step=""

## call function  StartHarvestProcedure
	dataset_urls=StartHarvestProcedure(commands,label,links,url,step,afterid,endpoint,dataset_keyword,dataset_keyword1,ckannotes,ckanlicense,ckanresource,ckantags,ckanauthor_email,ckanauthor,ckantitle,ckandate_updated,ckanExtrasCategory
	,ckanExtrasFrequency,ckanExtrasLanguage,ckanMaintainer,ckandate_released,ckancountry,ckantemporalcoverage,backupi,ckanorganization,ckanmaintainer_email,ckanstate,ckancity)
	return dataset_urls


