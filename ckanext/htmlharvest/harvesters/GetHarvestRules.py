import urllib2
import requests
from xml.dom.minidom import parseString
import re
import difflib
import json
import bson
import pymongo
import hashlib
import time
import datetime
import uuid
import configparser
import sys

#setting encoding to utf-8
reload(sys)
sys.setdefaultencoding("utf-8")

##read from development.ini file all the required parameters
config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
mongoclient=config['ckan:odm_extensions']['mongoclient']
mongoport=config['ckan:odm_extensions']['mongoport']
client = pymongo.MongoClient(str(mongoclient), int(mongoport))


def GetHarvestRules(cat_url):
	commands=[]
	label=[]
	xpath = {}
	links=[]
	rules={}
	db2 = client.odm
	collection=db2.html_jobs
	
	##find the cat_url's rules in mongoDB
	#print(cat_url)
	document=collection.find_one({"cat_url":{'$regex': cat_url}})
	if document==None:
		document=collection.find_one({"url":{'$regex': cat_url}})
	#temp_id=document['_id']
	#post_id=bson.ObjectId(temp_id)
	#job=collection.find_one({"_id":post_id})
	job=document



	url1=str(job['url'])
	rules.update({"url":str(url1)})
	url=str(job['cat_url'].encode('utf-8'))
	rules.update({"cat_url":str(url)})
	if 'btn_identifier' in rules.keys():
		rules.update({"btn_identifier":str(job['btn_identifier'])})
	id1=str(job['step'])
	if id1!=None and id1!="":
	  rules.update({"step":int(id1)})
	afterid=str(job['afterurl'])
	rules.update({"afterurl":str(afterid)})

	if 'rdf' in job.keys():
	  rdf=job['rdf']
	  if rdf!="" and rdf!=None:
		links.append(rdf)
		rules.update({"rdf":rdf})  


	dataset_keyword2=str(job['identifier'])
	if ',' in dataset_keyword2:
		dataset_keyword3=dataset_keyword2.split(',')
		dataset_keyword=dataset_keyword3[0]
		dataset_keyword1=dataset_keyword3[1]
		
	else:
		dataset_keyword=dataset_keyword2
		dataset_keyword1="nothingatall"
	rules.update({"identifier":str(dataset_keyword2)})

	
	title=job['title']
	title1=title.split('@/@')
	if title1[1]=='xpath' and title1[0]:
		xpath['title']=title1[0]
	elif title1[0]!="":
	  commands.append(title1[0])
	ckantitle=title1[0]
	rules.update({"title":str(ckantitle)})

	notes=job['notes']
	notes1=notes.split('@/@')
	if notes1[1]=='value' and notes1[0]!='':
		commands.append(notes1[0])
	if notes1[1]=='label' and notes1[0]!='':
		label.append(notes1[0])
	if notes1[1]=='link' and notes1[0]!='':
		links.append(notes1[0])
	ckannotes=notes1[0]
	rules.update({"notes":str(ckannotes)})

	author=job['author']
	author1=author.split('@/@')
	if author1[1]=='value' and author1[0]!='':
		commands.append(author1[0])
	if author1[1]=='label' and author1[0]!='':
		label.append(author1[0])
	if author1[1]=='link' and author1[0]!='':
		links.append(author1[0])
	ckanauthor=author1[0]
	rules.update({"author":str(ckanauthor)})

	country=job['country']
	country1=country.split('@/@')
	if country1[1]=='value' and country1[0]!='':
		commands.append(country1[0])
	if country1[1]=='label' and country1[0]!='':
		label.append(country1[0])
	if country1[1]=='link' and country1[0]!='':
		links.append(country1[0])
	ckancountry=country1[0]
	rules.update({"country":str(ckancountry)})

	temporal_coverage=job['temporal_coverage']
	temporal_coverage1=temporal_coverage.split('@/@')
	if temporal_coverage1[1]=='value' and temporal_coverage1[0]!='':
		commands.append(temporal_coverage1[0])
	if temporal_coverage1[1]=='label' and temporal_coverage1[0]!='':
		label.append(temporal_coverage1[0])
	if temporal_coverage1[1]=='link' and temporal_coverage1[0]!='':
		links.append(temporal_coverage1[0])
	ckantemporalcoverage=temporal_coverage1[0]
	rules.update({"temporal_coverage":str(ckantemporalcoverage)})

	date_released=job['date_released']
	date_released1=date_released.split('@/@')
	if date_released1[1]=='value' and date_released1[0]!='':
		commands.append(date_released1[0])
	if date_released1[1]=='label' and date_released1[0]!='':
		label.append(date_released1[0])
	if date_released1[1]=='link' and date_released1[0]!='':
		links.append(date_released1[0])
	ckandate_released=date_released1[0]
	rules.update({"date_released":str(ckandate_released)})

	author_email=job['author_email']
	author_email1=author_email.split('@/@')
	if author_email1[1]=='value' and author_email1[0]!='':
		commands.append(author_email1[0])
	if author_email1[1]=='label' and author_email1[0]!='':
		label.append(author_email1[0])
	if author_email1[1]=='link' and author_email1[0]!='':
		links.append(author_email1[0])
	ckanauthor_email=author_email1[0]
	rules.update({"author_email":str(ckanauthor_email)})

	tags=job['tags']
	tags1=tags.split('@/@')
	if tags1[1]=='value' and tags1[0]!='':
		commands.append(tags1[0])
	if tags1[1]=='label' and tags1[0]!='':
		label.append(tags1[0])
	if tags1[1]=='link' and tags1[0]!='':
		links.append(tags1[0])
	ckantags=tags1[0]
	rules.update({"tags":str(ckantags)})

	resource=job['resource']
	resource1=resource.split('@/@')
	if resource1[1]=='xpath' and resource1[0]!='':
		xpath['resource']=resource1[0]
	if resource1[1]=='value' and resource1[0]!='':
		commands.append(resource1[0])
	if resource1[1]=='label' and resource1[0]!='':
		label.append(resource1[0])
	if resource1[1]=='link' and resource1[0]!='':
		links.append(resource1[0])
	ckanresource=resource1[0]
	rules.update({"resource":str(ckanresource)})


	licence=job['license']
	licence1=licence.split('@/@')
	if licence1[1]=='label' and licence1[0]!='':
		label.append(licence1[0])
	if licence1[1]=='value' and licence1[0]!='':
		commands.append(licence1[0])
	if licence1[1]=='link' and licence1[0]!='':
		links.append(licence1[0])
	ckanlicense=licence1[0]
	rules.update({"license":str(ckanlicense)})

	date_updated=job['date_updated']
	date_updated1=date_updated.split('@/@')
	if date_updated1[1]=='value' and date_updated1[0]!='':
		commands.append(date_updated1[0])
	if date_updated1[1]=='label' and date_updated1[0]!='':
		label.append(date_updated1[0])
	if date_updated1[1]=='link' and date_updated1[0]!='':
		links.append(date_updated1[0])
	ckandate_updated=date_updated1[0]
	rules.update({"date_updated":str(ckandate_updated)})


	organization=job['organization']
	organization1=organization.split('@/@')
	if organization1[1]=='value' and organization1[0]!='':
		commands.append(organization1[0])
	if organization1[1]=='label' and organization1[0]!='':
		label.append(organization1[0])
	if organization1[1]=='link' and organization1[0]!='':
		links.append(organization1[0])
	ckanorganization=organization1[0]
	rules.update({"organization":str(ckanorganization)})



	maintainer_email=job['maintainer_email']
	maintainer_email1=maintainer_email.split('@/@')
	if maintainer_email1[1]=='value' and maintainer_email1[0]!='':
		commands.append(maintainer_email1[0])
	if maintainer_email1[1]=='label' and maintainer_email1[0]!='':
		label.append(maintainer_email1[0])
	if maintainer_email1[1]=='link' and maintainer_email1[0]!='':
		links.append(maintainer_email1[0])
	ckanmaintainer_email=maintainer_email1[0]
	rules.update({"maintainer_email":str(ckanmaintainer_email)})


	state=job['state']
	state1=state.split('@/@')
	if state1[1]=='value' and state1[0]!='':
		commands.append(state1[0])
	if state1[1]=='label' and state1[0]!='':
		label.append(state1[0])
	if state1[1]=='link' and state1[0]!='':
		links.append(state1[0])
	ckanstate=state1[0]	
	rules.update({"state":str(ckanstate)})

	city=job['city']
	city1=city.split('@/@')
	if city1[1]=='value' and city1[0]!='':
		commands.append(city1[0])
	if city1[1]=='label' and city1[0]!='':
		label.append(city1[0])
	if city1[1]=='link' and city1[0]!='':
		links.append(city1[0])
	ckancity=city1[0]
	rules.update({"city":str(ckancity)})


	category=job['category']
	category1=category.split('@/@')
	if category1[1]=='value' and category1[0]!='':
		commands.append(category1[0])
	if category1[1]=='label' and category1[0]!='':
		label.append(category1[0])
	if category1[1]=='link' and category1[0]!='':
		links.append(category1[0])
	ckanExtrasCategory=category1[0]
	rules.update({"category":str(ckanExtrasCategory)})

	frequency=job['frequency']
	frequency1=frequency.split('@/@')
	if frequency1[1]=='value' and frequency1[0]!='':
		commands.append(frequency1[0])
	if frequency1[1]=='label' and frequency1[0]!='':
		label.append(frequency1[0])
	if frequency1[1]=='link' and frequency1[0]!='':
		links.append(frequency1[0])
	ckanExtrasFrequency=frequency1[0]
	rules.update({"frequency":str(ckanExtrasFrequency)})


	language=job['language']
	ckanExtrasLanguage=language
	rules.update({"language":str(ckanExtrasLanguage)})


	maintainer=job['maintainer']
	maintainer1=maintainer.split('@/@')
	if maintainer1[1]=='value' and maintainer1[0]!='':
		commands.append(maintainer1[0])
	if maintainer1[1]=='label' and maintainer1[0]!='':
		label.append(maintainer1[0])
	if maintainer1[1]=='link' and maintainer1[0]!='':
		links.append(maintainer1[0])
	ckanMaintainer=maintainer1[0]
	rules.update({"maintainer":str(ckanMaintainer)})
	rules.update({"label":label})
	rules.update({"links":links})
	rules.update({"commands":commands})
	rules.update({'xpath':xpath})
	try:
	  step=int(id1)
	except:
	  step=""
	return rules

