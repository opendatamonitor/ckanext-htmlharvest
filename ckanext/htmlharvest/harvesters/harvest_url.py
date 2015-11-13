from bs4 import BeautifulSoup
import urllib2
import requests
from xml.dom.minidom import parseString
import re
import difflib
import json
import logging
import SaveLabels
import pymongo
import harvester_final
import AddNoLabelToJson
import AddLabelToJson
import AddLinkToJson
import hashlib
import time
import datetime
import uuid
import configparser
import logging.config
import GetHarvestRules
import AddXPathToJson
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
html_harvester_log_file_path=str(log_path)+'ckanext-htmlharvest/ckanext/htmlharvest/harvesters/html1.log'
mongoclient=config['ckan:odm_extensions']['mongoclient']
mongoport=config['ckan:odm_extensions']['mongoport']
text_file = open(str(html_harvester_log_file_path), "a")
client = pymongo.MongoClient(str(mongoclient), int(mongoport))

##function that harvests a specific url according to the harvest rules
def harvest_url(url2,rules):
  
  
  db = client.odm
  db_fetch_temp=db.fetch_temp
  db_jobs=db.jobs
  try:
	db1=db.odm

  except AttributeError as e:
	log.warn('error: {0}', e)
	

  
  a_link=[]
  type1=""
  jason='{'
  content={}
  ckanjson2={}
  ## reading the rules
  commands=rules['commands']
  label=rules['label']
  links=rules['links']
  ckannotes=rules['notes']
  ckanlicense=rules['license']
  ckanresource=rules['resource']
  ckantags=rules['tags']
  ckanauthor_email=rules['author_email']
  ckanauthor=rules['author']
  ckantitle=rules['title']
  ckandate_updated=rules['date_updated']
  ckanExtrasCategory=rules['category']
  ckanExtrasFrequency=rules['frequency']
  ckanExtrasLanguage=rules['language']
  ckanMaintainer=rules['maintainer']
  ckandate_released=rules['date_released']
  ckancountry=rules['country']
  ckantemporalcoverage=rules['temporal_coverage']
  ckanorganization=rules['organization']
  ckanmaintainer_email=rules['maintainer_email']
  ckanstate=rules['state']
  ckancity=rules['city']
  cat_url=rules['cat_url']
  try:
  	btn_identifier=rules['btn_identifier']
  except:
	btn_identifier=""
  try:
	rdf=rules['rdf']
  except:
	rdf=""

  if 'http://' in cat_url:
	mainurl1=cat_url[cat_url.find('http://')+7:]
	mainurl='http://'+mainurl1[0:mainurl1.find('/')]
  if 'https://' in cat_url:
	mainurl1=cat_url[cat_url.find('https://')+8:]
	mainurl='https://'+mainurl1[0:mainurl1.find('/')]
  ckanjason='{'
  ckanjason=ckanjason+"'url':"+"'"+str(url2)+"'"+","

  ## get data from given url
  try:
	r2  = requests.get(url2)
	data2 = r2.text
  except: data2=''

  soup2 = BeautifulSoup(data2)

  counterl=0
  
#--#--- for no label data
  ckanjason,xtras1=AddNoLabelToJson.AddToJson(commands,counterl,ckannotes,ckanlicense,ckanresource,ckantags,ckanauthor_email,ckanauthor,soup2,text_file,ckanjason,ckantitle,ckandate_updated
  ,ckanExtrasCategory,ckanExtrasFrequency,ckanExtrasLanguage,ckanMaintainer,ckandate_released,ckancountry,ckantemporalcoverage,ckanorganization,ckanmaintainer_email,
ckanstate,ckancity)

  counterl=0
  title=soup2.find_all('title')
  title2=str(title)

  if ckantitle=="":

	title2=title2[0:title2.find('|')]
	ckanjason=ckanjason+"'title':"+'"'+str(title2.replace('[<title>','').replace(']','').replace('|','').replace('</title>','').replace("'","").replace('"','').strip())+'"'+","
	ckanjason=ckanjason+"'name':"+"'"+str(hashlib.md5(title2.replace('[<title>','').replace(']','').replace('|','').replace('</title>','')).hexdigest())+"'"+","

  ckanjason=ckanjason+"'metadata_created':"+"'"+str(datetime.datetime.now())+"',"+"'catalogue_url':"+"'"+str(mainurl)+"',"+"'platform':'html',"
  
  soup3=soup2.findAll(text=True)
  j=0

  # xpath data
  try:
      xpath_json = AddXPathToJson.AddToJson(r2.content,rules)
  except:
      xpath_json=None
  if xpath_json is not None:
    ckanjason += xpath_json
  else:
      print('xpath returned nothing')
  #print(ckanjason)


#for label data
  ckanjason=AddLabelToJson.AddToJson(soup3,label,j,ckannotes,ckanlicense,ckanresource,ckantags,ckanauthor_email,ckanauthor,text_file,ckanjason,a_link,ckandate_updated,ckanExtrasCategory
  ,ckanExtrasFrequency,ckanExtrasLanguage,ckanMaintainer,ckandate_released,xtras1,ckancountry,ckantemporalcoverage,ckanorganization,ckanmaintainer_email,
ckanstate,ckancity)
  
  j=0
  k=0
  #--- links searching

  while j<len(links):
	  while k<len(soup3):
		  if links[j] in soup3[k]:

			  links[j]=soup3[k]
		  k=k+1
	  k=0
	  j=j+1
  j=0

# for links
  ckanjason=AddLinkToJson.AddToJson(links,soup3,ckannotes,ckanlicense,ckanresource,ckantags,ckanauthor_email,ckanauthor,text_file,ckanjason,j,type1,mainurl,btn_identifier)
  j=0

  print(('\n'+'\n'+'json: '+str(ckanjason.rstrip(','))+'}'))
  json2=jason.rstrip(',')+'}'
  ckanjson3=ckanjason.rstrip(',')+'}'
  ckanjsonWithoutTags=ckanjson3.replace(ckanjson3[ckanjson3.find("'tags':[{")-1:ckanjson3.find("}]")+2],'')
  ckanjson1='ckanjson2='+str(ckanjson3)
  ckanjsonWithoutTags1='ckanjsonWithoutTags2='+str(ckanjsonWithoutTags)

  #-- store metadata to database
  try:

	exec(ckanjson1)
	if 'author' in ckanjson2.keys()  or 'license_id' in ckanjson2.keys() or 'tags' in ckanjson2.keys() or 'author_email' in ckanjson2.keys() or 'organization' in ckanjson2.keys() or len(ckanjson2['extras'])>=3 or ('resources' in ckanjson2.keys() and len(ckanjson2['resources'])>0):
 #or len(ckanjson2['resources'])>0: #or len(ckanjson2['extras'])>=2
		try:
		  try:
			  #AddToCkan.AddtoCkan(ckanjson2)
			  #log.info('Metadata stored/updated succesfully to Ckan.')
			  temp_id=str(uuid.uuid3(uuid.NAMESPACE_OID, str(url2)))
			  ckanjson2.update({'id':str(temp_id)})
			  content=ckanjson2
			  ## check if dataset exists in mongodb
			  document=db1.find_one({"id":temp_id,"catalogue_url":mainurl})
			  if document==None:
				  db1.save(ckanjson2)
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
			  else:
				if len(ckanjson2.keys())>1:
				  met_created=document['metadata_created']
				  if 'copied' in document.keys():
					ckanjson2.update({'copied':document['copied']})
				  ckanjson2.update({'updated_dataset':True})
				  ckanjson2.update({'metadata_created':met_created})
				  ckanjson2.update({'metadata_updated':str(datetime.datetime.now())})
				  #existing_dataset=db1.find_one({"id":temp_id,"catalogue_url":mainurl})
				  objectid=document['_id']
				  ckanjson2.update({"_id":objectid})
				  db1.save(ckanjson2)
				  log.info('Metadata updated succesfully to MongoDb.')
				  fetch_document=db_fetch_temp.find_one()
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

		  except :
			pass
		  try:
			resources_num=len(ckanjson2['resources'])
		  except KeyError:
			resources_num=0
		  try:
			extras_num=len(ckanjson2['extras'])
		  except KeyError:
			extras_num=0
		  log.info('Total metadata elements stored: '+str(len(ckanjson2.keys())-1)+', metadata elements in extras: '+str(extras_num)+' ,number of resources: '+str(resources_num))

		except urllib2.HTTPError, e:
		  log.info("Ckan Json Validation Error.")
		  log.info("Error found in json: "+str(ckanjson2)+'.'+'Trying to store metadata without tags.')


		  try:
			# sometimes tags are a mess. try to store to mongo json without tags
			try:
				exec(ckanjsonWithoutTags1)
			except:
				ckanjsonWithoutTags2=""
			try:

			  #AddToCkan.AddtoCkan(ckanjsonWithoutTags2)
			  #log.info('Metadata without tags stored/updated succesfully to Ckan.')
			  temp_id=str(uuid.uuid3(uuid.NAMESPACE_OID, str(url2)))
			  ckanjsonWithoutTags2.update({'id':str(temp_id)})

			  ## check if dataset exists in mongodb
			  document=db1.find_one({"id":temp_id,"catalogue_url":mainurl})	
			  if document==None:		
				db1.save(ckanjsonWithoutTags2)
				log.info('Metadata without tags stored succesfully to MongoDb.')
			  try:
				resources_num=len(ckanjsonWithoutTags2['resources'])
			  except KeyError:
				resources_num=0
			  try:
				extras_num=len(ckanjsonWithoutTags2['extras'])
			  except KeyError:
				extras_num=0
			  log.info('Total metadata elements stored: '+str(len(ckanjsonWithoutTags2.keys())-1)+' ,metadata elements in extras: '+str(extras_num)+' ,number of resources: '+str(resources_num))

			except urllib2.HTTPError, e:
			  j+=1
			  #text_file.write("DB_FATAL_ERROR")
			  #log.info("Ckan Json Validation Error : "+str(ckanjsonWithoutTags2)+'.'+'Dataset with url:'+str(url2)+' did not stored')

		  except SyntaxError:
			  j+=1
			  #text_file.write("SYNTAX_ERROR1")
			  log.info("Syntax Error in json: "+str(ckanjsonWithoutTags2)+'.'+'Dataset with url:'+str(url2)+' did not stored')
  except SyntaxError:

	k+=1
	text_file.write("SYNTAX_ERROR")
	log.info("Syntax Error in json: "+str(ckanjson2)+'.'+'Dataset with url:'+str(url2)+' did not stored')
	ckanjson2={}
  try:
	del ckanjson2['_id']
  except: pass
  ckanjason="{"
  return json.dumps(ckanjson2)
#end of harvest_url function
