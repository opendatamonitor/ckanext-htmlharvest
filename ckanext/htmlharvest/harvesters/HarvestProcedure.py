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
backup_file_path=str(log_path)+'/ckanext-htmlharvest/ckanext/htmlharvest/harvesters/backup.txt'
mongoclient=config['ckan:odm_extensions']['mongoclient']
mongoport=config['ckan:odm_extensions']['mongoport']


def ProcedureWithNext(soup1,dataset_keyword,dataset_keyword1,mainurl,text_file,ckanjason,commands,ckannotes,ckanlicense,ckanresource,ckantags,ckanauthor_email,ckanauthor,j,label,k,a_link,
			  links,type1,jason,db1,endpoint,url,i,afterid,step,ckantitle,ckandate_updated,ckanExtrasCategory,ckanExtrasFrequency,ckanExtrasLanguage,ckanMaintainer,ckandate_released
			  ,ckancountry,ckantemporalcoverage,ckanorganization,ckanmaintainer_email,ckanstate,ckancity,ids):

  soup1=endpoint
  xtras=[]
  xtras1=[]
  mails=[]
  LinkCounter=0
  LastLinkCounter=0
## endpoint check
  #soupmatching=""
  harvested_pages=[]
  harvested_urls=[]
  harvested_pages[:]=[]
  harvested_urls[:]=[]
  cat_urls=[]
  counter=0
  text_file_mails = open('/var/local/ckan/default/pyenv/src/ckanext-htmlharvest/ckanext/htmlharvest/harvesters/emails.txt', "a")
  text_file_maintainer_mails=open('/var/local/ckan/default/pyenv/src/ckanext-htmlharvest/ckanext/htmlharvest/harvesters/maintainer_emails.txt', "a")
  print(mainurl)
  log.info('Started')
  if step=="":
	step=0
  print("***")
  print(url)
  if ',' in url:
	 url=url.replace('\n','').replace('\r','').rstrip(',')
	 cat_urls=url.split(',')
  else: cat_urls.append(url)
  print(cat_urls)
  
  count=0
  while count<len(cat_urls):
	while endpoint in soup1:

		try:
			print("_----->>>>"+'/n')
			#log.info("Harvesting of Catalogue: "+str(mainurl)+" started.")
			url1=str(cat_urls[count])+str(i)+str(afterid)
			log.info("Searching url: "+str(url1)+" for datasets.")
			print(url1)
			r  = requests.get(url1)

			data = r.text
			soup = BeautifulSoup(data)
			soup1=str(soup)


			h=0
			found=False
			while h<len(harvested_pages):
			  string_matching=difflib.SequenceMatcher(None,soup1,str(harvested_pages[h])).ratio()
			  #print("--------------------------->"+str(string_matching)+'\n')
			  print("string_matching: "+str(string_matching))
			  if string_matching>=0.945 and i>1:
				  print('Harvester gather procedure finished..')
				  #log.info("Harvesting of Catalogue: "+str(mainurl)+" finished.")
				  #log.info(" "+str(counter)+" datasets harvested and stored to Ckan and MongoDb")
				  text_file1 = open(str(backup_file_path),"w")
				  text_file1.write("")
				  text_file1.close()
				  found=True

			  h+=1
			if found==True:break
		  # soupmatching=soup1
			harvested_pages.append(soup1)

			if len(harvested_pages)>2:
			  harvested_pages.pop(1)

			for link in soup.find_all('a'):


  #--list of non label data
				try:
					ahref=str(link.get('href').encode('utf-8'))

				except :

				  #text_file.write(str(datetime.datetime.now())+'  UnicodeError in datasets url: '+str(ahref))

				  ahref=str(link.get('href'))

				Pointer=0

				if ("www" in ahref) or ("http" in ahref):
				  if (dataset_keyword in ahref )or((dataset_keyword1 in ahref )):
					  Pointer=1
				else:
				  if (ahref.startswith(dataset_keyword) )or(ahref.startswith(dataset_keyword1)):
					  Pointer=1
				if Pointer==1:
					LastLinkCounter=1



					if 'http' not in str(ahref):
					  if mainurl[-1]=='/' or dataset_keyword[0]=='/':
						  url2=mainurl+ahref
					  else:
						  url2=mainurl+'/'+ahref
					else: url2=ahref

				  ## module for http://opendata.service-bw.de/ because of hidden url

					if 'http://opendata.service-bw.de/' in cat_urls[count]:
					  url2=mainurl+'/Seiten/'+ahref
					if 'https://open.nrw' in cat_urls[count]:
					  url2=mainurl+'open.nrw'+ahref

					if url2 not in harvested_urls:

						log.info('Dataset with url: '+str(url2)+' found.')

						print(str(url2))
						harvested_urls.append(url2)
						
		except urllib2.HTTPError, e:

		  # text_file.write("404 ERROR")
			log.info('404 Error in page with url: '+str(url1))
			#jason="{"
			print('i: ='+str(+i))
		if LastLinkCounter==1:
		  i=i+step
		  LastLinkCounter=0

		  LinkCounter=0
		else:
		  if LinkCounter<=3:
			LastLinkCounter=0
			i=i+step
			LinkCounter+=1
		  else:
			text_file1 = open(str(backup_file_path), "w")
			text_file1.write("")
			text_file1.close()
			break
	  
	count+=1
	i=0

  #harvested_pages[:]=[]
  #harvested_urls[:]=[]
  return harvested_urls
