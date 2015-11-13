from bs4 import BeautifulSoup
import urllib2
import requests
import time
from xml.dom.minidom import parseString
import re
import difflib
import json
import logging
import pymongo
import bson
import configparser
import LoadPossibleLabels

##read from development.ini file all the required parameters
config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
mongoclient=config['ckan:odm_extensions']['mongoclient']
mongoport=config['ckan:odm_extensions']['mongoport']

i=0
j=0
k=0
found=False

#MongoClient
client = pymongo.MongoClient(str(mongoclient), int(mongoport))

def AutoFindingElements(url):

  try:

		#Database Connection
		db = client.odm
		try:
		  collection=db.possible_labels
		  possiblelabels=collection.find_one()
		except:
		  possiblelabels=None
		
		if possiblelabels==None or len(possiblelabels.keys())<1:
		  possiblelabels=LoadPossibleLabels.possible_labels
		  collection=db.possible_labels
		  collection.save(possiblelabels)


		labelLicense=possiblelabels['license']
		labelTags=possiblelabels['tags']
		labelcategory=possiblelabels['category']
		labelauthor_email=possiblelabels['author_email']
		labeldate_released=possiblelabels['date_released']
		labelfrequency=possiblelabels['frequency']
		labellanguage=possiblelabels['language']
		labelmaintainer=possiblelabels['maintainer']
		labelnotes=possiblelabels['notes']
		labelauthor=possiblelabels['author']
		labelresource=possiblelabels['resource']
		labeldate_updated=possiblelabels['date_updated']
		labelcountry=possiblelabels['country']
		labeltemporalcoverage=possiblelabels['temporal_coverage']
		labelorganization=possiblelabels['organization']
		labelmaintainer_email=possiblelabels['maintainer_email']
		labelstate=possiblelabels['state']
		labelcity=possiblelabels['city']		

		

		r  = requests.get(url)
		data2 = r.text
		soup2 = BeautifulSoup(data2)

		#-- make a list of all data in page
		soup3=soup2.findAll(text=True)

		labelLicense1=AutoLabelFinder(soup3,labelLicense,j,k,found)
		labelTags1=AutoLabelFinder(soup3,labelTags,j,k,found)
		labelcategory1=AutoLabelFinder(soup3,labelcategory,j,k,found)
		labelauthor_email1=AutoLabelFinder(soup3,labelauthor_email,j,k,found)
		labeldate_released1=AutoLabelFinder(soup3,labeldate_released,j,k,found)
		labelfrequency1=AutoLabelFinder(soup3,labelfrequency,j,k,found)
		labellanguage1=AutoLabelFinder(soup3,labellanguage,j,k,found)
		labelmaintainer1=AutoLabelFinder(soup3,labelmaintainer,j,k,found)
		labelnotes1=AutoLabelFinder(soup3,labelnotes,j,k,found)
		labelauthor1=AutoLabelFinder(soup3,labelauthor,j,k,found)
		labelresource1=AutoLabelFinder(soup3,labelresource,j,k,found)
		labeldate_updated1=AutoLabelFinder(soup3,labeldate_updated,j,k,found)
		labelcountry1=AutoLabelFinder(soup3,labelcountry,j,k,found)
		labeltemporalcoverage1=AutoLabelFinder(soup3,labeltemporalcoverage,j,k,found)
		labelorganization1=AutoLabelFinder(soup3,labelorganization,j,k,found)
		labelmaintainer_email1=AutoLabelFinder(soup3,labelmaintainer_email,j,k,found)
		labelstate1=AutoLabelFinder(soup3,labelstate,j,k,found)
		labelcity1=AutoLabelFinder(soup3,labelcity,j,k,found)



  except urllib2.HTTPError, e:
		print('http error')

  json={"license":str(labelLicense1[0].encode('utf-8')),"tags":str(labelTags1[0].encode('utf-8')),"category":str(labelcategory1[0].encode('utf-8')),"author_email":str(labelauthor_email1[0].encode('utf-8'))
		,"date_released":str(labeldate_released1[0].encode('utf-8')),"country":str(labelcountry1[0].encode('utf-8'))
		,"temporal_coverage":str(labeltemporalcoverage1[0].encode('utf-8')),"frequency":str(labelfrequency1[0].encode('utf-8'))
		,"language":str(labellanguage1[0].encode('utf-8')),"maintainer":str(labelmaintainer1[0].encode('utf-8')),"notes":str(labelnotes1[0].encode('utf-8')),"author":str(labelauthor1[0].encode('utf-8'))
		,"resource":str(labelresource1[0].encode('utf-8')),"date_updated":str(labeldate_updated1[0].encode('utf-8')),"organization":str(labelorganization1[0].encode('utf-8')),"maintainer_email":str(labelmaintainer_email1[0].encode('utf-8')),"state":str(labelstate1[0].encode('utf-8')),"city":str(labelcity1[0].encode('utf-8'))}
  
  return json


def AutoLabelFinder(soup3,label,j,k,found):

	while j<len(label):
			while k<len(soup3):

				if label[j] in soup3[k]:

					label[j]=soup3[k]
					found=True
				k=k+1
			k=0
			if found==False:
				del label[j]

			if found==True:
			  j=j+1
			  found=False
	j=0
 	l=0
	while l<len(label):
		label[l]=label[l].rstrip()
		l+=1
	if len(label)==0:
	  label.append("")
	return label


