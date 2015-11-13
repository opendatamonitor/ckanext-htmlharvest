from bs4 import BeautifulSoup
import urllib2
import requests
import time
from xml.dom.minidom import parseString
import re
import difflib
import json
import logging
import SaveLabels
import harvester_final
import hashlib
xtras1=[]

def AddToJson(commands,counterl,ckannotes,ckanlicense,ckanresource,ckantags,ckanauthor_email,ckanauthor,soup2,text_file,ckanjason,ckantitle,ckandate_updated,ckanExtrasCategory,
ckanExtrasFrequency,ckanExtrasLanguage,ckanMaintainer,ckandate_released,ckancountry,ckantemporalcoverage,ckanorganization,ckanmaintainer_email,ckanstate,ckancity):

  while counterl<len(commands):

	command=commands[counterl]
	if command!="":

	  command1=command[:command.rfind('find_all')]+"getText().encode('utf-8').lstrip().rstrip()))"
	  command2="nolabel1="+command.replace('text_file.write(str(','').replace(".encode('utf-8').lstrip().rstrip()))",'')



	try:
		exec command2

	#---- json transformations
		if command==ckantitle:
			title2=str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))
			ckanjason=ckanjason+"'title"+"':"+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8').replace(']','').replace('|','').replace('</title>','').replace("'","").replace('"','').strip())+"'"+","
			ckanjason=ckanjason+"'name':"+"'"+str(hashlib.md5(title2.replace('[<title>','').replace(']','').replace('|','').replace('</title>','')).hexdigest())+"'"+","
		if command==ckannotes:
			ckanjason=ckanjason+"'notes"+"':"+"'"+str(nolabel1.replace('\n','').strip().strip('\n').replace("'",'').replace('"','').replace("  ",'').encode('utf-8'))+"'"+","
		if command==ckanlicense:
			ckanjason=ckanjason+"'license_id"+"':"+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"'"+","
		if command==ckanresource:
			ckanjason=ckanjason+"'resources"+"':"+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"'"+","
		if command==ckantags:
			ckanjason=ckanjason+"'tags"+"':"+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"'"+","
		if command==ckanauthor_email:
			ckanjason=ckanjason+"'author_email"+"':"+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"'"+","
		if command==ckanauthor:
			ckanjason=ckanjason+"'author"+"':"+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"'"+","
		if command==ckanMaintainer:
			ckanjason=ckanjason+"'maintainer"+"':"+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"'"+","
		if command==ckandate_updated:
			update="{"+"'key':"+'"date_updated",'+'"value":'+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"'}"
			xtras1.append(update)
		if command==ckanExtrasCategory:
			category="{"+"'key':"+'"category",'+'"value":'+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"'}"
			xtras1.append(category)
		if command==ckanExtrasFrequency:
			frequency="{"+"'key':"+'"frequency",'+'"value":'+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"'}"
			xtras1.append(frequency)
		if command==ckanExtrasLanguage:
			language="{"+"'key':"+'"language",'+'"value":'+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"'}"
			xtras1.append(language)
		if command==ckandate_released:
			date="{"+"'key':"+'"date_released",'+'"value":'+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"'}"
			xtras1.append(date)
		if command==ckancountry:
			geographiccoverage="{"+"'key':"+'"country",'+'"value":'+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"'}"
			xtras1.append(geographiccoverage)
		if command==ckantemporalcoverage:
			temporalcoverage="{"+"'key':"+'"temporal_coverage",'+'"value":'+"'"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"'}"
			xtras1.append(temporalcoverage)
		if command==ckanorganization:
			ckanjason=ckanjason+"'organization':{'title': '"+str(nolabel1.replace('\n','').replace("'",'').encode('utf-8'))+"','type':'organization'},"
	except (IndexError,AttributeError):

		command1=command[:command.rfind('find_all')]+"getText().encode('utf-8').lstrip().rstrip()))"
		try:
			exec command1
		except (IndexError,AttributeError):
			nothing=""
			


	counterl=counterl+1

  return (ckanjason, xtras1)
