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


def AddToJson(links,soup3,ckannotes,ckanlicense,ckanresource,ckantags,ckanauthor_email,ckanauthor,text_file,ckanjason,j,type1,mainurl,btn_identifier):

  while j<len(links):
	format_temp=[]
	format_temp[:]=[]
	formats=[]
	formats[:]=[]
	
	if links[j] in soup3:

	  for x in range(0,len(soup3)):

		if soup3[x]==links[j]:
			print("===>"+str(soup3[x]))
			parent=str(soup3[x].parent.parent)

			##data.belgium.be module
			if mainurl=='http://data.belgium.be':
				parent=str(soup3[x].parent.parent.parent)
				if 'Links FR:' not in parent:
					parent=str(soup3[x].parent.parent.parent.parent)
					if 'Links FR:' not in parent:
						parent=str(soup3[x].parent.parent.parent.parent.parent)

			if mainurl=='http://transparencia.gijon.es':
				parent=str(soup3[x].parent.parent.parent.parent.parent)
			if mainurl=='http://www.data.rennes-metropole.fr':			
				parent=str(soup3[x].parent.parent.parent)
			if mainurl=='http://dadesobertes.gencat.cat':
				parent=str(soup3[x].parent.parent.parent.parent)
			if mainurl=='http://data.southampton.ac.uk':
				parent=str(soup3[x].parent.parent.parent)
			if mainurl=="http://www3.hants.gov.uk":
				parent=str(soup3[x].parent.parent.parent.parent.parent)
			if mainurl=="http://data.gent.be":
				parent=str(soup3[x].parent.parent.parent)
			if mainurl=="http://www.opengov.se":
				parent=str(soup3[x].parent.parent.parent)
			if mainurl=="http://www.dipusevilla.es":
				parent=str(soup3[x].parent.parent.parent)
			if mainurl=="http://dati.comune.bologna.it":
				parent=str(soup3[x].parent.parent.parent.parent)
			if mainurl=="http://www.datalocale.fr":
				parent=str(soup3[x].parent.parent.parent.parent)
			if mainurl=="http://www.chj.es":
				parent=str(soup3[x].parent.parent.parent)
			if mainurl=="https://open.nrw":
				parent=str(soup3[x].parent.parent.parent)
			if mainurl=="http://opendata.bcn.cat":
				parent=str(soup3[x].parent.parent.parent)
			if mainurl=="http://opendata.cloudbcn.cat":
				parent=str(soup3[x].parent.parent.parent)
			if mainurl=="http://opendata.tarragona.cat":
				parent=str(soup3[x].parent.parent.parent)
			if mainurl=="http://transparencia.castillalamancha.es":
				parent=str(soup3[x].parent.parent.parent.parent)
			parent1=BeautifulSoup(parent)
			
			parent2=parent1.find_all(text=True)
			parent3=parent1.find_all('a', {'href': True})
			parent3b=parent1.find_all('a', {'onclick': True})
			##datos.gob.es module
			if mainurl=='http://datos.gob.es':
				k=0
				while k<len(parent3):
					parent4=parent3[k].parent
					l=0
					format_temp=parent4.find_all(text=True)
					while l<len(format_temp):
						if '\n' == format_temp[l]:
							del(format_temp[l])
							l-=1
						l+=1
					if len(format_temp)>0:
						
						format1=str(format_temp[0].encode('utf-8')).strip('\n').strip('\t').strip().lower()
						format1=format1.replace('\t','').replace('\n','')	
						if '(' in format1:
							format1=format1[0:format1.find('(')]
						
						formats.append(format1)
						
					k+=1
			
			counter=0
			#text_file.write(soup3[x].string.encode('utf-8'))

			

			string_matching=difflib.SequenceMatcher(None, ckannotes.encode('utf-8'),soup3[x].string.encode('utf-8').strip()).ratio()

			if string_matching>=0.9:
				ckanjason=ckanjason+"'notes"+"':'"

			string_matching=difflib.SequenceMatcher(None, ckanlicense.encode('utf-8'),soup3[x].string.encode('utf-8').strip()).ratio()

			if string_matching>=0.9:
				ckanjason=ckanjason+",'license_id"+"':'"


			string_matching=difflib.SequenceMatcher(None, ckantags.encode('utf-8'),soup3[x].string.encode('utf-8').strip()).ratio()

			if string_matching>=0.9:
				ckanjason=ckanjason+"'tags"+"':["
			string_matching=difflib.SequenceMatcher(None, ckanauthor_email.encode('utf-8'),soup3[x].string.encode('utf-8').strip()).ratio()

			if string_matching>=0.9:
				ckanjason=ckanjason+"'author_email"+"':'"
			string_matching=difflib.SequenceMatcher(None, ckanauthor.encode('utf-8'),soup3[x].string.encode('utf-8').strip()).ratio()

			if string_matching>=0.8:
				ckanjason=ckanjason+"'author"+"':'"
			string_matching=difflib.SequenceMatcher(None, ckanresource.encode('utf-8'),soup3[x].string.encode('utf-8').strip()).ratio()

			if string_matching>=0.9:
				ckanjason=ckanjason+"'resources"+"':["
			

			while counter<len(parent3):
			
				url_temp=str(parent3[counter]['href'].encode('utf-8').lstrip().lower())

				if mainurl!='http://datos.gob.es':

					if counter<len(parent3)-1:
						if string_matching>=0.9:
							if 'jsp' in url_temp or 'pdf' in url_temp or 'csv' in url_temp or 'rdf' in url_temp or 'xls' in url_temp or '.htm' in url_temp or 'kml' in url_temp or 'xml' in url_temp or 'json' in url_temp or 'zip' in url_temp or 'doc' in url_temp or 'txt' in url_temp  or 'excel' in url_temp or 'aspx' in url_temp or 'gz' in url_temp or 'rss' in url_temp or 'https://ias1.larioja.org/opendata/download?recurso=' in url_temp or '/download/file/fid/' in url_temp or '/drupal7/file/' in url_temp or '/resourceunit/' in url_temp:
								type1,filesize=CheckTypeAndFilesize(url_temp)
								url_temp1=str(parent3[counter]['href'].encode('utf-8').lstrip())
								if 'http://' not in url_temp1 and 'https://' not in url_temp1:
									if url_temp1[0]=='/':
										url_temp1=mainurl+url_temp1
									else:
										url_temp1=mainurl+'/'+url_temp1
								#text_file.write('type1: '+type1)
								ckanjason=ckanjason+"{'name': '"+'resource'+"',"+'"url":'+"'"+url_temp1+"'"+',"format":'+"'"+type1.lower()+"'"+',"size":'+"'"+str(filesize)+"'"+"},"
							else:
							  #try:
								if btn_identifier!="" and btn_identifier!=None:
								  if counter<len(parent3b):
									try:
									  url_temp=str(parent3b[counter]['onclick'].encode('utf-8').lstrip())
									  url_temp=url_temp[url_temp.find('http'):url_temp.find("',")]
									  #if 'jsp' in url_temp or 'pdf' in url_temp or 'csv' in url_temp or 'rdf' in url_temp or 'xls' in url_temp or '.htm' in url_temp or 'kml' in url_temp or 'xml' in url_temp or 'json' in url_temp or 'zip' in url_temp or 'doc' in url_temp or 'txt' in url_temp  or 'excel' in url_temp or 'aspx' in url_temp or 'gz' in url_temp or 'rss' in url_temp or 'https://ias1.larioja.org/opendata/download?recurso=' in url_temp or '/download/file/fid/' in url_temp or '/drupal7/file/' in url_temp or '/resourceunit/' in url_temp:
									  type1,filesize=CheckTypeAndFilesize(url_temp)
									  #url_temp1=str(parent3b[counter]['onclick'].encode('utf-8').lstrip())
									  ckanjason=ckanjason+"{'name': '"+'resource'+"',"+'"url":'+"'"+url_temp+"'"+',"format":'+"'"+type1.lower()+"'"+',"size":'+"'"+str(filesize)+"'"+"},"
									except:pass
							  #except:pass
						else:
							ckanjason=ckanjason+str(parent3[counter]['href'].encode('utf-8').lstrip())+"',"

					else:
						if string_matching>=0.9:
							if 'jsp' in url_temp or 'pdf' in url_temp or 'csv' in url_temp or 'rdf' in url_temp or 'xls' in url_temp or '.htm' in url_temp or 'kml' in url_temp or 'xml' in url_temp or 'json' in url_temp or 'zip' in url_temp  or 'txt' in url_temp or 'doc' in url_temp or 'excel' in url_temp or 'aspx' in url_temp or 'gz' in url_temp or 'rss' in url_temp or 'https://ias1.larioja.org/opendata/download?recurso=' in url_temp or '/download/file/fid/' in url_temp or '/drupal7/file/' in url_temp or '/resourceunit/' in url_temp:
								type1,filesize=CheckTypeAndFilesize(url_temp)
								url_temp1=str(parent3[counter]['href'].encode('utf-8').lstrip().lower())
								if 'http://' not in url_temp1 and 'https://' not in url_temp1:
									if url_temp1[0]=='/':
										url_temp1=mainurl+url_temp1
									else:
										url_temp1=mainurl+'/'+url_temp1
								ckanjason=ckanjason+"{"+"'name': '"+'resource'+"',"+'"url":'+"'"+url_temp1+"'"+',"format":'+"'"+type1.lower()+"'"+',"size":'+"'"+str(filesize)+"'"+"}]"
							else:
								if btn_identifier!="" and btn_identifier!=None:
								  if counter<len(parent3b):
									  try:
										url_temp=str(parent3b[counter]['onclick'].encode('utf-8').lstrip())
										url_temp=url_temp[url_temp.find('http'):url_temp.find("',")] 
										type1,filesize=CheckTypeAndFilesize(url_temp)
										ckanjason=ckanjason+"{"+"'name': '"+'resource'+"',"+'"url":'+"'"+url_temp+"'"+',"format":'+"'"+type1.lower()+"'"+',"size":'+"'"+str(filesize)+"'"+"}"
									  except:pass
								ckanjason=ckanjason+']'
						else:
							try:
								ckanjason=ckanjason+str(parent3[counter].parent.string.encode('utf-8'))+"',"
							except :
								ckanjason=ckanjason+str(parent3[counter].parent.string)+"',"


				## datos.gob.es module

				if mainurl=='http://datos.gob.es':
					if counter<len(parent3)-1:
						if string_matching>=0.9:
							if 'jsp' in str(formats[counter]) or 'pdf' in str(formats[counter]) or 'csv' in str(formats[counter]) or 'rdf' in str(formats[counter]) or 'xls' in str(formats[counter]) or 'htm' in str(formats[counter]) or 'kml' in str(formats[counter]) or 'xml' in str(formats[counter]) or 'json' in str(formats[counter]) or 'zip' in str(formats[counter]) or 'doc' in str(formats[counter]) or 'txt' in str(formats[counter]) or 'doc' in str(formats[counter]) or 'sparql' in str(formats[counter]) or 'plain' in str(formats[counter]) or 'rss' in str(formats[counter]) or 'shp' in str(formats[counter]) or 'pc-axis' in str(formats[counter]) or 'ods' in str(formats[counter]) or 'solr' in str(formats[counter]) or 'atom' in str(formats[counter]) or 'kmz' in str(formats[counter]) or 'wfs' in str(formats[counter]) or 'plain' in str(formats[counter]) or 'wms' in str(formats[counter]) or 'calendar' in str(formats[counter]) or 'jpef' in str(formats[counter]) or 'mdb' in str(formats[counter]) or 'tiff' in str(formats[counter]) or 'dgn' in str(formats[counter]) or 'ecw' in str(formats[counter]) or 'dwg' in str(formats[counter]) or 'ascii' in str(formats[counter]) or 'soap' in str(formats[counter]) or 'gbd' in str(formats[counter]) or 'dxf' in str(formats[counter]) or 'gml' in str(formats[counter]) or 'xbrl' in str(formats[counter]) or 'dbf' in str(formats[counter]) or 'img' in str(formats[counter]) or 'djvu' in str(formats[counter]) or 'raster' in str(formats[counter]) or 'wcs' in str(formats[counter]) or 'png' in str(formats[counter]):

								try:
									f=urllib2.urlopen(url_temp,timeout=1)
									filesize=f.headers["Content-Length"]
								except :
									filesize=""
								ckanjason=ckanjason+"{'name': '"+'resource'+"',"+'"url":'+"'"+str(parent3[counter]['href'].encode('utf-8').lstrip())+"'"+',"format":'+"'"+str(formats[counter])+"'"+',"size":'+"'"+str(filesize)+"'"+"},"
						else:
							ckanjason=ckanjason+str(parent3[counter]['href'].encode('utf-8').lstrip())+"',"

					else:
						if string_matching>=0.9:
							if 'jsp' in str(formats[counter]) or 'pdf' in str(formats[counter]) or 'csv' in str(formats[counter]) or 'rdf' in str(formats[counter]) or 'xls' in str(formats[counter]) or 'htm' in str(formats[counter]) or 'kml' in str(formats[counter]) or 'xml' in str(formats[counter]) or 'json' in str(formats[counter]) or 'zip' in str(formats[counter]) or 'doc' in str(formats[counter]) or 'txt' in str(formats[counter]) or 'doc' in str(formats[counter]) or 'sparql' in str(formats[counter]) or 'plain' in str(formats[counter]) or 'rss' in str(formats[counter]) or 'shp' in str(formats[counter]) or 'pc-axis' in str(formats[counter]) or 'ods' in str(formats[counter]) or 'solr' in str(formats[counter]) or 'atom' in str(formats[counter]) or 'kmz' in str(formats[counter]) or 'wfs' in str(formats[counter]) or 'plain' in str(formats[counter]) or 'wms' in str(formats[counter]) or 'calendar' in str(formats[counter]) or 'jpef' in str(formats[counter]) or 'mdb' in str(formats[counter]) or 'tiff' in str(formats[counter]) or 'dgn' in str(formats[counter]) or 'ecw' in str(formats[counter]) or 'dwg' in str(formats[counter]) or 'ascii' in str(formats[counter]) or 'soap' in str(formats[counter]) or 'gbd' in str(formats[counter]) or 'dxf' in str(formats[counter]) or 'gml' in str(formats[counter]) or 'xbrl' in str(formats[counter]) or 'dbf' in str(formats[counter]) or 'img' in str(formats[counter]) or 'djvu' in str(formats[counter]) or 'raster' in str(formats[counter]) or 'wcs' in str(formats[counter]) or 'png' in str(formats[counter]):
								try:
									f=urllib2.urlopen(url_temp,timeout=1)
									filesize=f.headers["Content-Length"]
								except :
									filesize=""
								ckanjason=ckanjason+"{"+"'name': '"+'resource'+"',"+'"url":'+"'"+str(parent3[counter]['href'].encode('utf-8').lstrip())+"'"+',"format":'+"'"+str(formats[counter])+"'"+',"size":'+"'"+str(filesize)+"'"+"}]"
							
						else:
							ckanjason=ckanjason+str(parent3[counter].parent.string.encode('utf-8'))+"',"


				counter=counter+1
			break

	j=j+1
  ckanjason=ckanjason+"]"
  if mainurl=="http://opendata.euskadi.eus":
	ckanjason=ckanjason.replace(",Lege-informazioa'","")
  return ckanjason.replace("},]","}]").replace("]]","]").replace(",]","")


def CheckTypeAndFilesize(url_temp):
  #check if link contains a known type of file
		filesize=""

		print("url temp:   "+str(url_temp))

		if 'jsp' in url_temp:
			type1='jsp'
		if 'pdf' in url_temp:
			type1='pdf'
		if 'rdf' in url_temp:
			type1='rdf'
		if 'xls' in url_temp:
			type1='xls'
		if '.htm' in url_temp:
			type1='html'
		if 'csv' in str(url_temp):
			type1='csv'
		if 'kml' in url_temp:
			type1='kml'
		if 'xml' in url_temp:
			type1='xml'
		if 'json' in url_temp:
			type1='json'
		if 'api' in str(url_temp):
			type1='api'
		if 'gzip' in url_temp:
			type1='gzip'
		if 'viz' in url_temp:
			type1='viz'
		if 'tsv' in url_temp:
			type1='tsv'
		if 'zip' in str(url_temp):
			type1='zip'
		if 'shp' in url_temp:
			type1='shp'
		if 'ods' in url_temp:
			type1='ods'
		if 'wms' in url_temp:
			type1='wms'
		if 'kmz' in str(url_temp):
			type1='kmz'
		if '.doc' in url_temp:
			type1='doc'
		if 'shape' in url_temp:
			type1='shape'
		if 'txt' in url_temp:
			type1='txt'
		if '7z' in str(url_temp):
			type1='7z'
		if 'wfs' in url_temp:
			type1='wfs'
		if 'turtle' in url_temp:
			type1='turtle'
		if 'gml' in url_temp:
			type1='gml'
		if 'geojson' in str(url_temp):
			type1='geojson'
		if 'odt' in url_temp:
			type1='odt'
		if 'aspx' in url_temp:
			type1='aspx'
		if 'ppt' in url_temp:
			type1='ppt'
		if 'rtf' in url_temp:
			type1='rtf'
		if 'excel' in url_temp:
			type1='xls'
		if 'aspx' in url_temp:
			type1='aspx'
		if 'gz' in url_temp:
			type1='zip'
		if 'rss' in url_temp:
			type1='rss'


		if '.jsp' not in url_temp and '.pdf' not in url_temp and '.rdf' not in url_temp and '.xls' not in url_temp and '.htm' not in url_temp and '.csv' not in url_temp and '.kml' not in url_temp and '.xml' not in url_temp and '.json' not in url_temp and '.api' not in url_temp and '.gzip' not in url_temp and '.viz' not in url_temp and '.tsv' not in url_temp and '.zip' not in url_temp and '.shp' not in url_temp and '.ods' not in url_temp and '.wms' not in url_temp and '.kmz' not in url_temp and '.doc' not in url_temp and '.shape' not in url_temp and '.txt' not in url_temp and '.7z' not in url_temp and '.wfs' not in url_temp and '.turtle' not in url_temp and '.gml' not in url_temp and '.geojson' not in url_temp and '.odt' not in url_temp and '.aspx' not in url_temp and '.ppt' not in url_temp and '.rtf' not in url_temp and '.excel' not in url_temp and '.aspx' not in url_temp and '.gz' not in url_temp:
			type1=""

		## get filesize from url

		if type1!="htm" and type1!="html" and type1!="":

		  try:
			f=urllib2.urlopen(url_temp,timeout=1)
			filesize=f.headers["Content-Length"]
		  except :
			filesize=""

		return (type1,filesize)