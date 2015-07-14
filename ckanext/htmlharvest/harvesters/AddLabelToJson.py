# -*- coding: utf-8 -*-
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
import AddLinkToJson

num=0.8
xtras1=[]

def AddToJson(soup3,label,j,ckannotes,ckanlicense,ckanresource,ckantags,ckanauthor_email,ckanauthor,text_file,ckanjason,a_link,ckandate_updated,ckanExtrasCategory
,ckanExtrasFrequency,ckanExtrasLanguage,ckanMaintainer,ckandate_released,xtras1,ckancountry,ckantemporalcoverage,ckanorganization,ckanmaintainer_email,ckanstate,ckancity):


  count=0
  if ckantemporalcoverage!="":
	AddTemporalCoverage=False
  else:AddTemporalCoverage=True
  if ckandate_updated!="":
	Adddate_updated=False
  else:Adddate_updated=True
  if ckanExtrasCategory!="":
	AddCategory=False
  else:AddCategory=True
  if ckanExtrasLanguage!="":
	AddLanguage=False
  else:AddLanguage=True
  if ckanExtrasFrequency!="":
	AddFrequency=False
  else:AddFrequency=True
  if ckandate_released!="":
	Adddate_released=False
  else:Adddate_released=True
  if ckancountry!="":
	Addcountry=False
  else:Addcountry=True
  if ckanstate!="":
	Addstate=False
  else:Addstate=True
  if ckancity!="":
	Addcity=False
  else:Addcity=True

  extra=""
  string_matching2=0
  string_matching3=0
  string_matching4=0
  string_matching5=0
  string_matching6=0
  string_matching7=0
  string_matching8=0
  string_matching9=0
  string_matching10=0
  string_matching11=0
  xtras=[]
  
  extra="{"+"'key':"+'"language",'+'"value":'+"'"+str(ckanExtrasLanguage)+"'},"
  if extra!="":
	  xtras.append(extra)

  while count<len(xtras1):
	xtras.append(xtras1[count])
	count+=1
  xtras1[:]=[]

  while j<len(label):
		
	for x in range(0,len(soup3)):
		if label[j] in soup3[x]:
		#itan:
		#if label[j] in soup3[x]:


	#if any(label[j] in s for s in soup3):
				print(label[j])
	#	for x in range(0,len(soup3)):

			#if(soup3[x]==label[j]):
				###print("EDW TESTAROUME: ->"+'\n')
				###text_value=soup3[x].next_sibling.find_all(text=True)
				###print(text_value)
				###print('\n')
				parent=str(soup3[x].parent.parent)
				#edw tha dokimasw to next sibling na dw ti tha apodosei.
				parent1=BeautifulSoup(parent.decode('utf-8','ignore'))
				parent2=parent1.find_all(text=True)
				i1=0
				
				while i1<len(parent2):

					temp=parent2[i1]
					if temp==" " or temp=="\n":
						del parent2[i1]
					i1+=1
				

				if len(parent2)==1:

					parent=str(soup3[x].parent.parent.parent)
					parent1=BeautifulSoup(parent.decode('utf-8','ignore'))
					parent2=parent1.find_all(text=True)
					i1=0

					while i1<len(parent2):

						temp=parent2[i1]
						if temp==" " or temp=="\n":

							del parent2[i1]
						i1+=1
				
				##worst case senario handling( all metadata in a html list)
				i2=0
				labelId=0
				while i2<len(parent2):
					temp=parent2[i2]
					string_matching=difflib.SequenceMatcher(None, label[j],temp).ratio()
					if string_matching>=num:
						labelId=i2
					i2+=1
				#text_file.write("\n"+"\n"+">0>>"+str(parent2)+"\n"+str(labelId)+"   "+str(label[j].encode('utf-8')))

				if labelId>0:
					i3=0
					while i3<labelId:
					  del parent2[0]
					  i3+=1


				i4=1
				i5=0
				labelId1=0
				while i4<len(parent2):
					temp=parent2[i4]

					while i5<len(label):
						string_matching=difflib.SequenceMatcher(None, label[i5],temp).ratio()

						if string_matching>=num:
					
						  labelId1=i4

						
						  while len(parent2)>labelId1:
					 
							del parent2[labelId1]

						
						i5+=1
					i5=0
					i4+=1
				
				print(str(parent2))
				#---- check if label.next was found
				if len(parent2)>1:

					counter=0
					while counter<len(parent2):

						c=0
						link1=""
						finda=parent2[counter].parent.parent

						for a in finda.find_all('a', href=True):
							a=str(a['href'].encode('utf-8'))

							if a not in a_link:
								a_link.append(a)

						while c<len(a_link):
							link1=link1+' '+str(a_link[c])+' '

							c+=1

						if counter==0:

						#----find elements label,harmonise and add it to json:

							ckanjasonLabel=AddJsonLabelToElement(ckannotes,"notes",counter,ckanjason,parent2)
							ckanjason=ckanjason+ckanjasonLabel
							ckanjasonLabel=AddJsonLabelToElement(ckanlicense,"license_id",counter,ckanjason,parent2)
							ckanjason=ckanjason+ckanjasonLabel
							ckanjasonLabel=AddJsonLabelToElement(ckanresource,"resources",counter,ckanjason,parent2)
							ckanjason=ckanjason+ckanjasonLabel
							ckanjasonLabel=AddJsonLabelToElement(ckantags,"tags",counter,ckanjason,parent2)
							ckanjason=ckanjason+ckanjasonLabel
							ckanjasonLabel=AddJsonLabelToElement(ckanauthor_email,"author_email",counter,ckanjason,parent2)
							ckanjason=ckanjason+ckanjasonLabel
							ckanjasonLabel=AddJsonLabelToElement(ckanauthor,"author",counter,ckanjason,parent2)
							ckanjason=ckanjason+ckanjasonLabel
							ckanjasonLabel=AddJsonLabelToElement(ckanMaintainer,"maintainer",counter,ckanjason,parent2)
							ckanjason=ckanjason+ckanjasonLabel
							ckanjasonLabel=AddJsonLabelToElement(ckanmaintainer_email,"maintainer_email",counter,ckanjason,parent2)
							ckanjason=ckanjason+ckanjasonLabel


						if counter<len(parent2)-1 and counter>0:

					##---- ckan extras

							xtras,string_matching2,Adddate_updated=HandleExtras(ckandate_updated,"date_updated",parent2,counter,Adddate_updated,xtras)
							xtras,string_matching3,AddCategory=HandleExtras(ckanExtrasCategory,"category",parent2,counter,AddCategory,xtras)
							xtras,string_matching4,AddLanguage=HandleExtras(ckanExtrasLanguage,"language",parent2,counter,AddLanguage,xtras)
							xtras,string_matching5,AddFrequency=HandleExtras(ckanExtrasFrequency,"frequency",parent2,counter,AddFrequency,xtras)
							xtras,string_matching6,Addstate=HandleExtras(ckanstate,"state",parent2,counter,Addstate,xtras)
							xtras,string_matching7,Adddate_released=HandleExtras(ckandate_released,"date_released",parent2,counter,Adddate_released,xtras)
							xtras,string_matching8,Addcountry=HandleExtras(ckancountry,"country",parent2,counter,Addcountry,xtras)
							xtras,string_matching9,AddTemporalCoverage=HandleExtras(ckantemporalcoverage,"temporal_coverage",parent2,counter,AddTemporalCoverage,xtras)
							xtras,string_matching10,Addcity=HandleExtras(ckancity,"city",parent2,counter,Addcity,xtras)

					##--- ckan tags

							string_matching1=difflib.SequenceMatcher(None, ckantags.encode('utf-8'),parent2[0].encode('utf-8').lstrip()).ratio()
							if string_matching1>=num and str(parent2[0].encode('utf-8').lstrip())!='' and '.htm' not in str(parent2[counter].encode('utf-8').lstrip()):
							  if parent2[counter].encode('utf-8')==' ':
								ckanjason=ckanjason+"{'name': '"+str(parent2[counter].encode('utf-8').replace(' ','null').replace("'",'').replace('"','').rstrip())+"'},"
							  else:
								ckanjason=ckanjason+"{'name': '"+str(parent2[counter].encode('utf-8').replace(',',' ').replace(' ','  ').replace("'",'').replace('"','').rstrip())+"'},"

					##-- ckan organization

							string_matching11=difflib.SequenceMatcher(None, ckanorganization.encode('utf-8'),parent2[0].encode('utf-8').lstrip()).ratio()
							if string_matching11>=num and str(parent2[0].encode('utf-8').lstrip())!='': 
								ckanjason=ckanjason+"'organization':{'title': '"+str(parent2[counter].encode('utf-8').replace("'",'').replace('"','').rstrip())+"','type':'organization'},"
								

				##--- ckan resources

				
							ckanjasontemp=HandleResourcesAndLabelElements(ckanresource,parent2,counter,a_link,string_matching1,string_matching2,string_matching3,string_matching4,string_matching5,string_matching6,string_matching7,string_matching8,string_matching9,string_matching10,string_matching11,text_file)
							ckanjason=ckanjason+ckanjasontemp

						if counter==len(parent2)-1 :

					##- ckan extras
							xtras,string_matching2,Adddate_updated=HandleExtras(ckandate_updated,"date_updated",parent2,counter,Adddate_updated,xtras)
							xtras,string_matching3,AddCategory=HandleExtras(ckanExtrasCategory,"category",parent2,counter,AddCategory,xtras)
							xtras,string_matching4,AddLanguage=HandleExtras(ckanExtrasLanguage,"language",parent2,counter,AddLanguage,xtras)
							xtras,string_matching5,AddFrequency=HandleExtras(ckanExtrasFrequency,"frequency",parent2,counter,AddFrequency,xtras)
							xtras,string_matching6,Addstate=HandleExtras(ckanstate,"state",parent2,counter,Addstate,xtras)
							xtras,string_matching7,Adddate_released=HandleExtras(ckandate_released,"date_released",parent2,counter,Adddate_released,xtras)
							xtras,string_matching8,Addcountry=HandleExtras(ckancountry,"country",parent2,counter,Addcountry,xtras)
							xtras,string_matching9,AddTemporalCoverage=HandleExtras(ckantemporalcoverage,"temporal_coverage",parent2,counter,AddTemporalCoverage,xtras)
							xtras,string_matching10,Addcity=HandleExtras(ckancity,"city",parent2,counter,Addcity,xtras)
					##--- ckan tags

							string_matching1=difflib.SequenceMatcher(None, ckantags.encode('utf-8'),parent2[0].encode('utf-8').lstrip()).ratio()

							if string_matching1>=num and str(parent2[0].encode('utf-8').lstrip())!='' and '.htm' not in str(parent2[counter].encode('utf-8').lstrip()):
							  if parent2[counter].encode('utf-8')==' ':
								  ckanjason=ckanjason+"{'name': '"+str(parent2[counter].encode('utf-8').replace(' ','null').replace("'",'').replace('"','').rstrip())+"'}]"+','
							  else:
								ckanjason=ckanjason+"{'name': '"+str(parent2[counter].encode('utf-8').replace(',',' ').replace(' ','  ').replace("'",'').replace('"','').rstrip())+"'}]"+','


					##-- ckan organization
							string_matching11=difflib.SequenceMatcher(None, ckanorganization.encode('utf-8'),parent2[0].encode('utf-8').lstrip()).ratio()
							if string_matching11>=num and str(parent2[0].encode('utf-8').lstrip())!='': 
								ckanjason=ckanjason+"'organization':{'title': '"+str(parent2[counter].encode('utf-8').replace("'",'').replace('"','').rstrip())+"','type':'organization'},"


					##--- ckan resource
							ckanjasontemp1,x=HandleResourceAndLabelElements(x,ckanresource,parent2,counter,a_link,string_matching1,string_matching2,string_matching3,string_matching4,string_matching5,string_matching6,string_matching7,string_matching8,string_matching9,string_matching10,string_matching11,text_file)
							ckanjason=ckanjason+ckanjasontemp1

						if counter>0:
							text_file.write('')
						counter=counter+1

					#text_file.write('\n')

					
					a_link[:]=[]
					#text_file.write('\n')


	j=j+1
  extras_all=""
  count1=0

  while count1<len(xtras):
	if count1==len(xtras)-1:
	  extras_all+=str(xtras[count1])
	else: extras_all+=str(xtras[count1])+","
	count1+=1

  ckanjason=ckanjason+'"extras'+'":['+extras_all.rstrip(',')+"],"
  ckanjason1=str(ckanjason)
  ckanjason=ckanjason1.replace(",,",",")

  xtras[:]=[]
  return ckanjason



## Add Label to Json Element Function

def AddJsonLabelToElement(variable,variable_name,counter,ckanjason,parent2):
  ckanjasonLabel=""
  string_matching=difflib.SequenceMatcher(None, variable.encode('utf-8'),parent2[counter].encode('utf-8').lstrip()).ratio()
  if string_matching>=num and variable_name!="resources" and variable_name!="tags":
	ckanjasonLabel="'"+str(variable_name)+"':'"
  if string_matching>=num and variable_name=="resources":
	ckanjasonLabel="'resources"+"':["
  if string_matching>=num and str(parent2[counter].encode('utf-8').lstrip())!='' and variable_name=="tags":
	ckanjasonLabel="'tags"+"':["
  return ckanjasonLabel




## Handle Extras Function
def HandleExtras(variable,variable_name,parent2,counter,TempBoolean,xtras):
  extra=""
  string_matching=difflib.SequenceMatcher(None, variable.encode('utf-8'),parent2[0].encode('utf-8').lstrip()).ratio()
  if string_matching>=num and counter!=len(parent2)-1:
	if len(parent2)==1:
		extra="{"+"'key':"+'"'+str(variable_name)+'",'+'"value":'+'"'+str(parent2[counter].encode('utf-8').replace("'",'').replace('"','').replace("  ","").replace("	","").replace("\n","").lstrip().rstrip())+'"},'

	if len(parent2)>1 and TempBoolean==False:
		counter3=1
		## if the result is more than 1 string add to a list
		temp1="["
		while counter3<len(parent2):
			if counter3<len(parent2)-1:
			  temp1=temp1+'"'+parent2[counter3].lstrip().rstrip()+'"'+','
			if counter3==len(parent2)-1:
			  temp1=temp1+'"'+parent2[counter3].lstrip().rstrip()+'"'+']'
			counter3+=1
		extra="{"+"'key':"+'"'+str(variable_name)+'",'+'"value":'+str(temp1.encode('utf-8'))+"},"
		TempBoolean=True
  if counter==len(parent2)-1 :
	  if string_matching>=num and TempBoolean==False:
		extra="{"+"'key':"+'"'+str(variable_name)+'",'+'"value":'+'"'+str(parent2[counter].encode('utf-8').replace("'",'').replace('"','').replace("  ","").replace("	","").replace("\n","").lstrip().rstrip())+'"},'
		#xtras.append(temporalcoverage)
		TempBoolean=True
  if extra!="":
	  xtras.append(extra)

  return (xtras,string_matching,TempBoolean)





##Handle Resources Function and all label elements that are not extras
def HandleResourcesAndLabelElements(ckanresource,parent2,counter,a_link,string_matching1,string_matching2,string_matching3,string_matching4,string_matching5,string_matching6,string_matching7,string_matching8,string_matching9,string_matching10,string_matching11,text_file):
  ckanjasontemp=""
  string_matching=difflib.SequenceMatcher(None, ckanresource.encode('utf-8'),parent2[0].encode('utf-8').lstrip()).ratio()

  if string_matching>=num and counter>0:

	  try:
		  url_temp=str(a_link[counter-1].encode('utf-8'))
		  if len(a_link)>=1:
			  type1,filesize=AddLinkToJson.CheckTypeAndFilesize(url_temp)
			  ckanjasontemp=ckanjasontemp+"{'name': '"+str(parent2[counter].encode('utf-8'))+"','url':'"+str(a_link[counter-1])+"'"+",'format':"+"'"+type1+"'"+',"size":'+"'"+str(filesize)+"'"+"},"
		  if len(a_link)<1:
			  type1,filesize=AddLinkToJson.CheckTypeAndFilesize(url_temp)
			  ckanjasontemp=ckanjasontemp+"{'name': '"+str(parent2[counter].encode('utf-8'))+"','url':'"+str(a_link[counter-1])+"'"+",'format':"+"'"+type1+"'"+',"size":'+"'"+str(filesize)+"'"+"}],"

		  #else:ckanjason=ckanjason+"{'name': '"+str(parent2[counter].encode('utf-8'))+"','url':'"+"'},"
	  except IndexError:
		  try:
			  url_temp=str(a_link[counter-2].encode('utf-8'))
			  type1,filesize=AddLinkToJson.CheckTypeAndFilesize(url_temp)
			  if len(a_link)>1:
				  type1,filesize=AddLinkToJson.CheckTypeAndFilesize(url_temp)
				  ckanjasontemp=ckanjasontemp+"{'name': '"+str(parent2[counter-1].encode('utf-8'))+"','url':'"+str(a_link[counter-2])+"'"+",'format':"+"'"+type1+"'"+',"size":'+"'"+str(filesize)+"'"+"},"
			  if len(a_link)<=1:
				  print("type1:  "+str(type1))
				  type1,filesize=AddLinkToJson.CheckTypeAndFilesize(url_temp)
				  ckanjasontemp=ckanjasontemp+"{'name': '"+str(parent2[counter-1].encode('utf-8'))+"','url':'"+str(a_link[counter-2])+"'"+",'format':"+"'"+type1+"'"+',"size":'+"'"+str(filesize)+"'"+"}],"
			  else:ckanjasontemp=ckanjasontemp+"{'name': '"+str(parent2[counter].encode('utf-8'))+"','url':'"+"'},"
		  except:pass

  else :
	  if str(parent2[counter].encode('utf-8').lstrip())!='' and counter>0 and string_matching<=num and string_matching1<=num and string_matching2<num and string_matching3<num and string_matching4<num and string_matching5<num and string_matching6<num and string_matching7<num and string_matching8<num and string_matching9<num and string_matching10<num and string_matching11<num :
		  ckanjasontemp=ckanjasontemp+str(parent2[counter].encode('utf-8').strip().replace("'","").replace('"',''))
  return ckanjasontemp




def HandleResourceAndLabelElements(x,ckanresource,parent2,counter,a_link,string_matching1,string_matching2,string_matching3,string_matching4,string_matching5,string_matching6,string_matching7,string_matching8,string_matching9,string_matching10,string_matching11,text_file):
  ckanjasontemp=""
  string_matching=difflib.SequenceMatcher(None, ckanresource.encode('utf-8'),parent2[0].encode('utf-8').lstrip()).ratio()
  if string_matching>=num :
	  try:
		  url_temp=str(a_link[counter-1].encode('utf-8'))
		  type1,filesize=AddLinkToJson.CheckTypeAndFilesize(url_temp)
		  ckanjasontemp=ckanjasontemp+"{'name': '"+str(parent2[counter].encode('utf-8').replace("'",'').replace('"',''))+"','url':'"+str(a_link[counter-1])+"'"+",'format':"+"'"+type1+"'"+',"size":'+"'"+str(filesize)+"'"+"}]"+','

	  except IndexError:
		  #x+=1
		  try:
			  url_temp=str(a_link[counter-2].encode('utf-8'))
			  type1,filesize=AddLinkToJson.CheckTypeAndFilesize(url_temp)
			  if len(a_link)>1:
				  ckanjasontemp=ckanjasontemp+"{'name': '"+str(parent2[counter-1].encode('utf-8').replace("'",'').replace('"',''))+"','url':'"+str(a_link[counter-2])+"'"+",'format':"+"'"+type1+"'"+',"size":'+"'"+str(filesize)+"'"+"}"+','
			  if len(a_link)<=1:
				  ckanjasontemp=ckanjasontemp+"{'name': '"+str(parent2[counter-1].encode('utf-8').replace("'",'').replace('"',''))+"','url':'"+str(a_link[counter-2])+"'"+",'format':"+"'"+type1+"'"+',"size":'+"'"+str(filesize)+"'"+"}]"+','
		  except IndexError:
					  x+=1
  else:
	  if string_matching1<num and string_matching2<num and string_matching3<num and string_matching4<num and string_matching5<num and string_matching6<num and string_matching7<num and string_matching8<num and string_matching9<num and string_matching10<num and string_matching11<num :
		ckanjasontemp=ckanjasontemp+str(parent2[counter].encode('utf-8').replace("'","").replace('"','')).strip()+"'"+','
  return (ckanjasontemp,x)
