from bs4 import BeautifulSoup
import urllib2
import requests
import time
from xml.dom.minidom import parseString
import re
import difflib
import logging

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
candidates=[]


parent_attrs_list=[]
parent_name_list=[]
parent_name_list1=[]
parent_attrs_list1=[]
commands=[]


url=""


ids=[]

#---method NoLabelDataGetDiv()
#- This method gets the path (html-tree) to non label data

def LinksGetDiv(url,links):

	i=0
	j=0
	k=0

	try:

		r  = requests.get(url)
		data2 = r.text
		soup2 = BeautifulSoup(data2)

		#-- make a list of all data in page
		soup3=soup2.findAll(text=True)



		while j<len(links):
			while k<len(soup3):
				if links[j] in soup3[k]:

					links[j]=soup3[k]

				k=k+1
			k=0
			j=j+1
		j=0



	except urllib2.HTTPError, e:
		print('http error')
		#text_file.write("404 ERROR")
	return str(links[0].encode('utf-8'))
	links[:]=[]



def LabelDataGetDiv(url,label):

	i=0
	j=0
	k=0

	temp=""
	try:

		r  = requests.get(url)
		data2 = r.text
		soup2 = BeautifulSoup(data2)

		#-- make a list of all data in page
		soup3=soup2.findAll(text=True)



		while j<len(label):
			while k<len(soup3):
				if label[j] in soup3[k]:
					candidates.append(soup3[k])
					#label[j]=soup3[k]
				k=k+1
			k=0
			j=j+1
		j=0
		matching=0
		#print("************")
		while j<len(candidates):
			string_matching=0
			string_matching=difflib.SequenceMatcher(None, candidates[j].encode('utf-8'),label[0].encode('utf-8')).ratio()
			#print(str(string_matching)+ " " + str(candidates[j].encode('utf-8'))+" "+str(label[0].encode('utf-8')))
			if string_matching>=matching:
				matching=string_matching
				temp=candidates[j]
			j+=1
		j=0
		label[0]=temp
		candidates[:]=[]	
		label1={}


	except urllib2.HTTPError, e:
		print('httperror')
		#text_file.write("404 ERROR")
	return str(label[0].encode('utf-8'))
	label[:]=[]

def NoLabelDataGetDiv(url,nonlabel):

	#text_file.write(str(nonlabel))
	i=0
	j=0
	k=0
	all_ids[:]=[]
	commands[:]=[]
	found=False
	try:

		r  = requests.get(url)
		data2 = r.text
		soup2 = BeautifulSoup(data2)

		#-- make a list of all data in page
		soup3=soup2.findAll(text=True)

		
		#-- find non label
		while j<len(nonlabel):

			while k<len(soup3):

				if nonlabel[j] in soup3[k]:

				#--Check if non label data are included in that
					nonlabel[j]=soup3[k]
					found=True
				
				k=k+1
			k=0
			counter=len(nonlabel[j])
			while counter>=0:
				counter-=5
				if found==False:
					#print(counter)
					#print(nonlabel[j])
					nonlabel[j]=nonlabel[j][0:counter]
					while k<len(soup3):

						if nonlabel[j] in soup3[k]:

					#--Check if non label data are included in that
							nonlabel[j]=soup3[k]
							found=True
				
						k=k+1
					k=0
			
			j=j+1
			found=False
		j=0




		while j<len(nonlabel) :

			if nonlabel[j] in soup3:

				for x in range(0,len(soup3)):
					if(soup3[x]==nonlabel[j]):

						value_parent2=soup3[x]
						count=0
						#-- call Find_Parents method
						Find_Parents(value_parent2)

						parent_name_list1=parent_name_list[::-1]
						parent_attrs_list1=parent_attrs_list[::-1]

						counter2=0

						string_to_match=value_parent2.parent
						if str(string_to_match.name) != 'a' and str(string_to_match.name) != 'p':
							bottom_string=string_to_match

						else: bottom_string=string_to_match.parent
						countInputTimes=0

						#-- call find_ids method
						find_ids(counter2,bottom_string,j,countInputTimes,nonlabel)


						magiccommand='soup2.'
						counterr=0
						id_temp=all_ids[j]
						id_temp1=id_temp.strip(':')
						id_temp2=id_temp1.split(':')


						while counterr<len(parent_name_list1):
							try:
								magiccommand=magiccommand+"find_all("+"'"+parent_name_list1[counterr]+"'"+",recursive=False)["+str(id_temp2[counterr])+"]."

							except IndexError: nothing=0
							counterr=counterr+1

						magiccommand1=magiccommand.rstrip().rstrip('.')+'.getText()'+".encode('utf-8')"

						parent_name_list[:]=[]
						parent_attrs_list[:]=[]
						parent_name_list1[:]=[]
						parent_attrs_list1[:]=[]



						magiccommand2='text_file.write(str('+magiccommand1+'.lstrip().rstrip()))'
						if magiccommand2 not in commands:
							commands.append(magiccommand2)
						counterr=0
				#-- clearing ids storage


			j=j+1



#-- exception handling
	except urllib2.HTTPError, e:
		print('httperror')
		#text_file.write("404 ERROR")

	return str(commands[0])








#end of NoLabelDataGetDiv()

def Find_Parents(value_parent2):
	for parent in value_parent2.parents:
  		if parent is None:
       			print('None')

   		else:
			if parent.name !='[document]' and parent.name!='p' and parent.name!='a':
       				parent_name=parent.name
				parent_name_list.append(parent_name)
				parent_attrs=parent.attrs
				parent_attrs_list.append(parent_attrs)

			parent_name_list1=parent_name_list[::-1]
			parent_attrs_list1=parent_attrs_list[::-1]


#-- this methods finds the attributes ids in html tree .
def find_ids(counter2,bottom_string,j,countInputTimes,nonlabel):
	while counter2<len(parent_name_list):


		name=parent_name_list[counter2]

		if bottom_string.parent!='[document]':

			id1=(len(bottom_string.parent.find_all(name, recursive=False)))

			if id1==1:
				bottom_string=bottom_string.parent
				ids.append(id1-1)
			else:
				nextOne=nonlabel[j]

				count=0

				while count < id1:
					count1=counter2
					bottom_string1=bottom_string.parent.find_all(parent_name_list[count1], recursive=False)[count]

					try:
						while count1>1:
							count1=count1-1
							bottom_string1=bottom_string1.find_all(parent_name_list[count1], recursive=False)[ids[count1]]

						bottom_string1=bottom_string1.getText()
					except IndexError: nothing=0

					if nextOne in str(bottom_string1.encode('utf-8')).decode('utf-8'):
						bottom_string=bottom_string.parent
						ids.append(count)

						break

					count+=1

		ids1=ids[::-1]

		counter2=counter2+1
		countInputTimes+=1
	if ids not in all_ids and len(all_ids)<=j :
		k=0
		temp=''

		while k<len(ids1):
			temp=temp+':'+str(ids1[k])
			k=k+1

		all_ids.append(temp)


	ids[:]=[]

	ids1[:]=[]

