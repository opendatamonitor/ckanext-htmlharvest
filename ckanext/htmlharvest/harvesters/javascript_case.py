from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
import time
from pyvirtualdisplay import Display


# coding=utf-8
def ParseJavascriptPages(cat_url,dataset_identifier,btn_identifier,action_type,sleep_time):
  #print("sleep_time "+str(sleep_time))
  last_page_parsed=""
  html="html"
  dataset_urls=[]
  display = Display(visible=0, size=(1024, 768))
  display.start()

  
  browser = webdriver.Firefox()
  browser.get(cat_url)
  j=0
  ##get main url
  if 'https' not in cat_url:
	mainurl1=cat_url[cat_url.find('http://')+7:]
	mainurl='http://'+mainurl1[0:mainurl1.find('/')]
  if 'https' in cat_url: 
	mainurl1=cat_url[cat_url.find('http://')+8:]
	mainurl='https://'+mainurl1[0:mainurl1.find('/')]
  if btn_identifier=="2":btn_identifier="1"
  ##while there are next pages
  try:
    while True:
	##go to next page
	  if html!=last_page_parsed:
		j+=1
		print("Parsing page "+str(j))
		try:
		  btn_identifier=int(btn_identifier)
		  btn_identifier+=1
		  btn_identifier=str(btn_identifier)
		except:
		  pass
		
		time.sleep(sleep_time)
		
		last_page_parsed=html
		##get next page html code
		html = browser.page_source
		soup = BeautifulSoup(html)
		##find all links
		links=soup.find_all('a')
		
		
		##check which links are datasets
		i=0
		while i<len(links):
		  try:
			if mainurl=="http://opendata.terrassa.cat":
				link=str(links[i].get('onclick')).replace('showPopup("','').replace('");','')
			else:
				link=str(links[i].get('href'))
		  except:
			link="" 
			
		  if dataset_identifier in link:
			if 'http' not in link:
			  if mainurl=='datos.ua.es':
				link=mainurl+'/en/'+link
			  else:
				if mainurl=="http://data.fingal.ie":
					link=mainurl+'/ViewDataSets/'+link
				else:
					if mainurl=="http://opendata.bruxelles.be":
						link=mainurl+'/explore/'+link+'/?tab=metas'	
					else:
						if link[0]!='/':
						  link=mainurl+'/'+link
						else:
						  link=mainurl+link
			dataset_urls.append(link)
		  	#print(link)
		  i+=1
		  
		  
		##go to next page (if exists)
		if action_type=="id":
		  next_page = browser.find_element_by_id(btn_identifier).click()
		if action_type=="class":
		  next_page = browser.find_element_by_class_name(btn_identifier).click()
		if action_type=="link":
		  next_page = browser.find_element_by_link_text(btn_identifier).click()
	  else:
		#print(dataset_urls)
		#print(len(dataset_urls))
		break

  except:
	#print(dataset_urls)
	print(len(dataset_urls))
  
  ##close browser  
  browser.close();
  display.stop()
  return dataset_urls



