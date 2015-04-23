import SaveLabels

def MetadataFinder(temp_feature_name,temp_name,data,url):

	label=[]
	nonlabel=[]
	links=[]
	temp_feature=data[str(temp_feature_name)]
	temp=data[str(temp_name)]
	if temp!="":
		if temp_feature=='label':
			label.append(temp)
			temp1=SaveLabels.LabelDataGetDiv(url,label)
		if temp_feature=='link':
			links.append(temp)
			temp1=SaveLabels.LinksGetDiv(url,links)
		if temp_feature=='value':
			nonlabel.append(temp)
			temp1=SaveLabels.NoLabelDataGetDiv(url,nonlabel)
			label[:]=[]
			nonlabel[:]=[]
			links[:]=[]
	else: temp1=""
	return temp1,temp_feature
