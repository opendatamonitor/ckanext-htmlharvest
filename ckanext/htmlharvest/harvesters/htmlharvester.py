import urllib2

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json

from ckanext.harvestodm.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError
import pymongo
import logging
import configparser
import harvester_final
log = logging.getLogger(__name__)
from base import HarvesterBase
import harvest_url
import GetHarvestRules
import RdfToJson
import javascript_case


##read from development.ini file all the required parameters
config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
log_path=config['ckan:odm_extensions']['log_path']
html_harvester_log_file_path=str(log_path)+'ckanext-htmlharvest/ckanext/htmlharvest/harvesters/html1.log'
mongoclient=config['ckan:odm_extensions']['mongoclient']
mongoport=config['ckan:odm_extensions']['mongoport']

text_file = open(str(html_harvester_log_file_path), "a")
client = pymongo.MongoClient(str(mongoclient), int(mongoport))

class HTMLHarvester(HarvesterBase):
    '''
    A Harvester for HTML-based instances
    '''
    config = None

    api_version = 2

    def _get_rest_api_offset(self):
        return '/api/%d/rest' % self.api_version

    def _get_search_api_offset(self):
        return '/api/%d/search' % self.api_version

    def _get_content(self, url):
        http_request = urllib2.Request(
            url = url,
        )

        api_key = self.config.get('api_key',None)
        if api_key:
            http_request.add_header('Authorization',api_key)
        http_response = urllib2.urlopen(http_request)

        return http_response.read()

    def _get_group(self, base_url, group_name):
        url = base_url + self._get_rest_api_offset() + '/group/' + group_name
        try:
            content = self._get_content(url)
            return json.loads(content)
        except Exception, e:
            raise e

    def _set_config(self,config_str):
        if config_str:
            self.config = json.loads(config_str)
            if 'api_version' in self.config:
                self.api_version = int(self.config['api_version'])

            log.debug('Using config: %r', self.config)
        else:
            self.config = {}

    def info(self):
        return {
            'name': 'html',
            'title': 'HTML',
            'description': 'Harvests remote HTML instances',
            'form_config_interface':'Text'
        }

    def validate_config(self,config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'api_version' in config_obj:
                try:
                    int(config_obj['api_version'])
                except ValueError:
                    raise ValueError('api_version must be an integer')

            if 'default_tags' in config_obj:
                if not isinstance(config_obj['default_tags'],list):
                    raise ValueError('default_tags must be a list')

            if 'default_groups' in config_obj:
                if not isinstance(config_obj['default_groups'],list):
                    raise ValueError('default_groups must be a list')

                # Check if default groups exist
                context = {'model':model,'user':c.user}
                for group_name in config_obj['default_groups']:
                    try:
                        group = get_action('group_show')(context,{'id':group_name})
                    except NotFound,e:
                        raise ValueError('Default group not found')

            if 'default_extras' in config_obj:
                if not isinstance(config_obj['default_extras'],dict):
                    raise ValueError('default_extras must be a dictionary')

            if 'user' in config_obj:
                # Check if user exists
                context = {'model':model,'user':c.user}
                try:
                    user = get_action('user_show')(context,{'id':config_obj.get('user')})
                except NotFound,e:
                    raise ValueError('User not found')

            for key in ('read_only','force_all'):
                if key in config_obj:
                    if not isinstance(config_obj[key],bool):
                        raise ValueError('%s must be boolean' % key)

        except ValueError,e:
            raise e

        return config


    def gather_stage(self,harvest_job):
	  
		print('Html Harvest Gather Stage')
		db2 = client.odm
		collection=db2.html_jobs
		backupi=0
        ## Get source URL
		source_url = harvest_job.source.url
		## mongoDb connection
		document=collection.find_one({"cat_url":source_url})
		id1=document['_id']
		if 'btn_identifier' in document.keys():
		  if document['btn_identifier']!=None and document['btn_identifier']!='':
			cat_url=document['cat_url']
			dataset_identifier=document['identifier']
			btn_identifier=document['btn_identifier']
			action_type=document['action_type']
			try:
				sleep_time=document['sleep_time']
			except:
				sleep_time=3
			package_ids=javascript_case.ParseJavascriptPages(cat_url,dataset_identifier,btn_identifier,action_type,sleep_time)
			print(package_ids)
		  else:
			package_ids=harvester_final.read_data(id1,backupi)
		else:
			package_ids=harvester_final.read_data(id1,backupi)
		print(package_ids)
		#print(len(package_ids))
		#package_ids=[]
		#package_ids.append('http://data.belgium.be/dataset/mortality-tables-gender')
		#package_ids.append('test')
		try:
		    object_ids = []
		    if len(package_ids):
		        for package_id in package_ids:
		            # Create a new HarvestObject for this identifier
		            obj = HarvestObject(guid = package_id, job = harvest_job)
		            obj.save()
		            object_ids.append(obj.id)

		        return object_ids

		    else:
		       self._save_gather_error('No packages received for URL: %s' % source_url,
		               harvest_job)
		       return None
		except Exception, e:
		    self._save_gather_error('%r'%e.message,harvest_job)




    def fetch_stage(self,harvest_object):
	  
		log.debug('Html Harvest Fetch Stage')
		backupi=0
		dataset_url=harvest_object.guid
		mainurl=""
		if 'http://' in dataset_url:
		  mainurl1=dataset_url[dataset_url.find('http://')+7:]
		  mainurl='http://'+mainurl1[0:mainurl1.find('/')]
		if 'https://' in dataset_url:
		  mainurl1=dataset_url[dataset_url.find('https://')+8:]
		  mainurl='https://'+mainurl1[0:mainurl1.find('/')]
		
		## get the harvest rules
		#try:
		try:
		  rules=GetHarvestRules.GetHarvestRules(str(mainurl))
		except:rules=""
		print(rules)
		if rules!="":
		  if 'rdf' in rules.keys():
			if rules['rdf']!='' and rules['rdf']!=None:
			  content=str(RdfToJson.harvest_rdf(dataset_url,rules))
			else:
			  content=str(harvest_url.harvest_url(dataset_url,rules))

	  #print(rules)
	  ## content must be a string 
		  else:
			content=str(harvest_url.harvest_url(dataset_url,rules))
		  harvest_object.content = content
		  try:
			  harvest_object.save()
		  except:
			  pass
		#except:pass
        	return True



    def import_stage(self,harvest_object):
        log.debug('In HTMLHarvester import_stage')
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,
                    harvest_object, 'Import')
            return False

        self._set_config(harvest_object.job.source.config)

        try:
            harvest_object.content=harvest_object.content.replace("'",'"')

            #package_dict=harvest_object.content
            package_dict = json.loads(harvest_object.content.decode('utf-8','ignore'))

            
            ## handle notes validation errors as existance of:  " and / 
            extrasjson=[]
            try:
                extras=package_dict['extras']
            except: extras=""
            j=0
            ##transformations to json's extras
            if 'value' in str(extras) and 'key' in str(extras):

			  extrasjson[:]=[]
			  extrasjson2=""
			  while j< len(package_dict['extras']):
				extra_key=package_dict['extras'][j]['key']
				extra_value=package_dict['extras'][j]['value']
				if len(extra_value)>0:

				  c=0
				  extra_value1=""

				  while c<len(extra_value):
					extra_value1=extra_value1+extra_value[c]
					c+=1

				  c=0
				  extra_value=extra_value1
				extra='"'+str(extra_key.encode('utf-8'))+'":'+'"'+str(extra_value.encode('utf-8'))+'"'
				extrasjson.append(extra)

				j+=1

			  k=0
			  extrasjson1=""

			  while k<len(extrasjson):
				extrasjson1=extrasjson1+extrasjson[k]+","
				k+=1

			  k=0
			  j=0

			  extrasjson1="{"+extrasjson1.rstrip(',')+"}"

			  try:
				  extrasjson2=json.loads(extrasjson1)
			  except:
				  errorscounter+=1

			  if len(extrasjson)>0:
				package_dict.update({"extras":extrasjson2})

            try:

              tags=package_dict['tags']
              j=0

              if 'name' in str(tags):

	            while j< len(package_dict['tags']):

	              tag=package_dict['tags'][j]['name']
	              tagsarray.append(tag)
	              j+=1

              if len(tagsarray)>0:
	            package_dict.update({"tags":tagsarray})


              tagsarray[:]=[]
              j=0

            except:
             pass
  
            if package_dict.get('type') == 'harvest':
                log.warn('Remote dataset is a harvest source, ignoring...')
                return True

            # Set default tags if needed
            default_tags = self.config.get('default_tags',[])
            if default_tags:
                if not 'tags' in package_dict:
                    package_dict['tags'] = []
                package_dict['tags'].extend([t for t in default_tags if t not in package_dict['tags']])

            remote_groups = self.config.get('remote_groups', None)
            if not remote_groups in ('only_local', 'create'):
                # Ignore remote groups
                package_dict.pop('groups', None)
            else:
                if not 'groups' in package_dict:
                    package_dict['groups'] = []

                # check if remote groups exist locally, otherwise remove
                validated_groups = []
                context = {'model': model, 'session': Session, 'user': 'harvest'}

                for group_name in package_dict['groups']:
                    try:
                        data_dict = {'id': group_name}
                        group = get_action('group_show')(context, data_dict)
                        if self.api_version == 1:
                            validated_groups.append(group['name'])
                        else:
                            validated_groups.append(group['id'])
                    except NotFound, e:
                        log.info('Group %s is not available' % group_name)
                        if remote_groups == 'create':
                            try:
                                group = self._get_group(harvest_object.source.url, group_name)
                            except:
                                log.error('Could not get remote group %s' % group_name)
                                continue

                            for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name']:
                                group.pop(key, None)
                            get_action('group_create')(context, group)
                            log.info('Group %s has been newly created' % group_name)
                            if self.api_version == 1:
                                validated_groups.append(group['name'])
                            else:
                                validated_groups.append(group['id'])

                package_dict['groups'] = validated_groups

            context = {'model': model, 'session': Session, 'user': 'harvest'}

            # Local harvest source organization
            #source_dataset = get_action('package_show')(context, {'id': harvest_object.source.id})
            #local_org = source_dataset.get('owner_org')

            #remote_orgs = self.config.get('remote_orgs', None)

            #if not remote_orgs in ('only_local', 'create'):
                ## Assign dataset to the source organization
                #package_dict['owner_org'] = local_org
            #else:
                #if not 'owner_org' in package_dict:
                    #package_dict['owner_org'] = None

                ## check if remote org exist locally, otherwise remove
                #validated_org = None
                #remote_org = package_dict['owner_org']

                #if remote_org:
                    #try:
                        #data_dict = {'id': remote_org}
                        #org = get_action('organization_show')(context, data_dict)
                        #validated_org = org['id']
                    #except NotFound, e:
                        #log.info('Organization %s is not available' % remote_org)
                        #if remote_orgs == 'create':
                            #try:
                                #org = self._get_group(harvest_object.source.url, remote_org)
                                #for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name', 'type']:
                                    #org.pop(key, None)
                                #get_action('organization_create')(context, org)
                                #log.info('Organization %s has been newly created' % remote_org)
                                #validated_org = org['id']
                            #except:
                                #log.error('Could not get remote org %s' % remote_org)

                #package_dict['owner_org'] = validated_org or local_org

            # Set default groups if needed
            default_groups = self.config.get('default_groups', [])
            if default_groups:
                package_dict['groups'].extend([g for g in default_groups if g not in package_dict['groups']])

            

            # Set default extras if needed
            default_extras = self.config.get('default_extras',{})
            if default_extras:
                override_extras = self.config.get('override_extras',False)
                if not 'extras' in package_dict:
                    package_dict['extras'] = {}
                for key,value in default_extras.iteritems():
                    if not key in package_dict['extras'] or override_extras:
                        # Look for replacement strings
                        if isinstance(value,basestring):
                            value = value.format(harvest_source_id=harvest_object.job.source.id,
                                     harvest_source_url=harvest_object.job.source.url.strip('/'),
                                     harvest_source_title=harvest_object.job.source.title,
                                     harvest_job_id=harvest_object.job.id,
                                     harvest_object_id=harvest_object.id,
                                     dataset_id=package_dict['id'])

                        package_dict['extras'][key] = value

            # Clear remote url_type for resources (eg datastore, upload) as we
            # are only creating normal resources with links to the remote ones
            for resource in package_dict.get('resources', []):
                resource.pop('url_type', None)

            result = self._create_or_update_package(package_dict,harvest_object)

            if result and self.config.get('read_only',False) == True:

                package = model.Package.get(package_dict['id'])

                # Clear default permissions
                model.clear_user_roles(package)

                # Setup harvest user as admin
                user_name = self.config.get('user',u'harvest')
                user = model.User.get(user_name)
                pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

                # Other users can only read
                for user_name in (u'visitor',u'logged_in'):
                    user = model.User.get(user_name)
                    pkg_role = model.PackageRole(package=package, user=user, role=model.Role.READER)


            return True
        except ValidationError,e:
            self._save_object_error('Invalid package with GUID %s: %r' % (harvest_object.guid, e.error_dict),
                    harvest_object, 'Import')
        except Exception, e:
            self._save_object_error('%r'%e,harvest_object,'Import')

