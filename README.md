ckanext-htmlharvest
==========================

A harvester to allow CKAN directories to keep in sync with catalogues that do not provide the data via an API service.

In order to use this tool, you need to have the ODM CKAN harvester extension (https://github.com/opendatamonitor/ckanext-harvestodm) installed and loaded for your CKAN instance. 
Tested with CKAN v2.2 (http://docs.ckan.org/en/ckan-2.2/).

General
---------

The ckanext-htmlharvest is based on Html scrapping. It tries to extract information from the HTML pages. Uses the Python library BeautifulSoup for parsing the HTML code and Selenium library for javascript pagination.
The ckanext-htmlharvest plugin uses the mongo DB as metadata repository and developed as part of the ODM project (www.opendatamonitor.eu).

Building
---------

To build and use this plugin, simply:

    git clone https://github.com/opendatamonitor/ckanext-htmlharvest.git
    cd ckanext-htmlharvest
    pip install -r requirements.txt
    python setup.py develop
    
You also need to install PhantomJS.

Then you will need to update your CKAN configuration to include the new harvester.  This will mean adding the
ckanext-genericapiharvest plugin as a plugin.  E.g.

    ckan.plugins = htmlharvest html_harvester

Also you need to add the odm_extension settings to the development.ini file in your ckan folder.  

    [ckan:odm_extensions]
    mongoclient=localhost
    mongoport=27017
    log_path=/var/local/ckan/default/pyenv/src/
    
Using
---------

After setting this up, you should be able to go to:
    http://localhost:5000/harvest

Select Register a new Catalogue

Select the HTML radiobutton


Licence
---------

This work implements the ckanext-harvest template (https://github.com/ckan/ckanext-harvest) and thus 
licensed under the GNU Affero General Public License (AGPL) v3.0 (http://www.fsf.org/licensing/licenses/agpl-3.0.html).

