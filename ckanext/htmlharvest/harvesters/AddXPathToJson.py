# -*- coding: utf-8 -*-
from lxml import html,etree
import re
import json
from csv import reader
# for testing
import requests

from pymongo import MongoClient
from bson.objectid import ObjectId
# import unicode_fixer

def AddToJson(data_as_text,rules,verbose=False):
    ckanJson = {}

    try:
        for attr in rules['xpath']:
            xpathselector = rules['xpath'][attr]
            tree = html.fromstring(data_as_text)
            root = tree.xpath(xpathselector)

            for tree in root:
                if attr == 'title':
                    title_name = tree.text.replace('\r','').replace('\t','').replace('\n','').strip()
                    if isinstance(title_name, unicode):
                        ckanJson['title'] = title_name.encode('utf8')
                    else:
                        ckanJson['title'] = title_name
                elif attr == 'resource':
                    ckanJson['resources'] = decomposeHTMLsource(tree)

        if verbose:
            print(ckanJson)

        s = rreplace(json.dumps(ckanJson),'}',',',1)
        s = s.split('{',1)

        return ''.join(s)

    except (etree.XMLSyntaxError,etree.XPathEvalError) as e:
        print('hi')
        print(e)
        return None


def rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)


def decomposeHTMLsource(etree):
    def open_nutshell(etree):
        values = []
        # if isinstance(etree,list):
        for c in etree.getchildren():
        # handle tables
            if c.tag == 'table' or c.tag == 'tbody':
                values += handle_table(c)
# handle lists
            elif c.tag in ['ul','li']:
                tmp_val = handle_html_lists(c)
                if tmp_val is not None:
                    # values += [{'url': v} for v in tmp_val]
                    values += tmp_val
            elif c.tag in ['a']:
                if c.get('href') != None and \
                        c.get('href') != '#':
                    values.append({'url': c.get('href')})
            elif len(c):
                values += open_nutshell(c)

        return values

    # result = [{'url': val} for val in open_nutshell(etree) if val is not None]
    result = [val for val in open_nutshell(etree) if val is not None]

    return result


def handle_table(table):
    elem_list = []
    for tag_elem in table.getchildren():
        if tag_elem.tag == 'tbody':
            for tr in tag_elem.getchildren():
                elem = {}
                for td in tr.getchildren():
                    elem.update(get_table_elems(td))

                if 'url' in elem:
                    elem_list.append(elem)

        elif tag_elem.tag == 'tr':
            elem = {}
            for td in tag_elem.getchildren():
                if td.tag == 'th':
                    break

                # elem.update(get_table_elems(td))
                url_info = get_table_elems(td)
                for key,val in url_info.iteritems():
                    if key not in elem:
                        elem[key] = val

            if 'url' in elem:
                elem_list.append(elem)

    return elem_list


def get_table_elems(td):
    gl_elem = {}
    def parse(content):
        elem = {}
        if content.get('href') != None and \
                content.get('href') != '#':
            try:
                code = re.search('\((.*?)\)',content.get('href')).group(1)
                for line in reader(code,delimiter=',',quotechar="'"):
                    if line[0].startswith(('http://','https://','ftp://')):
                        elem['url'] = line[0]
                        break
                if 'url' not in elem:
                    elem['url'] = content.get('href')
            except AttributeError:
                elem['url'] = content.get('href')

        elif content.get('onclick') is not None:
            code = re.search('\((.*?)\)',content.get('onclick')).group(1)
            for line in reader(code.encode('utf8'),delimiter=',',quotechar="'"):
                if line[0].startswith(('http:','https:','ftp:')):
                    elem['url'] = line[0]
                    break
        # this is format or size
        elif content.text is not None:
            if contains_digits(content.text):
                try:
                    sub_str = re.search(r'[a-z]+', content.text, re.I).group()
                    elem['size'] = int(re.search(r'\d+', content.text).group())*get_unit(sub_str)
                except AttributeError:
                    pass
            else:
                if not content.text.isspace():
                    elem['format'] = content.text.replace('\r','').replace('\t','').replace('\n','').strip()

        return elem

    contents = traverse_tags(td)
    if len(contents)>1:
        for content in contents.getchildren():
            gl_elem = parse(content)
            if 'url' in gl_elem:
                break
    else:
        gl_elem = parse(contents)

    return gl_elem


def traverse_tags(c):
    while (len(c) == 1 or c.tag in ['li']) and c.tag not in ['a']:
        for t in c.getchildren():
            new_c = t
        c = new_c

    return c


def handle_html_lists(elem,exclude_list=['a']):
    def expand(elem):
        if len(elem):
            values = []
            for ll in elem.getchildren():
                if ll.tag == 'table':
                    values = handle_table(ll)
                elif ll.tag in exclude_list:
                    for val in values:
                        # print(val.get('href'),ll.get('href'))
                        if val in ll.get('href'):
                            return values

                    values.append({'url':ll.get('href')})
                elif len(ll):
                    values += [k for k in handle_html_lists(ll) if k is not None]

            return values
        elif elem.tag in exclude_list:
            return [elem.get('href')]

    try:
        items = [k for k in expand(elem) if k is not None]

        return items
    except TypeError as e:
        print(e)


_digits = re.compile(r'\d')


def contains_digits(d):
    return bool(_digits.search(d))


def get_unit(s):
    multi = 1
    if s.lower() in ['kb','kbyte', 'kbytes']:
        multi = 1000
    elif s.lower() in ['mb', 'mbytes']:
        multi = 1000000
    elif s.lower() in ['gb','gbytes']:
        multi = 1000000000

    return multi


def convert(s):
    try:
        # print(s)
        print(s.group(0),'hi')
        return s.group(0).encode('utf8').decode('utf8')
    except:
        return s.group(0)


def correct_unicode_names():
    conn = MongoClient('127.0.0.1',27017)
    client = conn['odm']['odm_harmonised']

    # documents = client.find({'_id':ObjectId("562b00879daf0928e4a80ecd")})
    documents = client.find({})
    for doc in documents:
        # print "ASCII value: ", ''.join(unichr(ord(c)) for c in doc['title'])
        # a = re.sub(ur'[\u0000-\u00FF]+',convert,doc['title'])
        # print(unicode_fixer.fix_bad_unicode(doc['title']))
        # print(unichr(ord(u'\u00f1')))
        try:
            if 'title' in doc and isinstance(doc['title'],unicode):
                client.update({'_id':doc['_id']},
                        { '$set': {'title': doc['title'].decode('unicode-escape') }}
                        )
        except UnicodeEncodeError,e:
            print(e,doc['_id'],doc['title'])


def test():
    rules = {}

    # rules['xpath'] = {'resource':"//div[@id='main']/div[contains(@class,'risp_dataset')]/div[@class='item-fields']/div[@class='formato']/div[@class='format_dataset']/ul[@class='dataset-formats']"}
    # url = "http://transparencia.gijon.es/set/economia/presupuesto_gastos_fmceup_2013"
    # url = "http://transparencia.gijon.es/set/transporte/aparcamientos_subterraneos"

    # rules['xpath'] = {'resource':"//div[@id='div_ver']/ul"}
    # url = "http://opendata.badalona.cat/badalona/es/catalog/SEGURETAT/actuacionsguardiaurbana/"
    # url = "http://opendata.badalona.cat/badalona/es/catalog/URBANISME_I_INFRAESTRUCTURES/districtescensalsgrafics/"
    # url = "http://opendata.badalona.cat/badalona/es/catalog/SECTOR_PUBLIC/elecautonomiques/"

    # rules['xpath'] = {'resource': "//div[@id='fila1']/div[contains(@class,'columna')]//div//div[@class='contidoSuperior']/div[contains(@class,'contidoInferior')]/div[contains(@class,'fila')]/div[@class='tablaDescargaCen']/div[@class='tablaDescargaInf']/div[@class='tablaDescargaSup']/div[@class='tablaDescargas']"}
    # url = "http://abertos.xunta.es/catalogo/territorio-vivienda-transporte/-/dataset/0347/ortofoto-galicia-2004-2005"
    # url = "http://abertos.xunta.es/catalogo/economia-empresa-emprego/-/dataset/0358/calendario-festivos-habiles-para-practica"
    # url = "http://abertos.xunta.es/catalogo/saude-servicios-sociais/-/dataset/0325/enquisa-condicions-vida-das-familias-coidado"

    # rules['xpath'] = {'resource': "//div[@id='div_ver']"}
    # url = "http://opendata.viladecans.cat/viladecans/ca/catalog/URBANISME_I_INFRAESTRUCTURES/carrerer/"

    # rules['xpath'] = {'resource': "//div[@id='div_ver']"}
    # url = "http://opendata.cornella.cat/cornella/ca/catalog/CULTURA_I_OCI/agendadiaria/"
    # url = "http://opendata.cornella.cat/cornella/ca/catalog/SOCIETAT_I_BENESTAR/entitats/"
    # url = "http://opendata.cornella.cat/cornella/ca/catalog/ESPORT/estadisticaesports/"
    # url = "http://opendata.cornella.cat/cornella/ca/catalog/SECTOR_PUBLIC/participaciomunicipals/"
    # url = "http://opendata.cornella.cat/cornella/ca/catalog/URBANISME_I_INFRAESTRUCTURES/planoldistrictes/"

    # rules['xpath'] = {'resource': "//div[@id='div_ver']"}
    # url = "http://opendata.santfeliu.cat/santfeliu/ca/catalog/DEMOGRAFIA/immigracionspadro/"
    # url = "http://opendata.santfeliu.cat/santfeliu/ca/catalog/SECTOR_PUBLIC/empreses/"
    # url = "http://opendata.santfeliu.cat/santfeliu/ca/catalog/URBANISME_I_INFRAESTRUCTURES/districtescensalsgrafics/"
    # url = "http://opendata.santfeliu.cat/santfeliu/es/catalog/DEMOGRAFIA/baixesemigraciopadro/"

    # rules['xpath'] = {'resource': "//div[@id='div_ver']"}
    # url = "http://opendata.elprat.cat/elprat/ca/catalog/URBANISME_I_INFRAESTRUCTURES/carrilsbici/"
    # url = "http://opendata.elprat.cat/elprat/ca/catalog/TRANSPORT/censvehicles/"
    # url = "http://opendata.elprat.cat/elprat/ca/catalog/SECTOR_PUBLIC/perfilcontractantcaoc/"

    # rules['xpath'] = {'resource': "//div[@id='div_ver']"}
    # url = "http://opendata.cloudbcn.cat/MULTI/ca/catalog/SEGURETAT/actuacionsguardiaurbana/"
    # url = "http://opendata.cloudbcn.cat/MULTI/ca/catalog/CULTURA_I_OCI/dadesmerce/"

    # rules['xpath'] = {'resource': "//div[@id='webcontainer']/div[@class='content-all']//div[@class='cont-txt']/div[@class='cont-form']/div[@class='blq-col']/dl[@class='fondo_gris']//ul[contains(@class,'rlist_ul')]"}
    # url = "http://www.bilbao.net/opendata/es/catalogo/dato-contratistas"
    # url = "http://www.bilbao.net/opendata/es/catalogo/dato-contratos-suministros"

    # rules['xpath'] = {'resource': "//div[@id='contentMain']/div[@class='box']/div[@class='contentBox']/table[@class='accessData']"}
    # url = "http://dadesobertes.gencat.cat/ca/cercador/detall-cataleg/?id=590"
    # url = "http://dadesobertes.gencat.cat/ca/cercador/detall-cataleg/?id=7710"
    # url = "http://dadesobertes.gencat.cat/ca/cercador/detall-cataleg/?id=7370"
    # url = "http://dadesobertes.gencat.cat/ca/cercador/detall-cataleg/?id=4278"
    # url = "http://dadesobertes.gencat.cat/ca/cercador/detall-cataleg/?id=82"

    # rules['xpath'] = {'resource': "//div[@id='div_ver']"}
    # url = "http://opendata.bcn.cat/opendata/ca/catalog/SEGURETAT/accidentsgubcn/"
    # url = "http://opendata.bcn.cat/opendata/es/catalog/LEGISLACIO_I_JUSTICIA/vignette6bop2972002325pdf/"

    # rules['xpath'] = {'resource': "//div[@id='fichadatos']/div[@class='datos_distribucion']"}
    # url = "http://datos.gob.es/catalogo/presupuestos-2015"
    # url = "http://datos.gob.es/catalogo/presupuestos-2015-0"
    # url = "http://datos.gob.es/catalogo/indicadores-demograficos-basicos"
    # url = "http://datos.gob.es/catalogo/indicadores-de-alta-tecnologia"
    # rules['xpath'] = {'title': "//h1[@id='page-title']"}
    # url = "http://datos.gob.es/catalogo/sistema-de-ocupacion-del-suelo-de-espana-siose-ano-2009"

    # rules['xpath'] = {'title': "//div[@id='containercontBuscadorDatos']/div[@class='content']/div[@class='inside']/div//h1"}
    # url = "http://opendata.euskadi.eus/katalogoa/-/kultur-baliabideen-direktorioa/"

    # rules['xpath'] = {'resource': "(//div[@id='main']//p)[14]"}
    # url = "http://www.mulhouse.fr/fr/Perimetre-et-caracteristiques-des-circonscriptions/Perimetre-et-caracteristiques-des-circonscriptions.html"
    # url = "http://www.mulhouse.fr/fr/Perimetre-et-caracteristiques-des-arrondissements/Perimetre-et-caracteristiques-des-arrondissements.html"

    # rules['xpath'] = {'resource': "(//body[@id='body']/div[contains(@class,'main')]/div[@class='main-bg']/div[@class='main-width']/div[@class='one-column']/div[@class='content']/div[contains(@class,'node')]/div[@class='indent']/div[@class='text-box']/div[contains(@class,'field')]/div[@class='field-items']/div[contains(@class,'odd')]/div[contains(@class,'node')]/div[@class='indent']/div[contains(@class,'column-data')]/div[@class='content-data'])[3]"}
    # url = "http://opendata.bordeaux.fr/content/budget-2012-par-politique-publique"

    # rules['xpath'] = {'resource': "(//div[@id='cContent']/div[@class='block']/div[@class='rolodex']/div[@class='content']/div[@class='method']/ul[@class='formatList'])[1]"}
    # url = "http://opendata.enschede.nl/opendata/dataset/?dataset=verkiezingsuitslag&method=view.html"
    # url = "http://opendata.enschede.nl/opendata/dataset/?dataset=straatnamen&method=view.html"

    # rules['xpath'] = {'resource': "(//div[@id='content']/section[contains(@class,'post')]//div[contains(@class,'clearfix')])[1]"}
    # url = "http://data.groningen.nl/financiele-gegevens-2013-gemeente-groningen-in-iv3-formaat/"

    # rules['xpath'] = {'resource': "//div[@id='container']//div//div//div[contains(@class,'nopadding')]/div[contains(@class,'table-artefacts')]/table"}
    # url = "https://digitaliser.dk/resource/2533742"
    # url = "https://digitaliser.dk/resource/2687606"
    # url = "https://digitaliser.dk/resource/432461"

    rules['xpath'] = {'resource': "//table[@id='resTable']"}
    url = "https://open.wien.gv.at//site/datensatz/?id=fc21780d-bbb0-4ec9-9cb2-8687fd28ebc2"
    url = "https://open.wien.gv.at//site/datensatz/?id=7c6e9dbf-487b-4146-a379-19e46642c392"

    r = requests.get(url)
    AddToJson(r.content,rules,True)


if __name__ == '__main__':
    test()
    # correct_unicode_names()
