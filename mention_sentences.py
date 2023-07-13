import libvoikko
from datetime import datetime
import numpy as np
import pandas as pd
import rdflib as rdflib
from rdflib.namespace import XSD, RDF, RDFS, Namespace, SKOS, OWL
import re
from SPARQLWrapper import SPARQLWrapper, JSON, POST
from subprocess import run, PIPE   

##### CONSTANTS FOR SPARQL QUERIES #####

ENDPOINT = "http://ldf.fi/semparl/sparql"

PREFIXES = """
PREFIX bioc: <http://ldf.fi/schema/bioc/>
PREFIX crm: <http://erlangen-crm.org/current/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX districts: <http://ldf.fi/semparl/groups/districts/>
PREFIX event: <http://ldf.fi/semparl/event/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX groups: <http://ldf.fi/semparl/groups/>
PREFIX label: <http://ldf.fi/semparl/label/>
PREFIX occupations: <http://ldf.fi/semparl/occupations/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX people: <http://ldf.fi/semparl/people/>
PREFIX places: <http://ldf.fi/semparl/places/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX roles: <http://ldf.fi/semparl/roles/>
PREFIX schema: <http://schema.org/>
PREFIX semparls: <http://ldf.fi/schema/semparl/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX times: <http://ldf.fi/semparl/times/>
PREFIX eterms: <http://ldf.fi/semparl/times/electoral-terms/>
PREFIX titles: <http://ldf.fi/semparl/titles/>
PREFIX xml: <http://www.w3.org/XML/1998/namespace>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX semparl_linguistics: <http://ldf.fi/schema/semparl/linguistics/>
"""

START = '2015-04-22'
END = '2019-04-16'



##### FUNCTIONS FOR HANDLING QUERY RESULTS (by Petri Leskinen) #####

def checkDate(v):
  try:
    d = datetime.strptime(v, '%Y-%m-%dT%H:%M:%S').date()
  except ValueError:
    '''
    cases e.g. 29th february of a non-loop year
    '''
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', v)
    d = datetime(int(m.groups()[0]), int(m.groups()[1]), 28).date()
  return d
  
DATATYPECONVERTERS = {
      str(XSD.integer):  int,
      str(XSD.decimal):  float,
      str(XSD.date):     lambda v: datetime.strptime(v, '%Y-%m-%d').date(),
      str(XSD.dateTime): checkDate
  }

def convertDatatype(obj):
  return DATATYPECONVERTERS.get(obj.get('datatype'), str)(obj.get('value')) 

def convertDatatypes(results):
    res = results["results"]["bindings"]
    return [dict([(k, convertDatatype(v)) for k,v in r.items()]) for r in res]



##### QUERY: Speeches that contain mentions #####

sparql = SPARQLWrapper(ENDPOINT)
sparql.setQuery(PREFIXES+"""
SELECT DISTINCT ?sp ?content ?source ?target ?date (GROUP_CONCAT(DISTINCT ?mentions; SEPARATOR=";") AS ?mention) WHERE {
  BIND('<START>'^^xsd:date AS ?start)
  BIND('<END>'^^xsd:date AS ?end)
 ?sp a semparls:Subcorpus5 .
 ?sp semparl_linguistics:referenceToPerson/skos:relatedMatch ?target ;
     semparls:speaker ?source ; 
     dct:date ?date
  FILTER (?start<=?date && ?date <= ?end)
  ?sp semparls:speechType ?type .
  FILTER (?type != <http://ldf.fi/semparl/speechtypes/PuhemiesPuheenvuoro>) .  
  ?sp dct:language <http://id.loc.gov/vocabulary/iso639-2/fin> .  

  ?sp semparl_linguistics:referenceToPerson [
            skos:relatedMatch ?target ;
            semparl_linguistics:surfaceForm ?mentions  ] .
            

  # Filter that referenced person (?target) is a current MP or minister
  ?target bioc:bearer_of/crm:P11i_participated_in ?event .
  ?event a semparls:ParliamentaryGroupMembership .
  ?event crm:P10_falls_within eterms:e_2015-04-22-2019-04-16 .
  #?event semparls:organization/rdfs:subClassOf/semparls:party ?target_party .
  ?event crm:P4_has_time-span ?tspant .
  ?tspant crm:P81a_begin_of_the_begin ?t_startt .
  OPTIONAL { ?tspant crm:P82b_end_of_the_end ?t_endt } 
  FILTER (?t_startt <= ?date && (!BOUND(?t_endtt) || ?t_endtt >= ?date))
  FILTER (?source != ?target)

  ?sp semparls:content ?content .
}
GROUPBY ?sp ?content ?date ?source ?target  
""".replace("<START>", START).replace("<END>", END))

sparql.setReturnFormat(JSON)
sparql.setMethod(POST)

results = sparql.query().convert()
results2 = convertDatatypes(results)

speeches = set([ob['sp'] for ob in results2])
print("Number of speeches:", len(speeches))

##### Get mention sentences from speeches and lemmitize them #####

sw_file = open('stopwords2.txt')
stop_words = [w.rstrip() for w in sw_file.readlines()]

v = libvoikko.Voikko(u"fi")
people = []
columns = ['speech', 'source', 'target', 'date', 'mention', 'og_sentence', 'lem_sentence', 'misses']
#columns = ['speech', 'source', 'target', 'date', 'mention', 'lem_sentence', 'misses']
data = []
num_mentions = 0 # Total number of mentions in speeches
zentences = 0 # Number of sentences that reduce to empty after filtering stopwords
for d in results2:
    
    mentions = d['mention'].lower().split(";")
    num_mentions += len(mentions)
    mentions.sort()
    mentions.sort(key=len, reverse=True)  #Huomio ehkä taivutusmuodot mutta ei sitä, että viitataan ensin koko nimellä ja sitten pelkällä sukunimellä
    # Toinen ongelma on jos useampi maininta esiintyy samassa lausessa
    #print(mentions)
    cc = re.sub('[\(\[].*?[\)\]]', '', d['content']) # Poista hakasulkeet ja niiden sisällä oleva teksti (huomautukset, välihuudot)
    # Poista/vaihda joitain merkkejä lauseiden tunnistamisen helpottamiseksi
    cc = re.sub('[\n]', ' ', cc)
    cc = re.sub('[\xa0"”;]', '', cc)
    cc = re.sub('[…]', '.', cc)
    if cc[-1] not in '.?!':
      cc += '. S'
    else:
      cc += ' S'
    sentences = re.findall(r'[A-ZÅÄÖ0-9].+?(?=[.?!]\s+[A-ZÅÄÖ0-9])[.?!]', cc)
    
    for m in mentions:
      row = [d['sp'], d['source'], d['target'], d['date'], m]
      if d['source'] not in people:
        people.append(d['source'])
      if d['target'] not in people:
        people.append(d['target'])
      if " " not in m:
        for s in sentences:
          s2 = s
          s = re.sub('[—.,;?!:]','',s)
          for m2 in mentions:  # Removes some mistakes, like http://ldf.fi/semparl/speeches/s2015_1_081_154 (surfaces from 'vehkeet', link to Markku Pakkanen)
            s = s.replace(m2, '')
          s = s.lower()
          words = s.split(" ")
          if m in words:
            #sentences.remove(s)
            s2 = s2.replace(';','')
            row.append(s2)
            misses = 0
            lw_list = []
            for w in words:
              if (w not in stop_words) and (w != m) and ('minister' not in w or 'ministeriö' in w) and (w not in m) and (w.isalpha()):
                voikko_dict = v.analyze(w)
                if voikko_dict:
                  w = voikko_dict[0]['BASEFORM'].lower()
                  if w not in stop_words:
                    lw_list.append(w)
                else:
                  lw_list.append(w)
                  misses += 1
            s3 = " ".join(lw_list)
            if len(s3.strip()) > 0:
              row.append(s3)
              row.append(misses)
              data.append(row)
            else:
              zentences += 1
            break
        else:
          row.append('missing')
          row.append(None)
          row.append(0)
          data.append(row)
      else:
        for s in sentences:
          s2 = s
          s = s.lower()
          if m in s:
            for m2 in mentions:
              s = s.replace(m2, '')
            s = re.sub('[—.,;?!:]','',s)
            s2 = s2.replace(';','')
            row.append(s2)
            words = s.split(" ")
            misses = 0
            lw_list = []
            for w in words:
              if w not in stop_words and ('minister' not in w or 'ministeriö' in w) and w.isalpha():
                voikko_dict = v.analyze(w)
                if voikko_dict:
                  w = voikko_dict[0]['BASEFORM'].lower()
                  if w not in stop_words:
                    lw_list.append(w)
                else:
                  lw_list.append(w)
                  misses += 1
            s3 = " ".join(lw_list)
            if len(s3.strip()) > 0:
              row.append(s3)
              row.append(misses)
              data.append(row)
            else:
              zentences += 1
            break
        else:
          row.append('missing')
          row.append(None)
          row.append(0)
          data.append(row)
          #print(m)
          #print(sentences)
          #print()

      
        
    
df = pd.DataFrame(data, columns=columns)
print("Number of mentions:", num_mentions)
print("Number of mentions sentence found:", len(data))
print("Number of empty sentences", zentences)
df.to_csv("mention_sentences_{:s}_{:s}.csv".format(START, END), sep=';', index=False)


## QUERY MPs

BLOCK = ' '.join({"<{}>".format(s) for s in people})
print(len(people))
q = """
SELECT DISTINCT ?id ?label ?group2 ?date (sample(?colors) AS ?color) WHERE {
  VALUES ?id { <BLOCK> }
  ?id skos:prefLabel ?label .
  ?id semparls:has_party_membership ?partyms .  
  ?partyms semparls:party ?group .
  ?group skos:prefLabel ?group2 .
  FILTER(LANG(?group2)='fi')
  ?group semparls:hexcolor ?colors .
  ?partyms crm:P4_has_time-span ?tspan .
  ?tspan crm:P81a_begin_of_the_begin ?date .
} GROUP BY ?id ?label ?group2 ?date""".replace('<BLOCK>', BLOCK)

sparql.setQuery(PREFIXES + q)
results = sparql.query().convert()

vars = results['head']['vars']
res_nodes = convertDatatypes(results)

end_date = checkDate(END)
people_data = {}
for ob in res_nodes:
  if ob['id'] not in people_data:
    label = " ".join(ob['label'].split(" ")[:-1])
    people_data[ob['id']] = [label, ob['group2'], ob['date'], ob['color']]
  else:
    if ob['date'] > people_data[ob['id']][2] and ob['date'] < end_date:
      label = " ".join(ob['label'].split(" ")[:-1])
      people_data[ob['id']] = [label, ob['group2'], ob['date'], ob['color']]

print(len(people_data))
people_df = pd.DataFrame.from_dict(people_data, orient='index', columns=['name', 'party', 'date', 'color'])
people_df.to_csv("people_{:s}_{:s}.csv".format(START, END), sep=';')

for p in people:
  if p not in people_data:
    print(p)

  




