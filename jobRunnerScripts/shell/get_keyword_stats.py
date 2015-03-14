#!/usr/bin/python
#
# Retrieve keyword stats from the GoogleAdWords api.
# Relies on having the neccessary credentials in a file 
# named googleads.yaml in the user's home directory.
# Reads keywords from a file and outputs to a csv file.
# 
import argparse
import csv
import codecs
import unicodedata
import sys
from googleads import adwords
import time

def query_google_api(query_array, writer):
  PAGE_SIZE = 500
  offset = 0
  more_pages = True
  i = 0

  language_id = -1
  location_id = -1
  #print 'query_google_api locale = ' + locale
  if locale == 'en_US':
    language_id = 1000
    location_id = 2840
  elif locale == 'en_GB':
    language_id = 1000
    location_id = 2826
  elif locale == 'en_IE':
    language_id = 1000
    location_id = 2372
  elif locale == 'de_DE':
    language_id = 1001
    location_id = 2276
  elif locale == 'es_ES':
    language_id = 1003
    location_id = 2724
  elif locale == 'fr_FR':
    language_id = 1002
    location_id = 2250
  elif locale == 'it_IT':
    language_id = 1004
    location_id = 2380
  elif locale == 'en_CA':
    language_id = 1000
    location_id = 2124
  elif locale == 'fr_CA':
    language_id = 1011
    location_id = 2124
  else:
    print 'Got an incorrect locale : ' + locale
    raise

  # Construct selector object 
  selector = {
    'searchParameters': [
        {
            'xsi_type': 'RelatedToQuerySearchParameter',
            'queries': query_array
        },
        {
            # Language setting (optional).
            # The ID can be found in the documentation:
            #  https://developers.google.com/adwords/api/docs/appendix/languagecodes
            'xsi_type': 'LanguageSearchParameter',
            'languages': [{'id': str(language_id)}]
        },
        {
            # Location
            # The ID can be found in the documentation:
            #  https://developers.google.com/adwords/api/docs/appendix/geotargeting
            'xsi_type': 'LocationSearchParameter',
            'locations': [{'id': str(location_id)}]
        }
    ],
    'ideaType': 'KEYWORD',
    'requestType': 'STATS',
    'requestedAttributeTypes': ['KEYWORD_TEXT','SEARCH_VOLUME', 'AVERAGE_CPC', 'COMPETITION'],
    'paging': {
        'startIndex': str(offset),
        'numberResults': str(PAGE_SIZE)
    }
  }

  #print '  staring while loop in query_google_api'
  while more_pages:
    page = targeting_idea_service.get(selector)

    # Display results.
    if 'entries' in page:
      for result in page['entries']:
        # some keywords may not have a result. Google does not return an empty result, 
        # just gives us the next result. So check whether keyword we sent matches 
        # keyword we got back. If not, print an empty row. 
        attributes = {}
        for attribute in result['data']:
            attributes[attribute['key']] = attribute['value']
        
        spreadsheet_row = dict()
 
        keyword_returned = str(attributes.get('KEYWORD_TEXT')['value'])
        while (i < len(query_array)) and (query_array[i].lower() != keyword_returned.lower()):
          # print an empty row
          spreadsheet_row['locale'] = locale
          spreadsheet_row['keyword'] = query_array[i]
          spreadsheet_row['keyword_text'] = ''
          spreadsheet_row['search_volume'] = '0'
          spreadsheet_row['competition'] = '0.00'
          spreadsheet_row['average_cpc'] = '0.00'
          writer.writerow(spreadsheet_row)
          spreadsheet_row = dict()
          i = i+1
        
        if i == len(query_array):
          # we shouldn't get here, so print some debugging info to see why we did
          print 'keyword_returned is .' + keyword_returned + '.'
          print 'i is ' + str(i)
          print 'query_array was ' 
          print query_array
          sys.stdout.flush()
          break 

        spreadsheet_row['locale'] = locale
        spreadsheet_row['keyword'] = query_array[i]
        spreadsheet_row['keyword_text'] = keyword_returned
        try:
          spreadsheet_row['search_volume'] = str(attributes.get('SEARCH_VOLUME')['value'])
        except AttributeError:
          spreadsheet_row['search_volume'] = '0'
        try:  
          spreadsheet_row['competition'] = str(attributes.get('COMPETITION')['value'])[:4]
        except AttributeError:
          spreadsheet_row['competition'] = '0.00'  
        try:
          spreadsheet_row['average_cpc'] =  str(float(attributes.get('AVERAGE_CPC')['value']['microAmount']/float(1000000)))[:4]
        except AttributeError: 
          spreadsheet_row['average_cpc'] = '0.00'  
        writer.writerow(spreadsheet_row)
        i = i+1
    else:
      print 'No related keywords were found.'

    offset += PAGE_SIZE
    selector['paging']['startIndex'] = str(offset)
    more_pages = offset < int(page['totalNumEntries'])

  #did we get to end of results before end of input array? 
  while (i < len(query_array)):
    # print an empty row
    spreadsheet_row = dict()
    spreadsheet_row['locale'] = locale
    spreadsheet_row['keyword'] = query_array[i]
    spreadsheet_row['keyword_text'] = ''
    spreadsheet_row['search_volume'] = '0'
    spreadsheet_row['competition'] = '0.00'
    spreadsheet_row['average_cpc'] = '0.00'
    writer.writerow(spreadsheet_row)
    i = i+1

parser = argparse.ArgumentParser(description='Get a list of keywords from a file, then call GoogleAdWords api to get stats about those keywords')
parser.add_argument('-i', '--input-file', help='location of input file')
parser.add_argument('-o', '--output-file', help='location of output file')
parser.add_argument('-l', '--locale', help='locale of keywords')
args = parser.parse_args()

input_file = args.input_file
output_file = args.output_file
locale = args.locale

# Initialize the service.
print 'Connecting to Google AdWords ' + locale
adwords_client = adwords.AdWordsClient.LoadFromStorage('./googleads.yaml')
targeting_idea_service = adwords_client.GetService('TargetingIdeaService', version='v201402')

# read list of query strings from file, in chunks
with codecs.open(input_file, 'r', 'utf-8') as in_f:
  print 'opened ' + input_file + ' for reading'
  with open(output_file, 'w') as out_f:
    print 'opened ' + output_file + ' for writing'
    sys.stdout.flush()
    output_fields = ['locale', 'keyword', 'keyword_text', 'search_volume', 'average_cpc', 'competition']
    writer = csv.DictWriter(out_f, output_fields, delimiter = '\t')
    #writer.writeheader() 
    
    query_array = []   
    processed = 0
    for line in in_f:
      #print 'line ' + line + ', size : ' + str(len(query_array))
      clean_line = unicodedata.normalize('NFKD', line.rstrip('\n')).encode('ascii','ignore').strip()
      if (clean_line not in query_array) and (len(clean_line.strip()) > 0):
        query_array.append(clean_line)
      
      if len(query_array) == 500: 
        # send this over to Google API 
        tries = 0
        while (tries < 5):
          try:
            query_google_api(query_array, writer)
            # and wait a little so we don't send too many requests too quickly
            #print 'finished query_google_api'
            time.sleep(5)
            tries = 5
          except:
            tries = tries + 1
            if (tries == 5):
              raise
            else:
              time.sleep(30)
       
        processed = processed + len(query_array)
        if processed%1000 == 0:
          print 'processed ' + str(processed) + ' queries'
          sys.stdout.flush()
        # empty the query_array 
        query_array = []
  

    if len(query_array) != 0:
      query_google_api(query_array, writer) 
      processed = processed + len(query_array) 
      print 'processed ' + str(processed) + ' queries'  

    print 'Done'    




