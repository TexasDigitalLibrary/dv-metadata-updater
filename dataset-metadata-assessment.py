import csv
import json
import os
import pandas as pd
import requests
import time
from datetime import datetime
from utils import extract_max_version, is_valid_orcid, is_valid_ror, retrieve_all_institutions

# ============================================
#               WORKFLOW SET-UP
# ============================================

# Config file
with open('config.json', 'r') as file:
    config = json.load(file)

# Toggles
## Test environment (incomplete run, faster to complete)
test = config['TOGGLES']['test_environment']
## Restrict to your/one institution in TDR (True to restrict to your institution)
only_my_institution = config['TOGGLES']['only_my_institution'] 
## Split output files by institution (IN DEVELOPMENT)
split_institution_output = config['TOGGLES']['split_institution_output']
## Whether ROR external vocab plug-in is working
ror_plugin = config['TOGGLES']['ror_plugin_enabled']

# Timestamp to calculate run time
start_time = datetime.now() 
# Current date for filenames
today = datetime.now().strftime('%Y%m%d') 

# Dynamic variables for which attributes to re-curate
recurate_funding = config['RECURATION']['funding']
recurate_keywords = config['RECURATION']['keywords']
recurate_licenses = config['RECURATION']['licenses']
recurate_names = config['RECURATION']['names']
recurate_orcid = config['RECURATION']['orcid']
recurate_punctuation = config['RECURATION']['punctuation']
recurate_ror = config['RECURATION']['ror']
recurate_works = config['RECURATION']['works']

# Filename version of your institution's name
my_institution_filename = config['INSTITUTION']['filename']
# Condition based on 'only_my_institution' toggle
if only_my_institution:
    institution_filename = my_institution_filename
else:
    institution_filename = 'all-institutions'
# Short-hand version of your institution's name
my_institution_short_name = config['INSTITUTION']['myInstitution']

print(f'String to add to filenames: {my_institution_filename}.\n')
print(f'Short hand version of institution name: {my_institution_short_name}.\n')

# Script directory
script_dir = os.getcwd()
print(f'The script directory is {script_dir}.\n')

# Create directories
if test:
    if os.path.isdir('test'):
        print('test directory found - no need to recreate\n')
    else:
        os.mkdir('test')
        print('test directory has been created\n')
    test_dir = os.path.join(script_dir, 'test')
    os.chdir('test')
    if os.path.isdir('outputs'):
        print('test outputs directory found - no need to recreate\n')
    else:
        os.mkdir('outputs')
        print('test outputs directory has been created\n')
    outputs_dir = os.path.join(test_dir, 'outputs')
    if os.path.isdir('logs'):
        print('test logs directory found - no need to recreate\n')
    else:
        os.mkdir('logs')
        print('test logs directory has been created\n')
    logs_dir = os.path.join(test_dir, 'logs')
else:
    if os.path.isdir('outputs'):
        print('outputs directory found - no need to recreate\n')
    else:
        os.mkdir('outputs')
        print('outputs directory has been created\n')
    outputs_dir = os.path.join(script_dir, 'outputs')
    if os.path.isdir('logs'):
        print('logs directory found - no need to recreate\n')
    else:
        os.mkdir('logs')
        print('logs directory has been created\n')
    logs_dir = os.path.join(script_dir, 'logs')

# Load existing ROR mapping for affiliation re-curation
## Only purpose of loading in this script is to add new affiliations
## Workflow will proceed if this file does not exist
file_path = f'{script_dir}/affiliation-map-primary.csv'
if os.path.exists(file_path):
    master_ror_matching = pd.read_csv(file_path)
    print(f'"{file_path}" exists and has been loaded into a dataFrame.')
else:
    master_ror_matching = None
    print(f'"{file_path}" does not exist. No file loaded.')

# ============================================
#           API PARAMETERS: Datasets
# ============================================

print('Beginning to define API call parameters.\n')

url_tdr = 'https://dataverse.tdl.org/api/search/'
# Filter for only published datasets
status = 'publicationStatus:Published'

if test and only_my_institution:
    page_limit_dataset = config['VARIABLES']['PAGE_LIMITS']['dataverse_test'] 
elif test and not only_my_institution: 
    page_limit_dataset = config['VARIABLES']['PAGE_LIMITS']['dataverse_test'] // 2 #halve page size if retrieving all institutions
elif not test:
    page_limit_dataset = config['VARIABLES']['PAGE_LIMITS']['dataverse_prod']
page_size_dataset = config['VARIABLES']['PAGE_SIZES']['dataverse_test'] if test else config['VARIABLES']['PAGE_SIZES']['dataverse_prod']

print(f'Retrieving {page_size_dataset} records per page over {page_limit_dataset} pages.\n')

query = '*'
page_start_dataset = config['VARIABLES']['PAGE_STARTS']['dataverse']
page_increment_dataset = config['VARIABLES']['PAGE_INCREMENTS']['dataverse']

headers_tdr = {
    'X-Dataverse-key': config['KEYS']['dataverse_token']
}

params_tdr_ut_austin = {
    'q': query,
    'fq': status,
    'subtree': 'utexas',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_baylor = {
    'q': query,
    'fq': status,
    'subtree': 'baylor',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_smu = {
    'q': query,
    'fq': status,
    'subtree': 'smu',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_tamu = {
    'q': query,
    'fq': status,
    'subtree': 'tamu',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_txst = {
    'q': query,
    'fq': status,
    'subtree': 'txst',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_ttu = {
    'q': query,
    'fq': status,
    'subtree': 'ttu',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_houston = {
    'q': query,
    'fq': status,
    'subtree': 'uh',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_hscfw = {
    'q': query,
    'fq': status,
    'subtree': 'unthsc',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_tamug = {
    'q': query,
    'fq': status,
    'subtree': 'tamug',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_tamui = {
    'q': query,
    'fq': status,
    'subtree': 'tamiu',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_utsah = {
    'q': query,
    'fq': status,
    'subtree': 'uthscsa',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_utswm = {
    'q': query,
    'fq': status,
    'subtree': 'utswmed',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_uta = {
    'q': query,
    'fq': status,
    'subtree': 'uta',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_twu = {
    'q': query,
    'fq': status,
    'subtree': 'twu',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}

all_params_datasets = {
        'UT Austin': params_tdr_ut_austin,
        'Baylor': params_tdr_baylor,
        'SMU': params_tdr_smu,
        'TAMU': params_tdr_tamu,
        'Texas State': params_tdr_txst,
        'Texas Tech': params_tdr_ttu,
        'Houston': params_tdr_houston,
        'HSC Fort Worth': params_tdr_hscfw,
        'TAMU Galveston': params_tdr_tamug,
        'TAMU International': params_tdr_tamui,
        'UT San Antonio Health': params_tdr_utsah,
        'UT Southwestern Medical': params_tdr_utswm,
        'UT Arlington': params_tdr_uta,
        "Texas Women's University": params_tdr_twu
    }

tamu_combined_params = {
        'TAMU': params_tdr_tamu,
        'TAMU Galveston': params_tdr_tamug,
        'TAMU International': params_tdr_tamui
}

#substitute for your institution
if only_my_institution:
    if my_institution_short_name == 'TAMU':
        params_list = tamu_combined_params
    else:
        params_list = {
            my_institution_short_name: all_params_datasets[my_institution_short_name]
        }
else:
    params_list = all_params_datasets

# ============================================
#          SEARCH API RETRIEVAL: Datasets
# ============================================

print('Starting TDR retrieval.\n')
data_tdr_search = retrieve_all_institutions(url_tdr, params_list, headers_tdr, page_start_dataset, page_size_dataset, page_limit_dataset)

print('Starting TDR filtering.\n')
data_tdr_search_select = []
for item in data_tdr_search:
    id = item.get('global_id', None)
    type = item.get('type', None)
    institution = item.get('institution',None)
    status = item.get('versionState', None)
    description = item.get('description', None)
    keywords = item.get('keywords', None)
    name = item.get('name', None)
    dataverse = item.get('name_of_dataverse', None)
    dataverse_code = item.get('identifier_of_dataverse', None)
    majorV = item.get('majorVersion', 0)
    minorV = item.get('minorVersion', 0)
    comboV = f'{majorV}.{minorV}'
    version_id = item.get('versionId', None)
    data_tdr_search_select.append({
        'institution': institution, 
        'doi': id,
        'description': description,
        'keywords': keywords,
        'dataset_title': name,
        'dataverse': dataverse,
        'dataverse_code': dataverse_code,
        'major_version': majorV,
        'minor_version': minorV,
        'total_version': comboV,
        'version_id': version_id
    })

df_data_tdr_search_select = pd.DataFrame(data_tdr_search_select)

# Ensuring full version (float)
df_data_tdr_search_select['total_version'] = df_data_tdr_search_select['total_version'].apply(extract_max_version)
# Editing DOI field
df_data_tdr_search_select['doi'] = df_data_tdr_search_select['doi'].str.replace('doi:', '')

df_data_tdr_search_select.to_csv(f'outputs/{today}_{institution_filename}_all-datasets-PUBLISHED.csv', index=False, encoding='utf-8-sig')

# ============================================
#           API PARAMETERS: Dataverses
# ============================================

page_limit_dataverse = config['VARIABLES']['PAGE_LIMITS']['dataverse_test'] if test else config['VARIABLES']['PAGE_LIMITS']['dataverse_prod']
page_size_dataverse = config['VARIABLES']['PAGE_SIZES']['dataverse_test'] if test else config['VARIABLES']['PAGE_SIZES']['dataverse_prod']
print(f'Retrieving {page_size_dataverse} dataverses per page over {page_limit_dataverse} pages.\n')

query = '*'
page_start_dataverse = config['VARIABLES']['PAGE_STARTS']['dataverse']
page_increment_dataverse = config['VARIABLES']['PAGE_INCREMENTS']['dataverse']

params_tdr_ut_austin = {
    'q': query,
    'subtree': 'utexas',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_baylor = {
    'q': query,
    'subtree': 'baylor',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_smu = {
    'q': query,
    'subtree': 'smu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_tamu = {
    'q': query,
    'subtree': 'tamu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_txst = {
    'q': query,
    'subtree': 'txst',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_ttu = {
    'q': query,
    'subtree': 'ttu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_houston = {
    'q': query,
    'subtree': 'uh',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_hscfw = {
    'q': query,
    'subtree': 'unthsc',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_tamug = {
    'q': query,
    'subtree': 'tamug',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}
params_tdr_tamui = {
    'q': query,
    'subtree': 'tamiu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}
params_tdr_utsah = {
    'q': query,
    'subtree': 'uthscsa',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}
params_tdr_utswm = {
    'q': query,
    'subtree': 'utswmed',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_uta = {
    'q': query,
    'subtree': 'uta',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_twu = {
    'q': query,
    'subtree': 'twu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

all_params_dataverses = {
        'UT Austin': params_tdr_ut_austin,
        'Baylor': params_tdr_baylor,
        'SMU': params_tdr_smu,
        'TAMU': params_tdr_tamu,
        'Texas State': params_tdr_txst,
        'Texas Tech': params_tdr_ttu,
        'Houston': params_tdr_houston,
        'HSC Fort Worth': params_tdr_hscfw,
        'TAMU Galveston': params_tdr_tamug,
        'TAMU International': params_tdr_tamui,
        'UT San Antonio Health': params_tdr_utsah,
        'UT Southwestern Medical': params_tdr_utswm,
        'UT Arlington': params_tdr_uta,
        "Texas Women's University": params_tdr_twu
    }

tamu_combined_params = {
        'TAMU': params_tdr_tamu,
        'TAMU Galveston': params_tdr_tamug,
        'TAMU International': params_tdr_tamui
}

if only_my_institution:
    if my_institution_short_name == 'TAMU':
        params_list = tamu_combined_params
    else:
        params_list = {
            my_institution_short_name: all_params_dataverses[my_institution_short_name]
        }
else:
    params_list = all_params_dataverses

# ============================================
#        SEARCH API RETRIEVAL: Dataverses
# ============================================

print('Starting TDR retrieval.\n')
dataverse_tdr_search = retrieve_all_institutions(url_tdr, params_list, headers_tdr, page_start_dataverse, page_size_dataverse, page_limit_dataverse)

print('Starting TDR filtering.\n')
dataverses_select_tdr = []
for item in dataverse_tdr_search:
    name = item.get('name', '')
    identifier = item.get('identifier', '')
    parent_dataverse_name = item.get('parentDataverseName', '')
    parent_dataverse_id = item.get('parentDataverseIdentifier', '')
    dataverses_select_tdr.append({
        'dataverse_code': identifier,
        'parent_dataverse': parent_dataverse_name,
        'parent_code': parent_dataverse_id
    })

df_dataverses_select_tdr = pd.DataFrame(dataverses_select_tdr)
df_datasets_dataverses = pd.merge(df_data_tdr_search_select, df_dataverses_select_tdr, left_on='dataverse_code', right_on='dataverse_code', how='left')

# ============================================
#         NATIVE API RETRIEVAL: Datasets
# ============================================

# Retrieving additional metadata for deposits
## If a previously published dataset is currently in DRAFT state, it will return the information for the DRAFT (most current) state
print('Starting Native API call\n')
url_tdr_native = 'https://dataverse.tdl.org/api/datasets/'

print(f'Total datasets to be analyzed: {len(df_datasets_dataverses)}.\n')

results = []
# Will try three passes on a DOI after timeout error before moving on
first_timeouts = []
second_timeouts = []
final_timeouts = []
for doi in df_datasets_dataverses['doi']:
    try:
        response = requests.get(f'{url_tdr_native}:persistentId/?persistentId=doi:{doi}', headers=headers_tdr, timeout=5)
        if response.status_code == 200:
            print(f'Retrieving {doi}\n')
            results.append(response.json())
        else:
            final_timeouts.append({"doi": doi, "reason": f"Status {response.status_code}"})
    except requests.exceptions.Timeout:
        first_timeouts.append(doi)
    except requests.exceptions.RequestException as e:
        final_timeouts.append({"doi": doi, "reason": str(e)})

if first_timeouts:
    print(f"\n--- Retrying {len(first_timeouts)} timeouts with 10s limit ---\n")
    time.sleep(2) 
    for doi in first_timeouts:
        try:
            response = requests.get(f'{url_tdr_native}:persistentId/?persistentId=doi:{doi}', headers=headers_tdr, timeout=5)
            if response.status_code == 200:
                print(f'Retrying {doi}\n')
                results.append(response.json())
            else:
                final_timeouts.append({"doi": doi, "reason": f"Status {response.status_code}"})
        except requests.exceptions.Timeout:
            second_timeouts.append(doi)
        except requests.exceptions.RequestException as e:
            second_timeouts.append({"doi": doi, "reason": str(e)})

if second_timeouts:
    print(f"\n--- Retrying {len(first_timeouts)} repeat timeouts with 10s limit ---\n")
    time.sleep(2) 
    for doi in first_timeouts:
        try:
            response = requests.get(f'{url_tdr_native}:persistentId/?persistentId=doi:{doi}', headers=headers_tdr, timeout=10)
            if response.status_code == 200:
                print(f'Retrying {doi} again\n')
                results.append(response.json())
            else:
                final_timeouts.append({"doi": doi, "reason": f"Retry Status {response.status_code}"})
        except Exception as e:
            final_timeouts.append({"doi": doi, "reason": "Persistent Timeout/Error"})

## Saving failed retrievals
with open(f'{logs_dir}/{today}_failed-retrievals.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['DOI', 'Error Message'])
    writer.writerows(final_timeouts)

data_tdr_native = {'datasets': results}

print(f"Number of datasets that initially timed out: {len(first_timeouts)}\n")
print(f"Number of datasets that subsequently timed out: {len(second_timeouts)}\n")
print(f"TNumber of datasets that perpetually timed out: {len(final_timeouts)}\n")

print('Beginning dataframe subsetting\n')
data_tdr_native_select = [] 
for item in data_tdr_native['datasets']:
    # Dataset level
    data = item.get('data', None)
    dataset_id = data.get('id', None)
    pubDate = data.get('publicationDate', None)
    latest = data.get('latestVersion', {})
    status = latest.get('versionState', None)
    status2 = latest.get('latestVersionPublishingState', None)
    doi = latest.get('datasetPersistentId', None)
    createDate = latest.get('createTime', None)
    releaseDate = latest.get('releaseTime', None)
    license = latest.get('license', {})
    licenseName = license.get('name', None)
    terms = latest.get('termsOfUse', None)
    usage = licenseName if licenseName is not None else terms
    citation = latest.get('metadataBlocks', {}).get('citation', {})
    fields = citation.get('fields', [])
    keywords = None
    keywords_vocab = None
    funding = None
    depositor = 'None listed'
    contacts = 'None listed'
    contact_emails = 'None listed'
    grant_agencies = None
    grant_numbers = None
    citations = None
    relations = None
    dois = None
    urls = None
    for field in fields:
        if field['typeName'] == 'subject':
            subjects = field.get('value', [])
        if field['typeName'] == 'notesText':
            notes = field.get('value', '')
        if field['typeName'] == 'keyword':
            keywords = []
            keywords_vocab=[]
            for keyword_dict in field.get('value', []):
                keyword_value = keyword_dict.get('keywordValue', {}).get('value', None)
                keyword_vocab = keyword_dict.get('keywordVocabulary', {}).get('value', None)
                if keyword_value:
                    keywords.append(keyword_value)
                if keyword_vocab:
                    keywords_vocab.append(keyword_vocab)
            keywords_str = '; '.join(keywords)
        if field['typeName'] == 'datasetContact':
            contacts = []
            contact_emails = []
            for contact in field.get('value', []):
                contact_value = contact.get('datasetContactName', {}).get('value', None)
                contact_email_value = contact.get('datasetContactEmail', {}).get('value', None)
                if contact_value:
                    contacts.append(contact_value)
                if contact_email_value:
                    contact_emails.append(contact_email_value)
            contacts = '; '.join(contacts) 
            contact_emails = '; '.join(contact_emails)
        if field['typeName'] == 'depositor':
            depositor = field.get('value', None)
        if field['typeName'] == 'grantNumber':
            grant_agencies = []
            grant_numbers = []
            for grant in field.get('value', []):
                agency = grant.get('grantNumberAgency', {}).get('value', None)
                number = grant.get('grantNumberValue', {}).get('value', None)
                if agency:
                    grant_agencies.append(agency)
                if number:
                    grant_numbers.append(number)
            grant_agencies = '; '.join(grant_agencies)
            grant_numbers = '; '.join(grant_numbers) 
        if field['typeName'] == 'publication':
            citations = []
            relations = []
            dois = []
            urls = []
            for pub in field.get('value', []):
                citation = pub.get('publicationCitation', {}).get('value', None)
                relation_value = pub.get('publicationRelationType', {}).get('value', None)   
                doi_value = pub.get('publicationIDNumber', {}).get('value', None)  
                url_value = pub.get('publicationURL', {}).get('value', None)
                if citation:
                    citations.append(citation)
                if relation_value:
                    relations.append(relation_value)
                if doi_value:
                    dois.append(doi_value)
                if url_value:
                    urls.append(url_value) 
            citations = '; '.join(citations)
            relations = '; '.join(relations) 
            dois = '; '.join(dois)  
            urls = '; '.join(urls)  
    # Author level
    num_authors = 0
    num_valid_orcid = 0
    num_valid_ror = 0
    for field in fields:
        if field['typeName'] == 'author':
            for position, author in enumerate(field.get('value', []), start=1):
                num_authors += 1

                name = author.get('authorName', {}).get('value', None)
                affiliation = author.get('authorAffiliation', {}).get('value', None)
                identifier = author.get('authorIdentifier', {}).get('value', None)
                scheme = author.get('authorIdentifierScheme', {}).get('value', None)
                affiliation_expanded = author.get('authorAffiliation', {}).get('expandedvalue', {}).get('termName', None)
                identifier_expanded = author.get('authorIdentifier', {}).get('expandedvalue', {}).get('@id', None)

                affiliationName = affiliation_expanded if affiliation_expanded else affiliation
                affiliation_ror = affiliation if affiliation_expanded else None

                if is_valid_orcid(identifier):
                    num_valid_orcid += 1
                if is_valid_ror(affiliation):
                    num_valid_ror += 1
    base_entry = {
        'dataset_id': dataset_id,
        'doi': doi,
        'publication_date': pubDate,
        "flag_orcid": (num_authors - num_valid_orcid) > 0,
        "flag_ror": (num_authors - num_valid_ror) > 0,
        'dataset_contact': contacts,
        'dataset_email': contact_emails,
        'dataset_depositor': depositor,
        'status': status,
        'current_status': status2,
        'reuse_requirements': usage,
        'license': licenseName,
        'keywords_vocab': keywords_vocab,
        'grant_agencies': grant_agencies,
        'grant_numbers': grant_numbers,
        'related_works_citations': citations,
        'related_works_dois': dois,
        'related_works_urls': urls,
        'related_works_types': relations
    }
    data_tdr_native_select.append(base_entry)

# Dataframe with entries for individual authors
author_entries = []
for item in data_tdr_native['datasets']:
    data = item.get('data', {})
    latest = data.get('latestVersion', {})
    doi = latest.get('datasetPersistentId', None)
    citation = latest.get('metadataBlocks', {}).get('citation', {})
    status2 = latest.get('latestVersionPublishingState', None)
    fields = citation.get('fields', [])
    for field in fields:
        if field['typeName'] == 'author':
            num_authors = len(field.get('value', []))
            for position, author in enumerate(field.get('value', []), start=1):
                name = author.get('authorName', {}).get('value', None)
                affiliation = author.get('authorAffiliation', {}).get('value', None)
                identifier = author.get('authorIdentifier', {}).get('value', None)
                scheme = author.get('authorIdentifierScheme', {}).get('value', None)
                affiliation_expanded = author.get('authorAffiliation', {}).get('expandedvalue', {}).get('termName', None)
                identifier_expanded = author.get('authorIdentifier', {}).get('expandedvalue', {}).get('@id', None)

                affiliationName = affiliation_expanded if affiliation_expanded else affiliation
                affiliation_ror = affiliation if affiliation_expanded else None

                author_entry = {
                    'doi': doi,
                    'current_status': status2,
                    'author_name': name,
                    'author_affiliation': affiliationName,
                    'ror_id': affiliation_ror,
                    'author_identifier': identifier,
                    'author_identifier_expanded': identifier_expanded,
                    'author_identifier_scheme': scheme,
                    'author_count': num_authors,
                    'author_position': position
                }
                author_entries.append(author_entry)

df_data_tdr_native_select = pd.json_normalize(data_tdr_native_select)
df_data_tdr_native_select['doi'] = df_data_tdr_native_select['doi'].str.replace('doi:', '')

df_select_concatenated = pd.merge(df_datasets_dataverses, df_data_tdr_native_select, on='doi', how='left')

# ============================================
#           DATASET-LEVEL FLAGGING
# ============================================

# ============================================
# Leading or trailing whitespace, terminal '.'
# ============================================
if recurate_punctuation:
    # If title ends in period
    ## Exempts flag if there are certain words involving periods at the end
    exempt_words = ['U.S.', 'U.S.A.', ' al.']
    df_select_concatenated['flag_title_period'] = (df_select_concatenated['dataset_title'].str.endswith('.') & ~df_select_concatenated['dataset_title'].str.endswith('|'.join(exempt_words)))
    
    # If extra space in front or behind title
    df_select_concatenated['flag_title_space'] = (df_select_concatenated['dataset_title'].str.endswith(' ') | df_select_concatenated['dataset_title'].str.startswith(' '))

# ============================================
# Non-CC0 license
# ============================================
if recurate_licenses:
    # If license is not CC0
    df_select_concatenated['flag_funding'] = df_select_concatenated['license'] != 'CC0 1.0'

# ============================================
# Keyword malformatting
# ============================================
if recurate_keywords:
    # If there are commas or semi-colons within one quote-bracketed string (e.g., 'fish, dog, cat') AND the keywords are not linked to a schema
    df_select_concatenated['flag_keyword'] = df_select_concatenated.apply(
    lambda row: (any(',' in kw or ';' in kw for kw in row['keywords']) if isinstance(row['keywords'], list) else False) 
    and row['keywords_vocab'] is None,
    axis=1
    )
    ## Example code (not deployed) if you want to use a list of excluded names to return False for a flag that it would otherwise return True for
    # df_select_concatenated['flag_keyword'] = df_select_concatenated.apply(lambda row: 
    #         (False if (row['dataset_depositor'] in excluded_people or row['dataset_contact'] in excluded_people) 
    #         else (any(',' in kw or ';' in kw for kw in row['keywords']) if isinstance(row['keywords'], list) 
    #         else False)),axis=1)
# ============================================
# Funding presence/format
# ============================================
if recurate_funding:
    df_select_concatenated['flag_funding'] = df_select_concatenated['grant_agencies'].isna()

# ============================================
# Related works presence/format
# ============================================
if recurate_works:
    # If a DOI is not listed but certain words in other fields suggest there is a related work
    ## Define flagging words
    related_work_title_flags = ['article', 'paper', 'preprint', 'pre-print', 'manuscript', 'publication', 'et al', 'supplemental materials', 'supplementary materials', 'supplemental data', 'replication data for', 'replication data and code', 'replication data and materials', 'data for', 'scripts for', 'analysis code for', 'analysis scripts for', 'data archive for', 'data file for', 'data published in', 'dataset for']
    related_work_description_flags = ['article', 'paper', 'preprint', 'pre-print', 'manuscript', 'publication', 'et al', 'supplemental materials', 'supplementary materials', 'supplemental data', 'replication data for', 'replication data and code', 'replication data and materials', 'analysis code for', 'analysis scripts for', 'data archive for', 'data file for', 'data published in', 'dataset for']
    pattern_title = '|'.join(related_work_title_flags)
    pattern_description = '|'.join(related_work_description_flags)
    df_select_concatenated['flag_work_missing'] = (df_select_concatenated['related_works_dois'].isna() | 
     (df_select_concatenated['related_works_dois'].str.strip() == '')) & ((
            df_select_concatenated['dataset_title'].str.contains(pattern_title, case=False) |
            df_select_concatenated['description'].str.contains(pattern_description, case=False)
    )|df_select_concatenated['related_works_citations'].notna())

    # If a DOI is listed but the full URL is not (or vice versa)
    ## Not splitting these as flags for now since the impact of having only one isn't clear
    df_select_concatenated['flag_work_url'] = (df_select_concatenated['related_works_dois'].notna()) & (df_select_concatenated['related_works_urls'].isna() | 
     (df_select_concatenated['related_works_urls'].str.strip() == '')) | (df_select_concatenated['related_works_urls'].notna()) & (df_select_concatenated['related_works_dois'].isna() | (df_select_concatenated['related_works_dois'].str.strip() == ''))

# Composite flag column
## Dynamic list of columns will handle any combination of enabled flagging
flags_cols = [col for col in df_select_concatenated.columns if col.startswith('flag')]
if flags_cols:
    df_select_concatenated['flagged_any'] = df_select_concatenated[flags_cols].any(axis=1)
    df_select_concatenated['flags'] = df_select_concatenated[flags_cols].sum(axis=1)

base_cols = ['institution', 'dataset_id', 'doi', 'publication_date', 'version_id', 'total_version','current_status', 'dataverse', 'parent_dataverse', 'dataset_title', 'description', 'keywords', 'keywords_vocab', 'grant_agencies', 'grant_numbers', 'dataset_depositor', 'dataset_contact', 'dataset_email', 'license', 'related_works_citations', 'related_works_dois', 'related_works_urls']

# Resetting list of columns to include 'flagged_any' and 'flags'
flags_cols = [col for col in df_select_concatenated.columns if col.startswith('flag')]
df_select_concatenated_pruned = df_select_concatenated[base_cols + flags_cols]
df_select_concatenated_pruned.to_csv(f'outputs/{today}_{institution_filename}_all-datasets-PUBLISHED_flagged.csv', index=False, encoding='utf-8-sig')

if split_institution_output and not only_my_institution:
    column = 'institution'
    output_dir = 'by-institution'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    for unique_value, df in df_select_concatenated_pruned.groupby(column):
        filename = f"{output_dir}/{unique_value.replace(' ', '_')}all-datasets-PUBLISHED_flagged.csv"
        df.to_csv(filename, index=False)
        print(f"Saved {filename}")

# ============================================
#           AUTHOR-LEVEL FLAGGING
# ============================================
df_author_entries = pd.json_normalize(author_entries)
df_author_entries['doi'] = df_author_entries['doi'].str.replace('doi:', '')
## Ensure that leading zeros in non-hyphenated ORCID are preserved
df_author_entries['author_identifier'] = df_author_entries['author_identifier'].astype(str)

# ============================================
# ROR presence
# ============================================
if recurate_ror:
    ### Is ROR present
    if ror_plugin:
        df_author_entries = df_author_entries.copy()
        df_author_entries['missing_ror'] = (df_author_entries['ror_id'].isna() | (df_author_entries['ror_id'] == ''))
    else:
        df_author_entries['missing_ror'] = ~df_author_entries['author_affiliation'].str.contains('https://ror.org/', na=False)
    
    ### Is ROR present and appropriately formatted
    # df_author_entries.loc[:, 'proper_ror'] = df_author_entries['ror_id'].str.contains('https://ror.org/', na=False)
    flags_cols = [col for col in df_author_entries.columns if col.startswith('missing')]
    if flags_cols:
        df_author_entries['flag_ror'] = df_author_entries[flags_cols].any(axis=1)

# ============================================
# ORCID presence & format
# ============================================
if recurate_orcid:
    ### ORCID missing
    df_author_entries['missing_orcid'] = df_author_entries['author_identifier'].isna()
    
    ### Is any author ID system present
    #### Sometimes there are ORCID-looking numerical identifiers that do not have ORCID in the scheme (this logic avoids picking up other identifiers with recorded schemes)
    df_author_entries['missing_author_scheme'] = ((df_author_entries['author_identifier_scheme'].isna()) & df_author_entries['author_identifier'].notna())
    
    ### ORCID present but malformatted (no dashes)
    df_author_entries['malformed_orcid_no_hyphens'] = (df_author_entries['author_identifier_scheme'].str.upper() == 'ORCID'
        ) & ~df_author_entries['author_identifier'].str.contains('-', na=False
        ) & (df_author_entries['author_identifier'].notna())
    
    ### ORCID present but malformatted (not hyperlinked)
    df_author_entries['malformed_orcid_no_url'] = (df_author_entries['author_identifier_scheme'].str.upper() == 'ORCID'
        ) & ~df_author_entries['author_identifier'].str.contains('https://orcid.org/00', na=False
        ) & (df_author_entries['author_identifier'].notna()
        ) & df_author_entries['author_identifier'].str.contains('-', na=False)
    
    ### ORCID present but malformatted (space between shoulder and identifier)
    df_author_entries['malformed_orcid_space'] = (df_author_entries['author_identifier_scheme'].str.upper() == 'ORCID'
        ) & df_author_entries['author_identifier'].str.contains('https://orcid.org/ 00', na=False)
    
    ### ORCID present but malformatted (no expanded field value)
    df_author_entries['malformed_orcid_single_field'] = (df_author_entries['author_identifier_scheme'].isna() | 
        df_author_entries['author_identifier_expanded'].isna()
        ) & df_author_entries['author_identifier'].str.contains('https://orcid.org/00', na=False)
    
    ### ORCID present but without https:// protocol
    df_author_entries['malformed_orcid_single_field'] = (df_author_entries['author_identifier_scheme'].isna() | 
        df_author_entries['author_identifier_expanded'].isna()
        ) & df_author_entries['author_identifier'].str.contains('orcid.org/00', na=False) & ~df_author_entries['author_identifier'].str.contains('http', na=False)
    
    flags_cols = [col for col in df_author_entries.columns if col.startswith(('malformed_orcid', 'missing'))]
    if flags_cols:
        df_author_entries['flag_orcid'] = df_author_entries[flags_cols].any(axis=1)

# ============================================
# Author name formatting
# ============================================
if recurate_names:
    ### Malformed author name (order)
    df_author_entries['malformed_name_order'] = (
        df_author_entries['author_name'].str.contains(' ', na=False) & 
        ~df_author_entries['author_name'].str.contains(',', na=False)
    )
    ### Malformed initial (standalone initial without period)
    df_author_entries['malformed_name_initial'] = df_author_entries['author_name'].str.contains(r'(?:^|\s)(?<!\')[A-Z](?:\s|,|$)(?!\.)', regex=True)
    
    ### Edge cases
    #### Semi-colon instead of comma for name divider and ALL CAPS
    df_author_entries['malformed_name_other'] = (
        df_author_entries['author_name'].str.contains('; ', na=False) |
        df_author_entries['author_name'].str.isupper()
    )

    flags_cols = [col for col in df_author_entries.columns if col.startswith('malformed_name')]
    if flags_cols:
        df_author_entries['flag_name'] = df_author_entries[flags_cols].any(axis=1)
    df_author_entries['malformed_name_case'] = (
    (df_author_entries['author_name'].str.islower() | df_author_entries['author_name'].str.isupper()) &
    (df_author_entries['author_name'].str.split().str.len() > 1)
    )

## Summarizing author-related flags
flags_cols = [col for col in df_author_entries.columns if col.startswith('flag')]
if flags_cols:
    df_author_entries['flagged_any'] = df_author_entries[flags_cols].any(axis=1)
    df_author_entries['flags'] = df_author_entries[flags_cols].sum(axis=1)

df_author_entries.to_csv(f'outputs/{today}_{institution_filename}_all-authors-PUBLISHED.csv', index=False, encoding='utf-8-sig')

# Create expanded authors df with contact/depositor info for filtering
df_dataset_select = df_select_concatenated_pruned[['institution', 'dataset_id', 'doi', 'publication_date', 'version_id', 'total_version','current_status', 'dataverse', 'parent_dataverse', 'dataset_title', 'description', 'keywords', 'keywords_vocab', 'grant_agencies', 'grant_numbers', 'dataset_depositor', 'dataset_contact', 'dataset_email', 'license', 'related_works_citations', 'related_works_dois', 'related_works_urls']]
df_author_entries_expanded = pd.merge(df_author_entries, df_dataset_select, on='doi', how='left')
df_author_entries_expanded.to_csv(f'outputs/{today}_{institution_filename}_all-authors-datasets-PUBLISHED.csv', index=False, encoding='utf-8-sig')

## Aggregating over select Boolean columns to merge back into dataset-level df
boolean_cols = ['flag_ror', 'flag_orcid', 'flag_name']

agg_dict = {}
for col in boolean_cols:
    agg_dict[f'{col}_any'] = (col, 'any')                                                                       # Whether any authors were flagged
    agg_dict[f'count_{col}'] = (col, 'sum')                                                                     # Count of flagged authors
    agg_dict[f'authors_{col}'] = (col, lambda x: df_author_entries.loc[x[x].index, 'author_name'].tolist())     # List of authors who were flagged

# Group by DOI and aggregate
df_authors_aggregated = df_author_entries.groupby('doi').agg(**agg_dict).reset_index()

df_combined = pd.merge(df_select_concatenated_pruned, df_authors_aggregated, on='doi', how='left')

# Create pruned output of datasets with some author information added
base_cols = ['institution', 'dataset_id', 'doi', 'publication_date', 'version_id', 'total_version','current_status', 'dataverse', 'parent_dataverse', 'dataset_title', 'description', 'keywords', 'keywords_vocab', 'grant_agencies', 'grant_numbers', 'dataset_depositor', 'dataset_contact', 'dataset_email', 'license', 'related_works_citations', 'related_works_dois', 'related_works_urls']

# Resetting list of columns to include 'flagged_any' and 'flags'
flags_cols = [col for col in df_combined.columns if col.startswith('flag_')]
counts_cols = [col for col in df_combined.columns if col.startswith('count_')]
authors_cols = [col for col in df_combined.columns if col.startswith('authors_')]
df_combined_pruned = df_combined[base_cols + flags_cols + counts_cols + authors_cols]
df_combined_pruned = df_combined_pruned.drop(columns=['flag_ror_any', 'flag_orcid_any']) 

flags_cols = [col for col in df_combined_pruned.columns if col.startswith('flag_')]
if flags_cols:
    df_combined_pruned['flagged_any'] = df_combined_pruned[flags_cols].any(axis=1)
    df_combined_pruned['flags'] = df_combined_pruned[flags_cols].sum(axis=1)
df_combined_pruned.to_csv(f'outputs/{today}_{institution_filename}_all-datasets-authors-PUBLISHED.csv', index=False, encoding='utf-8-sig')

df_all_affiliations_dedup = df_author_entries.drop_duplicates(subset=['author_affiliation'], keep='first')
df_all_affiliations_dedup = df_all_affiliations_dedup.rename(columns={'author_affiliation': 'affiliation'})
df_all_affiliations_dedup=df_all_affiliations_dedup[['affiliation']]

# ============================================
#           ROR-AFFILIATION MAP
# ============================================

if master_ror_matching is None: 
    # Create mapping file if it doesn't exist yet
    print('No existing primary map file found, creating new one.\n')
    print(f'Total unique affiliations: {len(df_all_affiliations_dedup) - 1}\n')
    if not ror_plugin:
        # If the ROR plug-in is not working, any previously-matched ROR entries will just list the ROR URL as the affiliation
        mask = ~df_all_affiliations_dedup['affiliation'].str.contains('https://ror.org/', case=False, na=False) # Case-insensitive and handles potential NaN values
        df_all_affiliations_dedup = df_all_affiliations_dedup[mask]
    df_all_affiliations_dedup.to_csv(f'{script_dir}/affiliation-map-primary.csv', index=False, encoding='utf-8-sig')
else: 
    # Concat existing mapping file with new list of unique affiliations, drop duplicates (keep first will retain existing matches)
    ## Requires you to have manually added a 'ror' column to original output
    print('Found existing primary map file, adding and deduplicating.\n')
    df_all_affiliations_dedup_expanded = pd.concat([master_ror_matching, df_all_affiliations_dedup])
    if not ror_plugin:
        # If the ROR plug-in is not working, any previously-matched ROR entries will just list the ROR URL as the affiliation name
        mask = ~df_all_affiliations_dedup_expanded['affiliation'].str.contains('https://ror.org/', case=False, na=False)
        df_all_affiliations_dedup_expanded = df_all_affiliations_dedup_expanded[mask]
    print(f'Total affiliations: {len(df_all_affiliations_dedup_expanded)}\n')
    df_all_affiliations_dedup_expanded_pruned = df_all_affiliations_dedup_expanded.drop_duplicates(subset=['affiliation'], keep='first')
    print(f'Total unique affiliations: {len(df_all_affiliations_dedup_expanded_pruned) - 1}\n')
    df_all_affiliations_dedup_expanded_pruned = df_all_affiliations_dedup_expanded_pruned[['affiliation', 'ror', 'official_name']]
    df_all_affiliations_dedup_expanded_pruned = df_all_affiliations_dedup_expanded_pruned.dropna(subset=['affiliation'])
    df_all_affiliations_dedup_expanded_pruned.to_csv(f'{script_dir}/affiliation-map_TEMP.csv', index=False, encoding='utf-8-sig')
if master_ror_matching is not None:
    print(f'Number of new affiliations to check: {len(df_all_affiliations_dedup_expanded_pruned) - len(master_ror_matching)}.\n')

print(f'Done\n---Time to run: {datetime.now() - start_time}---\n')
if len(final_timeouts) > 0:
    print('The following datasets were not retrieved from the API:')
    print(final_timeouts)
if test:
    print('\n**REMINDER: THIS IS A TEST RUN, AND ANY RESULTS ARE NOT COMPLETE!**\n')