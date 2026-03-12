import math
import os
import pandas as pd
import requests

# Retrieves single page of Dataverse results
def retrieve_page_dataverse(url, params=None, headers=None):
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'data': {'items': [], 'total_count': 0}}
# Retrieves all pages of DataCite results
def retrieve_dataverse(url, params, headers, page_start, per_page, page_limit=None):
    all_data_dataverse = []
    params = params.copy()
    current_page = 0
    adjusted_page = current_page + 1
    params['start'] = page_start
    params['per_page'] = per_page

    while True:
        data = retrieve_page_dataverse(url, params, headers)
        total_count = data['data']['total_count']
        total_pages = math.ceil(total_count / per_page) if per_page else 1
        adjusted_pages = total_pages + 1
        print(f'Retrieving page {adjusted_page} of {adjusted_pages} pages...\n')

        if not data['data']:
            print('No data found.')
            break

        all_data_dataverse.extend(data['data']['items'])

        # Pagination logic
        current_page += 1
        adjusted_page +=1
        params['start'] += per_page

        if params['start'] >= total_count:
            print('End of response.\n')
            break
        if page_limit and current_page >= page_limit:
            print('Reached page limit.\n')
            break

    return all_data_dataverse
## Retrieves many pages from many institutions
def retrieve_all_institutions(url, params_list, headers, page_start, per_page, page_limit = None):
    all_data = []

    for institution_name, params in params_list.items():
        # Reset k for each institution if needed (but is k still used?)
        all_data_tdr = retrieve_dataverse(url, params, headers, page_start, per_page, page_limit)
        for entry in all_data_tdr:
            entry['institution'] = institution_name 
            all_data.append(entry)

    return all_data

# Standard function to look for file with specified pattern in name in specified directory
def load_most_recent_file(outputs_dir, pattern):
    files = os.listdir(outputs_dir)
    files.sort(reverse=True)

    latest_file = None
    for file in files:
        if pattern in file:
            latest_file = file
            break

    if not latest_file:
        print(f"No file with '{pattern}' was found in the directory '{outputs_dir}'.\n")
        return None
    else:
        file_path = os.path.join(outputs_dir, latest_file)
        df = pd.read_csv(file_path)
        print(f"The most recent file '{latest_file}' has been loaded successfully.\n")
        return df
    
# Validate formatting of ORCID and ROR in metadata
def is_valid_orcid(orcid):
    # ORCID must be a URL not just the string and not have a space after the shoulder
    return isinstance(orcid, str) and orcid.startswith("https://orcid.org/0")
def is_valid_ror(ror):
    return isinstance(ror, str) and ror.startswith("https://ror.org/")

# Return only the highest value for the version number in a Dataverse retrieval
def extract_max_version(val):
    if isinstance(val, str):
        try:
            versions = [float(v.strip()) for v in val.split(';')]
            return max(versions)
        except ValueError:
            return val  # In case of unexpected format
    return val