import math
import os
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def env_bool(key, default=False):
    return os.environ.get(key, str(default)).strip().lower() in ('true', '1', 'yes')

# Build one API params dict per institution, adding a subtree filter only for institutions that have one configured
def build_institution_params(base_params, institutions, subtree_map):
    params_list = {}
    for name in institutions:
        params = base_params.copy()
        subtree = subtree_map.get(name)
        if subtree:
            params['subtree'] = subtree
        params_list[name] = params
    return params_list

# Whether a file should stay put (True) or be archived to an 'old-*' subdirectory (False)
# Files without a leading YYYYMMDD_ date are treated as not recent, matching prior behavior
def is_recent_file(filename, days=14, reference_date=None):
    reference_date = reference_date or datetime.now()
    try:
        file_date = datetime.strptime(filename[:8], '%Y%m%d')
    except ValueError:
        return False
    return (reference_date - file_date).days < days

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

# Standard function to look for file with specified pattern in name in specified directory, optionally also checking a fallback directory
def load_most_recent_file(outputs_dir, pattern, fallback_dir=None):
    directories = [outputs_dir] + ([fallback_dir] if fallback_dir else [])

    candidates = []
    for directory in directories:
        if not os.path.isdir(directory):
            continue
        for file in os.listdir(directory):
            if pattern in file:
                candidates.append((file, directory))
    candidates.sort(reverse=True)

    if not candidates:
        print(f"No file with '{pattern}' was found in {' or '.join(directories)}.\n")
        return None
    else:
        latest_file, directory = candidates[0]
        file_path = os.path.join(directory, latest_file)
        df = pd.read_csv(file_path)
        print(f"The most recent file '{latest_file}' has been loaded successfully from '{directory}'.\n")
        return df

# Standard function to return the 'nth' file with specified pattern in name in specified directory (starts at 1)
def load_nth_most_recent_file(outputs_dir, pattern, n=1):
    files = os.listdir(outputs_dir)
    files.sort(reverse=True)

    matching_files = [file for file in files if pattern in file]

    if len(matching_files) < n:
        print(f"Less than {n} files with '{pattern}' found in '{outputs_dir}'.\n")
        return None
    else:
        nth_file = matching_files[n - 1]
        file_path = os.path.join(outputs_dir, nth_file)
        df = pd.read_csv(file_path)
        print(f"The {n}{'st' if n==1 else 'nd' if n==2 else 'rd' if n==3 else 'th'} most recent file '{nth_file}' has been loaded successfully.\n")
        return df

# Load the file matching a specific date string in its filename, checking outputs_dir first and falling back to old_outputs_dir
def load_file_by_date(outputs_dir, old_outputs_dir, pattern, date_str):
    if not date_str:
        raise ValueError(f"No date provided to search for a file matching pattern '{pattern}'.")

    for directory in (outputs_dir, old_outputs_dir):
        if not os.path.isdir(directory):
            continue
        for file in os.listdir(directory):
            if date_str in file and pattern in file:
                file_path = os.path.join(directory, file)
                df = pd.read_csv(file_path)
                print(f"The file '{file}' matching date '{date_str}' has been loaded successfully from '{directory}'.\n")
                return df

    raise FileNotFoundError(f"No file matching date '{date_str}' and pattern '{pattern}' was found in '{outputs_dir}' or '{old_outputs_dir}'.")

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