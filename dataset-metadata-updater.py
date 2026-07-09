import csv
import os
import json
import pandas as pd
import re
import requests
import time
from datetime import datetime
from pathlib import Path
from utils import env_bool, load_most_recent_file

# ============================================
#               WORKFLOW SET-UP
# ============================================

# Config file (static variables)
with open('config.json', 'r') as file:
    config = json.load(file)

my_institution = os.environ['MY_INSTITUTION']

# Toggles
## Test environment (small sample size and other non-prod uses)
test = env_bool('TEST_ENVIRONMENT')
## Whether to retrieve from Native API
retrieve = env_bool('RETRIEVE_JSON')
# Number of datasets to fully process remediation
test_remediate = env_bool('TEST_REMEDIATION')
# Whether ROR external vocab plug-in is working
ror_plugin = env_bool('ROR_PLUGIN_ENABLED')
# Toggles for which attributes to re-curate
recurate_orcid = env_bool('RECURATION_ORCID')
recurate_ror = env_bool('RECURATION_ROR')
recurate_names = env_bool('RECURATION_NAMES')
recurate_punctuation = env_bool('RECURATION_PUNCTUATION')
recurate_keywords = env_bool('RECURATION_KEYWORDS')
recurate_works = env_bool('RECURATION_WORKS')
recurate_licenses = env_bool('RECURATION_LICENSES')
recurate_funding = env_bool('RECURATION_FUNDING')

# Load in data file
script_dir = os.getcwd()
if test:
    inputs_dir = os.path.join(script_dir, 'test/outputs')
else:
    inputs_dir = os.path.join(script_dir, 'outputs')

# Load primary file of remediations
pattern = '_final-combined-remediated_ANNOTATED.csv'
df = load_most_recent_file(inputs_dir, pattern)
# df = df.sort_values(by=['parent_dataverse', 'dataverse'])

# Load funder ROR map
funder_map_path = os.path.join(script_dir, 'funder-map-primary.csv')
funder_lookup = {}
if os.path.exists(funder_map_path):
    funder_df = pd.read_csv(funder_map_path)
    for _, row in funder_df.iterrows():
        agency = row.get('grant_agencies')
        ror    = row.get('ror')
        name   = row.get('official_name')
        if pd.notna(agency) and pd.notna(ror) and str(ror).strip() != '':
            funder_lookup[str(agency).strip()] = (str(ror).strip(), str(name).strip() if pd.notna(name) else None)
    print(f'Loaded funder map: {len(funder_lookup)} active matches.\n')
else:
    print('No funder map found — funder ROR fixes will be skipped.\n')

# Need to update Boolean 'fixed' columns to handle manual edits
## ORCID
df['fix_orcid'] = (df['orcid_action'].notna() & (df['orcid_action'] != '')) | (df['inferred_basis'].notna() & (df['inferred_basis'] != ''))
## Overarching 'fix' column
fixed_cols = [col for col in df.columns if col.startswith('fix_')]
if fixed_cols:
    df['fixed'] = df[fixed_cols].any(axis=1)

## Remove ones flagged only for review (fixed = False)
fixed_df = df[df['fixed']]
df_dois = fixed_df.drop_duplicates(subset=['doi'])
## Various ways to restrict the df so you can test on prod
if test_remediate:
    ### Funders are rare, need to ensure test catches some
    if recurate_funding:
        df_dois = df_dois.sort_values(by='fix_funder_ror', ascending=False)
    ### Just take the first X entries
    df_dois = df_dois.head(3)
    df = df[df['doi'].isin(df_dois['doi'])]

# Timestamp to calculate run time
start_time = datetime.now() 
# Current date for filenames
today = datetime.now().strftime('%Y%m%d') 

# Create directories
if test:
    test_dir = os.path.join(script_dir, 'test')
    if os.path.isdir(test_dir):
        print('test directory found - no need to recreate\n')
    else:
        os.mkdir(test_dir)
        print('test directory has been created\n')
    outputs_dir = os.path.join(test_dir, 'outputs')
    if os.path.isdir(outputs_dir):
        print('test outputs directory found - no need to recreate\n')
    else:
        os.mkdir(outputs_dir)
        print('test outputs directory has been created\n')
    json_dir = os.path.join(test_dir, 'json')
    if os.path.isdir(json_dir):
        print('test json directory found - no need to recreate\n')
    else:
        os.mkdir(json_dir)
        print('test json directory has been created\n')
    modified_json_dir = os.path.join(json_dir, 'modified')
    if os.path.isdir(modified_json_dir):
        print('modified json directory found - no need to recreate\n')
    else:
        os.mkdir(modified_json_dir)
        print('modified json directory has been created\n')
    logs_dir = os.path.join(test_dir, 'logs')
    if os.path.isdir(logs_dir):
        print('test logs directory found - no need to recreate\n')
    else:
        os.mkdir(logs_dir)
        print('test logs directory has been created\n')
else:
    outputs_dir = os.path.join(script_dir, 'outputs')
    if os.path.isdir(outputs_dir):
        print('outputs directory found - no need to recreate\n')
    else:
        os.mkdir(outputs_dir)
        print('outputs directory has been created\n')
    json_dir = os.path.join(script_dir, 'json')
    if os.path.isdir(json_dir):
        print('json directory found - no need to recreate\n')
    else:
        os.mkdir(json_dir)
        print('json directory has been created\n')
    modified_json_dir = os.path.join(json_dir, 'modified')
    if os.path.isdir(modified_json_dir):
        print('modified json directory found - no need to recreate\n')
    else:
        os.mkdir(modified_json_dir)
        print('modified json directory has been created\n')
    logs_dir = os.path.join(script_dir, 'logs')
    if os.path.isdir(logs_dir):
        print('logs directory found - no need to recreate\n')
    else:
        os.mkdir(logs_dir)
        print('logs directory has been created\n')

# ============================================
#           API PARAMETERS: Datasets
# ============================================
if retrieve:
    print('Beginning to retrieve JSON representations of datasets.\n')
    print(f'Retrieving {len(df_dois)} records.\n')
    # Create empty lists to record timeouts
    first_timeouts = []
    final_timeouts = []

    headers_tdr = {
        'X-Dataverse-key': os.environ['DATAVERSE_TOKEN']
    }

    url_tdr_native = 'https://dataverse.tdl.org/api/datasets/'

    for idx, row in df_dois.iterrows():
        doi = row['doi']
        status = row['current_status']
        try:
            response = requests.get(f'{url_tdr_native}:persistentId/?persistentId=doi:{doi}', headers=headers_tdr, timeout=5)
            if response.status_code == 200:
                print(f'Retrieving JSON representation for {doi}...\n')
                item = response.json()
                
                safe_doi = re.sub(r'[<>:\'/\\\\|?*]', '_', doi)
                filename = f'{json_dir}/{safe_doi}-dataset-metadata_{status}.json'
                with open(filename, 'w') as f:
                    json.dump(item, f, indent=4)
            else:
                print(f'Error fetching {doi}: {response.status_code}, {response.text}')
        except Exception:
            first_timeouts.append({'doi': doi, 'reason': 'Persistent Timeout/Error'})
            list_length = len(first_timeouts)
            print(f'The current number of timeouts is: {list_length}.\n')
        
if retrieve and first_timeouts:
    print(f'\n--- Retrying {len(first_timeouts)} repeat timeouts with 10s limit ---\n')
    time.sleep(2) 
    for timeout in first_timeouts:
        doi = timeout['doi']
        status = df_dois[df_dois['doi'] == doi]['current_status'].values[0]
        try:
            response = requests.get(f'{url_tdr_native}:persistentId/?persistentId=doi:{doi}', headers=headers_tdr, timeout=5)
            if response.status_code == 200:
                print(f'Retrieving JSON representation for {doi}...\n')
                item = response.json()
                
                safe_doi = re.sub(r'[<>:\"/\\\\|?*]', '_', doi)
                filename = f'{json_dir}/{safe_doi}-dataset-metadata_{status}.json'
                with open(filename, 'w') as f:
                    json.dump(item, f, indent=4)
            else:
                print(f'Error fetching {doi}: {response.status_code}, {response.text}')
        except Exception:
            final_timeouts.append({'doi': doi, 'reason': 'Persistent Timeout/Error'})
            list_length = len(final_timeouts)
            print(f'The final number of timeouts is: {list_length}.\n')

    print('Retrieval complete.')

# ----- Creates JSON and CSV logs ----- #
class MetadataChangeLogger:
    def __init__(self):
        self.changes = []
    # ----- Creates individual record for each change ----- #
    def log_change(self, doi, author_original_name, field_name, original_value, new_value, change_type):
        self.changes.append({
            'doi': doi,
            'author_original_name': author_original_name,
            'field_name': field_name,
            'original_value': original_value,
            'new_value': new_value,
            'change_type': change_type,
            'timestamp': datetime.now().isoformat()
        })
    # ----- Exports to CSV ----- #
    def export_to_csv(self, output_path):
        """
        Exports change log to CSV format.
        """
        if not self.changes:
            print('  ⚠ No changes to log')
            return
        
        df_log = pd.DataFrame(self.changes)
        df_log.to_csv(output_path, index=False, encoding='utf-8')
        print(f'  ✓ CSV log saved: {output_path}')
    # ----- Exports to JSON ----- #
    def export_to_json(self, output_path):
        """
        Exports change log to structured JSON format.
        """
        if not self.changes:
            print('  ⚠ No changes to log')
            return
        
        # Group changes by DOI and author for structured output
        structured_log = {}
        
        for change in self.changes:
            doi = change['doi']
            author = change['author_original_name']
            
            # Initialize DOI entry if needed
            if doi not in structured_log:
                structured_log[doi] = {
                    'doi': doi,
                    'authors': {}
                }
            
            # Initialize author entry if needed
            if author not in structured_log[doi]['authors']:
                structured_log[doi]['authors'][author] = {
                    'author_original_name': author,
                    'changes': {}
                }
            
            # Add change to author's record
            field = change['field_name']
            structured_log[doi]['authors'][author]['changes'][field] = {
                'original': change['original_value'],
                'revised': change['new_value'],
                'change_type': change['change_type'],
                'timestamp': change['timestamp']
            }
        
        # Convert to list format for JSON
        output = {
            'metadata_changes': list(structured_log.values()),
            'summary': {
                'total_changes': len(self.changes),
                'datasets_affected': len(structured_log),
                'generated_at': datetime.now().isoformat()
            }
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=4, ensure_ascii=False)
        
        print(f'  ✓ JSON log saved: {output_path}')

# ----- Locate JSON by DOI ----- #
def find_json_by_doi(doi, json_folder):
    safe_doi = re.sub(r'[<>:\"/\\\\|?*]', '_', doi)
    expected_filename = f'{safe_doi}-dataset-metadata_'
    json_path = os.path.join(json_folder, expected_filename)
    
    try:
        files = os.listdir(json_folder)
        matching_files = [f for f in files if f.startswith(expected_filename) and f.endswith('.json')]
        
        if matching_files:
            json_path = os.path.join(json_folder, matching_files[0])
            return json_path
        else:
            print(f'Warning: JSON file not found for DOI {doi}')
            return None
    except FileNotFoundError:
        print(f'Warning: JSON folder not found at {json_folder}')
        return None

# ----- Applies and logs all metadata fixes ----- #
def apply_author_fixes(author, row, doi, logger):
    author_original_name = row['author_name']
    
    # Fix 1: Update author name (Last, First format)
    if recurate_names and row['fix_name'] == True:
        original_name = author['authorName']['value']
        new_name = row['author_name_remediated']
        
        author['authorName']['value'] = new_name
        
        logger.log_change(
            doi=doi,
            author_original_name=author_original_name,
            field_name='authorName',
            original_value=original_name,
            new_value=new_name,
            change_type=row['author_name_action']
        )
        
        print(f'  ✓ Fixed name: {original_name} → {new_name}')
    
    # Fix 2: Expand ORCID identifier
    if recurate_orcid and row['fix_orcid'] == True:
        # Determine which ORCID to use
        orcid_value = None
        if pd.notna(row.get('orcid_remediated')):
            orcid_value = row['orcid_remediated']
            change_type = row['orcid_action']
        elif pd.notna(row.get('inferred_orcid')):
            orcid_value = row['inferred_orcid']
            change_type = 'orcid_inference'
        
        if orcid_value:
            # Determine person name (use remediated if available)
            person_name = row['author_name_remediated'] if pd.notna(row.get('author_name_remediated')) else row['author_name']
            
            # Get original value for logging
            original_orcid = author.get('authorIdentifier', {}).get('value', 'None')
            
            # Add authorIdentifierScheme if it doesn't exist
            if 'authorIdentifierScheme' not in author:
                author['authorIdentifierScheme'] = {
                    'typeName': 'authorIdentifierScheme',
                    'multiple': False,
                    'typeClass': 'controlledVocabulary',
                    'value': 'ORCID'
                }
            
            # Add authorIdentifier if it doesn't exist
            if 'authorIdentifier' not in author:
                author['authorIdentifier'] = {
                    'typeName': 'authorIdentifier',
                    'multiple': False,
                    'typeClass': 'primitive',
                    'value': orcid_value,
                    'expandedvalue': {
                        'personName': person_name,
                        '@id': orcid_value,
                        'scheme': 'ORCID',
                        '@type': 'https://schema.org/Person'
                    }
                }
            else: # Update if field exists in some form
                author['authorIdentifier'] = {
                    'typeName': 'authorIdentifier',
                    'multiple': False,
                    'typeClass': 'primitive',
                    'value': orcid_value,
                    'expandedvalue': {
                        'personName': person_name,
                        '@id': orcid_value,
                        'scheme': 'ORCID',
                        '@type': 'https://schema.org/Person'
                    }
                }
            
            logger.log_change(
                doi=doi,
                author_original_name=author_original_name,
                field_name='authorIdentifier',
                original_value=original_orcid,
                new_value=orcid_value,
                change_type=change_type
            )
            
            print(f'  ✓ Expanded ORCID: {orcid_value}')
    
    # Fix 3: Add ROR affiliation
    if recurate_ror and row['fix_ror'] == True and pd.notna(row.get('ror')) and pd.notna(row.get('official_name')):
        # Get original value for logging
        original_affiliation = author.get('authorAffiliation', {}).get('value', 'None')
        
        # Create or update authorAffiliation with expanded structure
        if 'authorAffiliation' not in author:
            author['authorAffiliation'] = {}
            
        author['authorAffiliation'] = {
            'typeName': 'authorAffiliation',
            'multiple': False,
            'typeClass': 'primitive',
            'value': row['ror'],
            'expandedvalue': {
                'scheme': 'http://www.grid.ac/ontology/',
                'termName': row['official_name'],
                '@type': 'https://schema.org/Organization'
            }
        }
        
        logger.log_change(
            doi=doi,
            author_original_name=author_original_name,
            field_name='authorAffiliation',
            original_value=original_affiliation,
            new_value=f"{row['official_name']} ({row['ror']})",
            change_type='added author ROR'
        )
        
        print(f'  ✓ Added author ROR: {row['official_name']} ({row['ror']})')

# ----- Pushes updates into new JSON ----- #    
def update_author_in_json(data, row, doi, logger):
    try:
        # Navigate to citation fields
        citation_fields = data['data']['latestVersion']['metadataBlocks']['citation']['fields']
        
        # Find the author field
        for field in citation_fields:
            if field.get('typeName') == 'author':
                authors = field.get('value', [])
                
                # Find matching author by name
                for author in authors:
                    author_name_value = author.get('authorName', {}).get('value', '')
                    
                    if author_name_value == row['author_name']:
                        print(f'  → Matched author: {row['author_name']}')
                        apply_author_fixes(author, row, doi, logger)
                        return True
                
                print(f'  ⚠ Author not found: {row['author_name']}')
                return False
        
        print('  ⚠ No author field found in dataset')
        return False
        
    except KeyError as e:
        print(f'  ✗ Error navigating JSON structure: {e}')
        return False

# ----- Fixes malformatted keywords ----- #
def fix_keywords(data, row, doi, logger):
    if not row['fix_keywords'] or not pd.notna(row.get('keywords_remediated')):
        return False
    
    try:
        citation_fields = data['data']['latestVersion']['metadataBlocks']['citation']['fields']
        
        for field in citation_fields:
            if field.get('typeName') == 'keyword':

                original_keywords = []
                if field.get('value'):
                    for kw in field['value']:
                        original_keywords.append(kw.get('keywordValue', {}).get('value', ''))
                
                import ast
                if isinstance(row['keywords_remediated'], str):
                    try:
                        keywords_list = ast.literal_eval(row['keywords_remediated'])
                    except:
                        keywords_list = [kw.strip() for kw in row['keywords_remediated'].split(',')]
                else:
                    keywords_list = row['keywords_remediated']
                
                new_keyword_values = []
                for keyword in keywords_list:
                    if keyword:
                        new_keyword_values.append({
                            'keywordValue': {
                                'typeName': 'keywordValue',
                                'multiple': False,
                                'typeClass': 'primitive',
                                'value': keyword.strip()
                            }
                        })
                
                field['value'] = new_keyword_values
                
                logger.log_change(
                    doi=doi,
                    author_original_name='DATASET_LEVEL',
                    field_name='keyword',
                    original_value=', '.join(original_keywords),
                    new_value=', '.join(keywords_list),
                    change_type='keywords split'
                )
                
                print(f'  ✓ Fixed keywords: {len(original_keywords)} → {len(keywords_list)} entries')
                return True
        
        print('  ⚠ No keyword field found in dataset')
        return False
        
    except Exception as e:
        print(f'  ✗ Error fixing keywords: {e}')
        return False

# ----- Fixes title malformatting ----- #
def fix_title(data, row, doi, logger):
    if not row['fix_title'] or not pd.notna(row.get('dataset_title_remediated')):
        return False
    
    try:
        citation_fields = data['data']['latestVersion']['metadataBlocks']['citation']['fields']
        for field in citation_fields:
            if field.get('typeName') == 'title':

                original_title = field.get('value', '')
                new_title = row['dataset_title_remediated']
                
                field['value'] = new_title
                
                logger.log_change(
                    doi=doi,
                    author_original_name='DATASET_LEVEL',
                    field_name='title',
                    original_value=original_title,
                    new_value=new_title,
                    change_type=row.get('title_action', 'title_correction')
                )
                
                print('  ✓ Fixed title')
                return True
        
        print(' ⚠ No title field found in dataset')
        return False
        
    except Exception as e:
        print(f' ✗ Error fixing title: {e}')
        return False

# ----- Adds ROR to funder ----- #    
def fix_funder_ror(data, row, doi, logger):
    """Add ROR value + expandedvalue to grantNumber entries using the funder_lookup map."""
    if not (row.get('fix_funder_ror') == True):
        return False
    try:
        citation_fields = data['data']['latestVersion']['metadataBlocks']['citation']['fields']
        for field in citation_fields:
            if field.get('typeName') == 'grantNumber':
                changed_pairs = []
                for grant in field.get('value', []):
                    agency_field = grant.get('grantNumberAgency', {})
                    # Only fix entries that don't already have an expandedvalue
                    if agency_field.get('expandedvalue'):
                        continue
                    agency_value = agency_field.get('value', '').strip()
                    if not agency_value:
                        continue
                    match = funder_lookup.get(agency_value)
                    if not match:
                        continue
                    matched_ror, matched_name = match
                    agency_field['value'] = matched_ror
                    agency_field['expandedvalue'] = {
                        'scheme':   'http://www.grid.ac/ontology/',
                        'termName': matched_name or agency_value,
                        '@type':    'https://schema.org/Organization'
                    }
                    changed_pairs.append((agency_value, matched_ror))
                    logger.log_change(
                        doi=doi,
                        author_original_name='DATASET_LEVEL',
                        field_name='funder',
                        original_value=agency_value,
                        new_value=matched_ror,
                        change_type='added funder ROR'
                    )

                if changed_pairs:
                    for orig, ror in changed_pairs:
                        print(f'  ✓ Funder ROR added: {orig} → {ror}')
                    return True
        return False
    except Exception as e:
        print(f'  ✗ Error fixing funder ROR: {e}')
        return False

# ----- Adds language field if missing ----- #
def fix_language(data, doi, logger):
    try:
        citation_fields = data['data']['latestVersion']['metadataBlocks']['citation']['fields']

        for field in citation_fields:
            if field.get('typeName') == 'language':
                return False  # already present, nothing to do

        citation_fields.append({
            'typeName': 'language',
            'multiple': True,
            'typeClass': 'controlledVocabulary',
            'value': ['English']
        })

        logger.log_change(
            doi=doi,
            author_original_name='DATASET_LEVEL',
            field_name='language',
            original_value=None,
            new_value='English',
            change_type='added missing language field'
        )

        print('  ✓ Added language field: English')
        return True

    except Exception as e:
        print(f'  ✗ Error adding language field: {e}')
        return False

# ----- Overarching function ----- #
def update_all_author_metadata(df, json_folder, output_folder):
    print('\n' + '='*60)
    print('STARTING METADATA UPDATE PROCESS')
    print('='*60 + '\n')
    
    # Initialize logger
    logger = MetadataChangeLogger()
    
    os.makedirs(output_folder, exist_ok=True)
    
    # Group by DOI
    grouped = df.groupby('doi')
    
    total_datasets = len(grouped)
    processed = 0
    updated = 0
    
    for doi, author_group in grouped:
        processed += 1
        print(f'\n[{processed}/{total_datasets}] Processing DOI: {doi}')
        
        # Find corresponding JSON file
        json_file = find_json_by_doi(doi, json_folder)
        
        if json_file is None:
            continue
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f'  ✗ Error loading JSON: {e}')
            continue
        
        # Track if any changes were made
        changes_made = False
        
        # ============================================
        # DATASET-LEVEL FIXES (once per DOI)
        # ============================================
        # Use first row since all rows have same dataset-level data
        dataset_row = author_group.iloc[0]
        
        print('  → Checking dataset-level metadata...')
        
        # Fix keywords
        if recurate_keywords:
            if fix_keywords(data, dataset_row, doi, logger):
                changes_made = True
        
        # Fix title
        if recurate_punctuation:
            if fix_title(data, dataset_row, doi, logger):
                changes_made = True

        if recurate_funding:
            if fix_funder_ror(data, dataset_row, doi, logger):
                changes_made = True

        if my_institution == 'UT Austin' and fix_language(data, doi, logger):
            changes_made = True

        # ============================================
        # AUTHOR-LEVEL FIXES (for each author)
        # ============================================
        print('  → Checking author-level metadata...')
        
        # Update each author in this dataset
        for idx, row in author_group.iterrows():
            if update_author_in_json(data, row, doi, logger):
                changes_made = True
        
        # ============================================
        # SAVE MODIFIED JSON
        # ============================================
        if changes_made:
            original_filename = os.path.basename(json_file)
            output_path = os.path.join(output_folder, f'modified-{original_filename}')
            
            try:
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4, ensure_ascii=False)
                print(f'  ✓ Saved: modified-{original_filename}')
                updated += 1
            except Exception as e:
                print(f'  ✗ Error saving JSON: {e}')
        else:
            print('  → No changes made for this dataset')
    
    print('\n' + '='*60)
    print('PROCESS COMPLETE')
    print(f'Datasets processed: {processed}')
    print(f'Datasets updated: {updated}')
    print('='*60 + '\n')
    
    # Export logs
    print('\n' + '='*60)
    print('EXPORTING CHANGE LOGS')
    print('='*60 + '\n')
    
    log_csv_path = os.path.join(logs_dir, f'{today}_metadata-changes-log.csv')
    log_json_path = os.path.join(logs_dir, f'{today}_metadata-changes-log.json')
    
    logger.export_to_csv(log_csv_path)
    logger.export_to_json(log_json_path)
    
    print(f'\n{'='*60}')
    print(f'Summary: {len(logger.changes)} total changes logged')
    print(f'{'='*60}\n')
    
    return logger

# ============================================
#     UPDATE AUTHOR METADATA IN JSONs
# ============================================

print('\n' + '-'*60)
print('Beginning updating author metadata')
print('-'*60 + '\n')

# Use the modified_json_dir we defined earlier
update_all_author_metadata(df, json_dir, modified_json_dir)

print('\n' + '-'*60)
print('Finished updating author metadata')
print('-'*60 + '\n')

# ============================================
#     UPDATE AUTHOR METADATA THROUGH API
# ============================================

server_url = 'https://dataverse.tdl.org'
headers_tdr = {
    'X-Dataverse-key': os.environ['DATAVERSE_TOKEN'],
    'Content-Type': 'application/json'
}

dir_path = Path(modified_json_dir)

# Restrict uploads to DOIs processed this run
current_dois = set(df_dois['doi'].tolist())

failed_uploads = []
for filename in dir_path.iterdir():
    if not filename.is_file():
        continue
    doi_temp = filename.stem.split("modified-")[1].split("-dataset-metadata")[0]
    doi_without_prefix = doi_temp.replace('_', '/')
    if doi_without_prefix not in current_dois:
        print(f'Skipping {doi_without_prefix} (not in current run)\n')
        continue
    doi = f"doi:{doi_without_prefix}"
    print(f'Updating {doi}\n')

    with open(filename, 'r', encoding='utf-8') as f:
        full_data = json.load(f)

    dataset_info = full_data['data']
    # Check for latestVersion or datasetVersion (varies depending on whether it has been versioned or not)
    if 'latestVersion' in dataset_info:
        dataset_version = dataset_info['latestVersion']
    else:
        dataset_version = dataset_info['datasetVersion']

    # Create payload with EVERYTHING except 'files' (https://guides.dataverse.org/en/latest/api/native-api.html#update-metadata-for-a-dataset)
    payload = {k: v for k, v in dataset_version.items() if k != 'files'}

    # Create draft
    try:
        update_url = f'{server_url}/api/datasets/:persistentId/versions/:draft?persistentId={doi}'
        response = requests.put(update_url, headers=headers_tdr, json=payload)

        if response.status_code == 200:
            print('✓ Dataset metadata updated.\n')
        else:
            error_msg = f'Status code: {response.status_code}. {response.text}'
            print(f'✗ Failed to update metadata. {error_msg}\n')
            failed_uploads.append((doi, error_msg))

        filtered_row = df_dois[df_dois['doi'] == doi_without_prefix]
        needs_review = filtered_row['to_review'].iloc[0]
        is_draft = filtered_row['current_status'].iloc[0] == 'DRAFT'

        if needs_review or is_draft:
            print('⚠ Dataset needs further review or was in draft status.\n')

            # ------------------------------------------------------------------------------------------------ #
            # WARNING: uncommenting the else block below will AUTO-PUBLISH datasets to production.
            # Only do this after verifying modified JSONs are correct and no datasets are flagged for review.
            # ------------------------------------------------------------------------------------------------ #

        # else:
        #     # Publish the dataset
        #     publish_url = f'{server_url}/api/datasets/:persistentId/actions/:publish?persistentId={doi}&type=minor'
        #     response = requests.post(publish_url, headers=headers_tdr)

        #     if response.status_code == 200:
        #         print('✓ Dataset published successfully.\n')
        #     else:
        #         print(f'✗ Failed to publish dataset. Status code: {response.status_code}')
        #         print(response.text)
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        print(f'Request failed for {doi}: {error_msg}')
        failed_uploads.append((doi, error_msg))

# Print failed uploads
print(failed_uploads)
list_length = len(failed_uploads)
print(f'The final number of failed uploads is: {list_length}.\n')
## Saving failed uploads
with open(f'{logs_dir}/{today}_failed-uploads.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['DOI', 'Error Message'])
    writer.writerows(failed_uploads)

# Calculate total runtime
end_time = datetime.now()
runtime = end_time - start_time
print(f'\n{'='*60}')
print(f'Total Runtime: {runtime}')
print(f'{'='*60}\n')