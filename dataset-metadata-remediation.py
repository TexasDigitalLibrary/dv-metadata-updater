import ast
import json
import os
import pandas as pd
import re
from datetime import datetime
from rapidfuzz import process, fuzz
from utils import env_bool, load_most_recent_file

# ============================================
#           WORKFLOW SET-UP
# ============================================

# Config file
with open('config.json', 'r') as file:
    config = json.load(file)

# Dynamic variables for which attributes to re-curate
recurate_funding = env_bool('RECURATION_FUNDING')
recurate_keywords = env_bool('RECURATION_KEYWORDS')
recurate_licenses = env_bool('RECURATION_LICENSES')
recurate_names = env_bool('RECURATION_NAMES')
recurate_orcid = env_bool('RECURATION_ORCID')
recurate_punctuation = env_bool('RECURATION_PUNCTUATION')
recurate_ror = env_bool('RECURATION_ROR')
recurate_works = env_bool('RECURATION_WORKS')

# Test environment (incomplete run, faster to complete)
test = env_bool('TEST_ENVIRONMENT')
## Restrict to your/one institution in TDR (True to restrict to your institution)
only_my_institution = env_bool('ONLY_MY_INSTITUTION')
# Filename version of your institution's name
my_institution_filename = os.environ['INSTITUTION_FILENAME']
# Condition based on 'only_my_institution' toggle
if only_my_institution:
    institution_filename = my_institution_filename
else:
    institution_filename = 'all-institutions'
# Short-hand version of your institution's name
my_institution_short_name = os.environ['MY_INSTITUTION']
# Get ROR
institution_ror_map = config['INSTITUTION']['ROR_MAP']
institution_ror = institution_ror_map.get(my_institution_short_name)

# Get directories
script_dir = os.getcwd()
if test:
    outputs_dir = os.path.join(script_dir, 'test/outputs')
else:
    outputs_dir = os.path.join(script_dir, 'outputs')


# Load most recent version of dataset-authors file
pattern = '_all-datasets-authors-PUBLISHED.csv'
datasets = load_most_recent_file(outputs_dir, pattern)

# Load most recent version of authors file
pattern = '_all-authors-datasets-PUBLISHED.csv'
authors = load_most_recent_file(outputs_dir, pattern)

# Load affiliation ROR map
ror_map = pd.read_csv('affiliation-map-primary.csv')

# Load funder ROR map and build lookup: agency_name -> (ror, official_name)
if recurate_funding:
    funder_map_path = os.path.join(script_dir, 'funder-map-primary.csv')
    funder_lookup = {}
    if os.path.exists(funder_map_path):
        funder_map_df = pd.read_csv(funder_map_path)
        for _, row in funder_map_df.iterrows():
            agency = row.get('grant_agencies')
            ror = row.get('ror')
            name = row.get('official_name')
            if pd.notna(agency) and pd.notna(ror) and str(ror).strip() != '':
                funder_lookup[str(agency).strip()] = (str(ror).strip(), str(name).strip() if pd.notna(name) else None)
        print(f'Loaded funder map: {len(funder_lookup)} active matches.\n')
    else:
        print('No funder map found — funder ROR matching will be skipped.\n')

# Timestamp to calculate run time
start_time = datetime.now() 
# Current date for filenames
today = datetime.now().strftime('%Y%m%d') 

# List of depositor/contact names to omit from select re-curation flagging
excluded_people = config['EXCLUDED']
excluded_people_set = set(excluded_people)

# ============================================
#           DATASET-LEVEL RE-CURATION
# ============================================

# Progressive subsetting and printing
print(f'Total number of datasets: {len(datasets)}.\n')
datasets_flagged = datasets[datasets['flags'] > 0]
datasets_clean = datasets[datasets['flags'] == 0]
print(datasets_clean)
print(f'Total number of flagged datasets: {len(datasets_flagged)}.\n')
datasets_flagged_retained = datasets_flagged[
    ~datasets_flagged['dataset_depositor'].isin(excluded_people_set) &
    ~datasets_flagged['dataset_contact'].isin(excluded_people_set)
]

print(f'Total number of flagged datasets after omission: {len(datasets_flagged_retained)}.\n')

# ============================================
# Keyword formatting
# ============================================
if recurate_keywords:
    datasets_flagged_retained['keywords_remediated'] = pd.NA
    mask_keywords = datasets_flagged_retained['flag_keyword'] == True
    datasets_flagged_retained.loc[mask_keywords, 'keywords_remediated'] = datasets_flagged_retained.loc[mask_keywords, 'keywords'].apply(
            lambda x: [item.strip() for item in re.split(r'[;,]', ast.literal_eval(x)[0]) if item.strip()]
        )
    
    fixed_keyword_count = datasets_flagged_retained['keywords_remediated'].count()
    print(f'Number of datasets that had keywords fixed: {fixed_keyword_count}.\n')

# ============================================
# Title formatting
# ============================================
if recurate_punctuation:
    datasets_flagged_retained['dataset_title_remediated'] = pd.NA
    mask_title = (datasets_flagged_retained['flag_title_space'] | datasets_flagged_retained['flag_title_period'])
    datasets_flagged_retained.loc[mask_title, 'dataset_title_remediated'] = (datasets_flagged_retained.loc[mask_title, 'dataset_title'])

    mask_space = datasets_flagged_retained['flag_title_space']
    datasets_flagged_retained.loc[mask_space, 'dataset_title_remediated'] = (datasets_flagged_retained.loc[mask_space, 'dataset_title_remediated'].str.strip())
    mask_period = datasets_flagged_retained['flag_title_period']
    datasets_flagged_retained.loc[mask_period, 'dataset_title_remediated'] = (datasets_flagged_retained.loc[mask_period, 'dataset_title_remediated'].str.replace(r'\.$', '', regex=True))

    # Add Boolean columns
    datasets_flagged_retained['fix_keywords'] = datasets_flagged_retained['keywords_remediated'].notna()
    datasets_flagged_retained['fix_title'] = datasets_flagged_retained['dataset_title_remediated'].notna()

    fixed_title_count = datasets_flagged_retained['dataset_title_remediated'].count()
    print(f'Number of datasets that had the title fixed: {fixed_title_count}.\n')

# ============================================
# Funder RORs
# ============================================
if recurate_funding and funder_lookup:
    def match_funder_rors(row):
        agencies = row.get('grant_agencies')
        if pd.isna(agencies) or str(agencies).strip() == '':
            return pd.NA, pd.NA
        agency_list = [a.strip() for a in str(agencies).split(';') if a.strip()]
        rors   = []
        names  = []
        matched_any = False
        for agency in agency_list:
            if agency in funder_lookup:
                ror, official_name = funder_lookup[agency]
                rors.append(ror)
                names.append(official_name or agency)
                matched_any = True
            else:
                # Preserve existing ROR if already present, else leave blank
                existing_rors = [r.strip() for r in str(row.get('grant_rors', '') or '').split(';')]
                idx = agency_list.index(agency)
                existing = existing_rors[idx] if idx < len(existing_rors) else ''
                rors.append(existing)
                names.append('')
        if not matched_any:
            return pd.NA, pd.NA
        return '; '.join(rors), '; '.join(names)

    mask_funder = datasets_flagged_retained.get('flag_funder_ror', pd.Series(False, index=datasets_flagged_retained.index)).astype(bool)
    datasets_flagged_retained['grant_rors_remediated']      = pd.NA
    datasets_flagged_retained['grant_agencies_remediated']  = pd.NA

    if mask_funder.any():
        results = datasets_flagged_retained.loc[mask_funder].apply(match_funder_rors, axis=1, result_type='expand')
        datasets_flagged_retained.loc[mask_funder, 'grant_rors_remediated']     = results[0]
        datasets_flagged_retained.loc[mask_funder, 'grant_agencies_remediated'] = results[1]

    datasets_flagged_retained['fix_funder_ror'] = datasets_flagged_retained['grant_rors_remediated'].notna()
    print(f'Datasets with funder ROR added: {datasets_flagged_retained["fix_funder_ror"].sum()}.\n')

datasets_flagged_retained.to_csv(f'{outputs_dir}/{today}_{my_institution_filename}_final-datasets-remediated.csv', index=False, encoding='utf-8-sig')

# ============================================
#           AUTHOR-LEVEL RE-CURATION
# ============================================
## Creating mask to omit certain author entries from re-curation
authors_retained = authors[
    ~(
        (
            authors['dataset_depositor'].isin(excluded_people_set) |
            authors['dataset_contact'].isin(excluded_people_set) |
            authors['author_name'].str.contains('Author', case=False, na=False)
        ) & ~(
            authors['author_name'].str.contains('university', case=False, na=False) |
            authors['author_affiliation'].str.contains('university', case=False, na=False)
        )
    )
]
print(f'There are {len(authors_retained)} of {len(authors)} authors being analyzed.\n')

# ============================================
# ROR standardization
# ============================================
if recurate_ror:
    authors_retained['flag_ror'] = authors_retained['flag_ror'].astype(bool)
    missing_count = authors_retained['flag_ror'].sum()
    print(f'There are {missing_count} of {len(authors_retained)} authors without a ROR ID.\n')

    authors_merged = pd.merge(authors_retained, ror_map, left_on='author_affiliation', right_on='affiliation', how='left')
    mask_ror = authors_merged['flag_ror']
    mask_ror_match = authors_merged['ror'].notna()
    mask_ror_final = mask_ror & mask_ror_match
    fixed_ror = mask_ror_final.sum()
    print(f'Number of author entries that had ROR added: {fixed_ror}.\n')

    if institution_ror:
        institution_ror_count = (authors_merged.loc[mask_ror_final, 'ror'] == institution_ror).sum()
        print(f'Number of author entries that had the {my_institution_short_name} ROR added ({institution_ror}): {institution_ror_count}.\n')
    else:
        print(f'No ROR URL configured for {my_institution_short_name}, skipping institution-specific count.\n')

# ============================================
# Author name formatting
# ============================================
if recurate_names:
    # Excluding certain datasets
    authors_merged = authors_merged[
        ~authors_merged['dataset_depositor'].isin(excluded_people_set) &
        ~authors_merged['dataset_contact'].isin(excluded_people_set)
    ]

    # Create remediated columns
    authors_merged['author_name_remediated'] = pd.NA
    authors_merged['author_name_action'] = pd.NA

    # Work only with rows that need name fixing
    mask_author_broad = authors_merged['flag_name'] & authors_merged['author_name'].notna()

    # Get the name column as string for pattern matching
    names = authors_merged.loc[mask_author_broad, 'author_name'].astype(str).str.strip().str.rstrip(',')

    # Initialize working columns for this subset
    fixed_names = names.copy()
    corrections = pd.Series('', index=names.index)

    ## Order correct but with semi-colon not comma
    mask_semicolon = mask_author_broad & names.str.contains(';') & ~names.str.contains(',')
    fixed_names.loc[mask_semicolon] = names.str.replace(';', ',')
    corrections.loc[mask_semicolon] = 'semi-colon replacement'

    ## ALL CAPS
    mask_case = (mask_author_broad & names.str.isupper() & (names.str.split().str.len() > 1))
    fixed_names.loc[mask_case] = names.loc[mask_case].str.title()
    corrections.loc[mask_case] = 'changing case'

    ## Two names, no comma (presumed to be First Last; want in Last, First)
    mask_two_parts = mask_author_broad & (
        names.str.split().str.len() == 2
    ) & (~names.str.contains(',', na=False))
    if mask_two_parts.any():
        # Split names
        first_names = names[mask_two_parts].str.split().str[0]
        last_names = names[mask_two_parts].str.split().str[1]
        # Flip and add comma + space
        fixed_names.loc[mask_two_parts] = last_names + ', ' + first_names
        corrections.loc[mask_two_parts] = 'order flipped'

    ## Two full names and one initial in between, no comma
    mask_three_parts = mask_author_broad & (
        names.str.split().str.len() == 3
    ) & (~names.str.contains(',', na=False))
    if mask_three_parts.any():
        # Split names
        parts_split = names[mask_three_parts].str.split()
        first_names = parts_split.str[0]
        middle_names = parts_split.str[1]
        last_names = parts_split.str[2]
        
        # Check if middle is an initial (single letter with optional period)
        mask_middle_initial = middle_names.str.match(r'^[A-Z](\.?)$', na=False)
        mask_three_initial = mask_three_parts.copy()
        mask_three_initial.loc[mask_three_parts] = mask_middle_initial
        
        if mask_three_initial.any():
            # Add period if missing
            middle_with_period = middle_names[mask_middle_initial].copy()
            mask_needs_period = ~middle_with_period.str.endswith('.')
            middle_with_period.loc[mask_needs_period] = middle_with_period.loc[mask_needs_period] + '.'
            
            # Create flipped format
            fixed_names.loc[mask_three_initial] = (
                last_names[mask_middle_initial] + ', ' + 
                first_names[mask_middle_initial] + ' ' + 
                middle_with_period
            )

            corrections.loc[mask_three_initial] = 'order flipped'
            corrections.loc[mask_three_initial & mask_needs_period] = 'period added; order flipped'

    ## Order looks fine but standalone single letter (in any position) without period
    initials_pattern = r'\b([A-Z])\b(?![.\'\-])'
    mask_has_initials = fixed_names.str.contains(initials_pattern, regex=True, na=False)

    if mask_has_initials.any():
        # Add periods to initials (work on the subset)
        names_with_periods = fixed_names[mask_has_initials].str.replace(
            initials_pattern, r'\1.', regex=True
        )
        
        # Compare within the same subset to find which ones actually changed
        mask_period_added_subset = (fixed_names[mask_has_initials] != names_with_periods)
        
        # Map back to full index
        mask_period_added = mask_has_initials.copy()
        mask_period_added.loc[mask_has_initials] = mask_period_added_subset
        
        # Update corrections for rows where periods were added
        if mask_period_added.any():
            existing_corrections = corrections[mask_period_added]
            corrections.loc[mask_period_added] = existing_corrections.apply(
                lambda x: 'period added' if x == '' else x + '; period added'
            )
        
        # Update fixed names
        fixed_names.loc[mask_has_initials] = names_with_periods

    ## Order looks fine but no space after comma
    mask_no_space = fixed_names.str.contains(r',(?=\S)', regex=True, na=False)

    if mask_no_space.any():
        names_with_space = fixed_names[mask_no_space].str.replace(r',(?=\S)', ', ', regex=True)
        
        # Append 'space added' to existing corrections
        existing_corrections = corrections[mask_no_space]
        corrections.loc[mask_no_space] = existing_corrections.apply(
            lambda x: 'space added' if x == '' else x + '; space added'
        )
        
        fixed_names.loc[mask_no_space] = names_with_space

    ## Merge back
    mask_corrected = (corrections != '') & mask_author_broad
    authors_merged.loc[mask_corrected, 'author_name_remediated'] = fixed_names[mask_corrected]
    authors_merged.loc[mask_corrected, 'author_name_action'] = corrections[mask_corrected]

    # Report results
    corrected_count = mask_corrected.sum()
    print(f'Number of author names remediated: {corrected_count}\n')
    print(authors_merged['author_name_action'].value_counts(dropna=False))

    ## Using fuzzy matching to attempt to identify permutations of same person's name 
    ### Use remediated name if present else use original
    authors_merged['author_name_temp'] = authors_merged['author_name_remediated'].fillna(authors_merged['author_name'])

    ## Sorting by length to get it to retain a longer, more detailed name (e.g., with middle initial vs. without)
    unique_names = sorted(
        authors_merged['author_name_temp'].unique(), 
        key=len, 
        reverse=True
    )
    standardized_names = {}

    ## Fuzzy matching author names
    for name in unique_names:
        if not standardized_names:
            # 100 is maximum score
            standardized_names[name] = (name, 100.0)
            continue

        result = process.extractOne(
            name, 
            standardized_names.keys(), 
            scorer=fuzz.token_sort_ratio
        )
        
        if result:
            match, score, _ = result
            if score > 85:
                standardized_names[name] = (match, score)
            else:
                standardized_names[name] = (name, 100.0)
        else:
            standardized_names[name] = (name, 100.0)

    results_map = authors_merged['author_name_temp'].map(standardized_names)

    authors_merged['author_name_remediated_standardized'] = results_map.apply(lambda x: x[0])
    authors_merged['match_score'] = results_map.apply(lambda x: x[1])

# ============================================
# ORCID formatting
# ============================================
if recurate_orcid:
    ### Not just using 'flag_orcid' in order to also catch ones that have ORCID but not full complementary fields
    mask_orcid_broad = (authors_merged['author_identifier_scheme'] == 'ORCID') | \
                (authors_merged['author_identifier_scheme'].isna())

    print(f'There are {mask_orcid_broad.sum()} of {len(authors_merged)} authors to check for ORCID presence and/or formatting.\n')

    ## Create remediated columns for ALL rows in df
    authors_merged['orcid_remediated'] = pd.NA
    authors_merged['orcid_action'] = pd.NA
    authors_merged['author_identifier_scheme_remediated'] = ''
    authors_merged['author_identifier_expanded_remediated'] = ''

    ## Create compound mask for remediation
    mask_orcid_target = mask_orcid_broad & authors_merged['author_identifier'].notna()

    # Get the identifier column as string for pattern matching
    orcid_str = authors_merged.loc[mask_orcid_target, 'author_identifier'].astype(str).str.strip()

    ## Target patterns
    proper_pattern = r'^https://orcid\.org/\d{4}-\d{4}-\d{4}-\d{3}[\dXx]$'
    url_space_pattern = r'^https://orcid\.org/\s+\d{4}-\d{4}-\d{4}-\d{3}[\dXx]$'    # space after URL shoulder
    incomplete_pattern = r'^\d{3}-\d{4}-\d{4}-\d{3}[\dXx]$'                         # missing leading 0
    hyphen_pattern = r'^\d{4}-\d{4}-\d{4}-\d{3}[\dXx]$'                             
    digits_pattern = r'^\d{15}[\dXx]$'                                              # missing hyphens
    http_pattern = r'^http://orcid\.org/\d{4}-\d{4}-\d{4}-\d{3}[\dXx]$' 
    http_space_pattern = r'^http://orcid\.org/\s+\d{4}-\d{4}-\d{4}-\d{3}[\dXx]$' 
    missing_protocol_pattern = r'^orcid\.org/\d{4}-\d{4}-\d{4}-\d{3}[\dXx]$'

    ## Hyphenated and with URL but with space in URL
    mask_space = mask_orcid_target & orcid_str.str.match(url_space_pattern, na=False)
    authors_merged.loc[mask_space, 'orcid_remediated'] = orcid_str[mask_space].str.replace(r'\s+', '', regex=True)
    authors_merged.loc[mask_space, 'author_identifier_expanded_remediated'] = authors_merged.loc[mask_space, 'orcid_remediated']
    authors_merged.loc[mask_space, 'author_identifier_scheme_remediated'] = 'ORCID'
    authors_merged.loc[mask_space, 'orcid_action'] = 'removed space; added schema'

    ## Hyphenated but without URL
    ## This is short-form and not a problem per-se (this is actually the only one that cross-walks to DC right now), but standardization can't hurt, and it does add to the 'expanded identifier' field
    mask_hyphen = mask_orcid_target & orcid_str.str.match(hyphen_pattern, na=False)
    authors_merged.loc[mask_hyphen, 'orcid_remediated'] = 'https://orcid.org/' + orcid_str[mask_hyphen]
    authors_merged.loc[mask_hyphen, 'orcid_action'] = 'added url'

    ## Missing protocol (orcid.org/XXXX-XXXX-XXXX-XXXX)
    mask_no_protocol = mask_orcid_target & orcid_str.str.match(missing_protocol_pattern, na=False)
    authors_merged.loc[mask_no_protocol, 'orcid_remediated'] = 'https://' + orcid_str[mask_no_protocol]
    authors_merged.loc[mask_no_protocol, 'author_identifier_expanded_remediated'] = authors_merged.loc[mask_no_protocol, 'orcid_remediated']
    authors_merged.loc[mask_no_protocol, 'author_identifier_scheme_remediated'] = 'ORCID'
    authors_merged.loc[mask_no_protocol, 'orcid_action'] = 'added protocol'

    ## Neither hyphenated nor with URL
    mask_digits = mask_orcid_target & orcid_str.str.match(digits_pattern, na=False)
    formatted_orcids = (
        orcid_str[mask_digits].str[:4] + '-' +
        orcid_str[mask_digits].str[4:8] + '-' +
        orcid_str[mask_digits].str[8:12] + '-' +
        orcid_str[mask_digits].str[12:]
    )
    authors_merged.loc[mask_digits, 'orcid_remediated'] = 'https://orcid.org/' + formatted_orcids
    authors_merged.loc[mask_digits, 'orcid_action'] = 'added_url and hyphens'

    # Populate scheme and expanded fields for remediated ORCIDs
    mask_remediated = authors_merged['orcid_remediated'].notna()
    authors_merged.loc[mask_remediated, 'author_identifier_expanded_remediated'] = authors_merged.loc[mask_remediated, 'orcid_remediated']
    authors_merged.loc[mask_remediated, 'author_identifier_scheme_remediated'] = 'ORCID'

    # Edge case: ORCID is properly hyperlinked but not fully expanded/rooted in schema
    mask_edge = (
        authors_merged['author_identifier'].str.contains('https://orcid.org/0', na=False) & 
        (authors_merged['author_identifier_scheme'].isna() | 
        (authors_merged['author_identifier_expanded'].isna()))
    )
    authors_merged.loc[mask_edge, 'orcid_remediated'] = authors_merged.loc[mask_edge, 'author_identifier']
    authors_merged.loc[mask_edge, 'author_identifier_expanded_remediated'] = authors_merged.loc[mask_edge, 'author_identifier']
    authors_merged.loc[mask_edge, 'author_identifier_scheme_remediated'] = 'ORCID'
    authors_merged.loc[mask_edge, 'orcid_action'] = 'added schema'

    # Edge case: scheme is marked as ORCID but ORCID is missing a 0 in front and is not hyperlinked
    mask_incomplete = mask_orcid_target & orcid_str.str.match(incomplete_pattern, na=False)
    authors_merged.loc[mask_incomplete, 'orcid_remediated'] = 'https://orcid.org/0' + orcid_str[mask_incomplete]
    authors_merged.loc[mask_incomplete, 'orcid_action'] = 'fixed incomplete ORCID'
    authors_merged.loc[mask_incomplete, 'author_identifier_expanded_remediated'] = 'https://orcid.org/0' + orcid_str[mask_incomplete]
    authors_merged.loc[mask_incomplete, 'author_identifier_scheme_remediated'] = 'ORCID'

    # Edge case: ORCID is hyperlinked but only with http:// not https://
    ## Straight conversion
    mask_http = mask_orcid_target & orcid_str.str.match(http_pattern, na=False)
    authors_merged.loc[mask_http, 'orcid_remediated'] = orcid_str[mask_http].str.replace('http://', 'https://', regex=False)
    authors_merged.loc[mask_http, 'orcid_action'] = 'changed_http to https'

    ## Also has space inside
    mask_http_space = mask_orcid_target & orcid_str.str.match(http_space_pattern, na=False)
    authors_merged.loc[mask_http_space, 'orcid_remediated'] = (
        orcid_str[mask_http_space].str.replace(r'\s+', '', regex=True).str.replace('http://', 'https://', regex=False)
    )
    authors_merged.loc[mask_http_space, 'orcid_action'] = 'changed_http to https; removed_space'

    ## Also not in expanded schema
    mask_edge_http = (
        authors_merged['author_identifier'].str.contains('http://orcid.org/', na=False) & 
        (authors_merged['author_identifier_scheme'].isna() | 
        (authors_merged['author_identifier_expanded'].isna())) &
        authors_merged['orcid_remediated'].isna()
    )
    authors_merged.loc[mask_edge_http, 'orcid_remediated'] = (
        authors_merged.loc[mask_edge_http, 'author_identifier'].str.replace('http://', 'https://', regex=False)
    )
    authors_merged.loc[mask_edge_http, 'author_identifier_expanded_remediated'] = (
        authors_merged.loc[mask_edge_http, 'author_identifier'].str.replace('http://', 'https://', regex=False)
    )
    authors_merged.loc[mask_edge_http, 'author_identifier_scheme_remediated'] = 'ORCID'
    authors_merged.loc[mask_edge_http, 'orcid_action'] = 'changed_http_to_https; added schema'

    # Edge case: ORCID is properly formatted and hyperlinked but name is in front of it
    orcid_pattern = r'https://orcid\.org/\d{4}-\d{4}-\d{4}-\d{3}[\dXx]'
    mask_invalid_orcid = (
        authors_merged['author_identifier'].str.contains(orcid_pattern, regex=True, na=False) &
        ~authors_merged['author_identifier'].str.startswith('https://', na=False)
    )
    ## Overwrites any previous value
    extracted = authors_merged.loc[mask_invalid_orcid, 'author_identifier'].str.extract(f'({orcid_pattern})', expand=False).copy()
    authors_merged.loc[mask_invalid_orcid, 'orcid_remediated'] = extracted
    authors_merged.loc[mask_invalid_orcid, 'author_identifier_expanded_remediated'] = extracted
    authors_merged.loc[mask_invalid_orcid, 'author_identifier_scheme_remediated'] = 'ORCID'
    authors_merged.loc[mask_invalid_orcid, 'orcid_action'] = 'removed leading characters; added schema'

    fixed_count = authors_merged['orcid_remediated'].notna().sum()
    print(f'Number of ORCID entries remediated: {fixed_count}\n')
    print(authors_merged['orcid_action'].value_counts(dropna=False))

# ============================================
# ORCID inference
# ============================================
if recurate_orcid:
    # Create column with all ORCIDs
    ### Use remediated name if present else use original
    authors_merged['orcid_url'] = authors_merged['orcid_remediated']
    mask_orcid_original = (authors_merged['orcid_url'].isna() & (authors_merged['author_identifier_scheme'] == 'ORCID'))

    # Only use author_identifier if it matches a valid ORCID pattern (complete format)

    mask_orcid_original_valid = (mask_orcid_original & authors_merged['author_identifier'].str.contains(proper_pattern, na=False, regex=True))
    authors_merged.loc[mask_orcid_original_valid, 'orcid_url'] = (authors_merged.loc[mask_orcid_original_valid, 'author_identifier'])

    # Initialize columns
    authors_merged['inferred_orcid'] = pd.NA
    authors_merged['inferred_basis'] = pd.NA

    # ============================================
    # Inference 1: By shared name + email
    # ============================================
    # Group and get unique non-null ORCIDs per group based on listed columns
    grouped = authors_merged.groupby(['author_name', 'dataset_email'])['orcid_url']

    # Count unique non-null ORCIDs per group
    unique_counts = grouped.transform(lambda x: x.dropna().nunique())

    # Get the first non-null ORCID per group (for single unique case)
    first_orcid = grouped.transform(lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else pd.NA)

    # If  there is only one ORCID in the group, okay to propagate
    mask_single = (unique_counts == 1) & authors_merged['orcid_url'].isna() & (authors_merged['author_count'] == 1)
    # If there are multiple ORCIDs in the group, return a warning flag
    mask_multiple = (unique_counts > 1) & authors_merged['orcid_url'].isna()

    authors_merged.loc[mask_single, 'inferred_orcid'] = first_orcid[mask_single]
    authors_merged.loc[mask_single, 'inferred_basis'] = 'shared original name and contact email'

    authors_merged.loc[mask_multiple, 'inferred_basis'] = 'WARNING'

    # ============================================
    # Inference 2: By shared name + dataverse + date
    # ============================================
    # Only for rows still missing inferred ORCID
    mask_still_missing = authors_merged['inferred_orcid'].isna()

    if mask_still_missing.any():
        grouped2 = authors_merged.groupby(['author_name', 'dataverse', 'publication_date'])['orcid_url']
        
        unique_counts2 = grouped2.transform(lambda x: x.dropna().nunique())
        first_orcid2 = grouped2.transform(lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else pd.NA)
        
        mask_single2 = mask_still_missing & (unique_counts2 == 1) & authors_merged['orcid_url'].isna()
        mask_multiple2 = mask_still_missing & (unique_counts2 > 1) & authors_merged['orcid_url'].isna()
        
        authors_merged.loc[mask_single2, 'inferred_orcid'] = first_orcid2[mask_single2]
        authors_merged.loc[mask_single2, 'inferred_basis'] = 'shared original name, dataverse, and publication date'
        
        authors_merged.loc[mask_multiple2, 'inferred_orcid'] = 'WARNING'
        authors_merged.loc[mask_multiple2, 'inferred_basis'] = 'WARNING'

    # ===============================================
    # Inference 3: By shared reformatted name + email
    # ===============================================
    mask_still_missing = authors_merged['inferred_orcid'].isna()

    if mask_still_missing.any(): 
        grouped3 = authors_merged[mask_still_missing].groupby(['author_name_temp', 'dataset_email'])['orcid_url']

        unique_counts3 = grouped3.transform(lambda x: x.dropna().nunique())
        first_orcid3 = grouped3.transform(lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else pd.NA)

        mask_single3 = mask_still_missing & (unique_counts3 == 1) & authors_merged['orcid_url'].isna()
        mask_multiple3 = mask_still_missing & (unique_counts3 > 1) & authors_merged['orcid_url'].isna()

        authors_merged.loc[mask_single3, 'inferred_orcid'] = first_orcid3[mask_single3]
        authors_merged.loc[mask_single3, 'inferred_basis'] = 'shared reformatted name and contact email'

        authors_merged.loc[mask_multiple3, 'inferred_orcid'] = 'WARNING'
        authors_merged.loc[mask_multiple3, 'inferred_basis'] = 'WARNING'

    # ===============================================
    # Inference 4: By shared standardized name + email
    # ===============================================
    mask_still_missing = authors_merged['inferred_orcid'].isna()

    if mask_still_missing.any(): 
        grouped4 = authors_merged[mask_still_missing].groupby(['author_name_remediated_standardized', 'dataset_email'])['orcid_url']

        unique_counts4 = grouped4.transform(lambda x: x.dropna().nunique())
        first_orcid4 = grouped4.transform(lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else pd.NA)

        mask_single4 = mask_still_missing & (unique_counts4 == 1) & authors_merged['orcid_url'].isna()
        mask_multiple4 = mask_still_missing & (unique_counts4 > 1) & authors_merged['orcid_url'].isna()

        authors_merged.loc[mask_single4, 'inferred_orcid'] = first_orcid4[mask_single4]
        authors_merged.loc[mask_single4, 'inferred_basis'] = 'shared standardized name and contact email'

        authors_merged.loc[mask_multiple4, 'inferred_orcid'] = 'WARNING'
        authors_merged.loc[mask_multiple4, 'inferred_basis'] = 'WARNING'

    # ================================================
    # Inference 5: By shared name and parent dataverse
    # ================================================
    mask_still_missing = authors_merged['inferred_orcid'].isna()

    if mask_still_missing.any(): 
        grouped5 = authors_merged[mask_still_missing].groupby(['author_name', 'parent_dataverse'])['orcid_url']

        unique_counts5 = grouped5.transform(lambda x: x.dropna().nunique())
        first_orcid5 = grouped5.transform(lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else pd.NA)

        # Exclude if parent dataverse is top-level
        mask_not_ut = authors_merged['parent_dataverse'] != 'University of Texas at Austin Dataverse Collection'

        mask_single5 = mask_still_missing & (unique_counts5 == 1) & authors_merged['orcid_url'].isna() & mask_not_ut
        mask_multiple5 = mask_still_missing & (unique_counts5 > 1) & authors_merged['orcid_url'].isna() & mask_not_ut

        authors_merged.loc[mask_single5, 'inferred_orcid'] = first_orcid5[mask_single5]
        authors_merged.loc[mask_single5, 'inferred_basis'] = 'shared original name and parent dataverse'

        authors_merged.loc[mask_multiple5, 'inferred_orcid'] = 'WARNING'
        authors_merged.loc[mask_multiple5, 'inferred_basis'] = 'WARNING'

    # ================================================
    # Inference 6: By shared reformatted name and parent dataverse
    # ================================================
    mask_still_missing = authors_merged['inferred_orcid'].isna()

    if mask_still_missing.any(): 
        grouped6 = authors_merged[mask_still_missing].groupby(['author_name_temp', 'parent_dataverse'])['orcid_url']

        unique_counts6 = grouped6.transform(lambda x: x.dropna().nunique())
        first_orcid6 = grouped6.transform(lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else pd.NA)

        mask_not_ut = authors_merged['parent_dataverse'] != 'University of Texas at Austin Dataverse Collection'

        mask_single6 = mask_still_missing & (unique_counts6 == 1) & authors_merged['orcid_url'].isna() & mask_not_ut
        mask_multiple6 = mask_still_missing & (unique_counts6 > 1) & authors_merged['orcid_url'].isna() & mask_not_ut

        authors_merged.loc[mask_single6, 'inferred_orcid'] = first_orcid6[mask_single6]
        authors_merged.loc[mask_single6, 'inferred_basis'] = 'shared reformatted name and parent dataverse'

        authors_merged.loc[mask_multiple6, 'inferred_orcid'] = 'WARNING'
        authors_merged.loc[mask_multiple6, 'inferred_basis'] = 'WARNING'

    # ================================================
    # Inference 7: By shared reformatted name and parent dataverse
    # ================================================
    mask_still_missing = authors_merged['inferred_orcid'].isna()

    if mask_still_missing.any(): 
        grouped7 = authors_merged[mask_still_missing].groupby(['author_name_remediated_standardized', 'parent_dataverse'])['orcid_url']

        unique_counts7 = grouped7.transform(lambda x: x.dropna().nunique())
        first_orcid7 = grouped7.transform(lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else pd.NA)

        mask_not_ut = authors_merged['parent_dataverse'] != 'University of Texas at Austin Dataverse Collection'

        mask_single7 = mask_still_missing & (unique_counts7 == 1) & authors_merged['orcid_url'].isna() & mask_not_ut
        mask_multiple7 = mask_still_missing & (unique_counts7 > 1) & authors_merged['orcid_url'].isna() & mask_not_ut

        authors_merged.loc[mask_single7, 'inferred_orcid'] = first_orcid7[mask_single7]
        authors_merged.loc[mask_single7, 'inferred_basis'] = 'shared standardized name and parent dataverse'

        authors_merged.loc[mask_multiple7, 'inferred_orcid'] = 'WARNING'
        authors_merged.loc[mask_multiple7, 'inferred_basis'] = 'WARNING'

    ### shared name and dataverse
    ### shared (standardized) name, affiliation, and parent dataverse

    # Add Boolean columns
    if recurate_ror:
        authors_merged['fix_ror'] = authors_merged['ror'].notna()
    if recurate_orcid:
        authors_merged['fix_orcid'] = (authors_merged['orcid_remediated'].notna() | authors_merged['inferred_orcid'].notna())
    if recurate_names:
        authors_merged['fix_name'] = authors_merged['author_name_remediated'].notna()

    added_ror_count = authors_merged['ror'].count()
    print(f'Number of author entries that had ROR added: {added_ror_count}.\n')
    fixed_orcid_count = authors_merged['orcid_remediated'].count()
    print(f'Number of author entries that had the ORCID fixed: {fixed_orcid_count}.\n')
    inferred_orcid_count = authors_merged['inferred_orcid'].count()
    print(f'Number of author entries that had ORCID inferred: {inferred_orcid_count}.\n')

authors_merged.to_csv(f'{outputs_dir}/{today}_{my_institution_filename}_final-authors-remediated.csv', index=False, encoding='utf-8-sig')

## Create single combined df at author-level but with indication of dataset-level fixes
### Prune out columns to keep it clean
columns_to_drop = ['current_status_x', 'author_count', 'flagged_any', 'institution', 'dataset_id', 'version_id', 'total_version','parent_dataverse', 'keywords', 'description', 'license',	'related_works_citations', 'related_works_dois', 'related_works_urls', 'author_name_temp', 'author_name_remediated_standardized', 'match_score']
authors_merged_pruned = authors_merged.drop(columns_to_drop, axis=1)
# Explicitly keep only doi, current_status, dataset-level fix/remediated cols, and review flag cols
# This avoids duplicate columns in the merge caused by overlap with authors_merged_pruned
_dataset_keep = ['doi', 'current_status']
_dataset_keep += [c for c in ['keywords_remediated', 'dataset_title_remediated', 'fix_keywords', 'fix_title',
                               'grant_rors_remediated', 'grant_agencies_remediated', 'fix_funder_ror']
                  if c in datasets_flagged_retained.columns]
_dataset_keep += [c for c in datasets_flagged_retained.columns
                  if c.startswith('flag_work') or c.startswith('flag_license')]
datasets_flagged_retained_pruned = datasets_flagged_retained[_dataset_keep]

combined = pd.merge(authors_merged_pruned, datasets_flagged_retained_pruned, on='doi', how='left')
fixed_cols = [col for col in combined.columns if col.startswith('fix_')]
if fixed_cols:
    combined['fixed'] = combined[fixed_cols].any(axis=1)

review_terms = ['flag_work', 'flag_license']
review_cols = [col for col in combined.columns if col.startswith(tuple(review_terms))]
if review_cols:
    combined['to_review'] = combined[review_cols].any(axis=1)

combined.to_csv(f'{outputs_dir}/{today}_{my_institution_filename}_final-combined-remediated.csv', index=False, encoding='utf-8-sig')

fixed_authors = combined["fixed"].sum()
print(f'Number of authors who had a metadata field fixed: {fixed_authors}.\n')
combined_sorted = combined.sort_values(by='fixed', ascending=False)
combined_sorted = combined_sorted.drop_duplicates(subset=['doi'])
fixed_datasets = combined_sorted["fixed"].sum()
print(f'Number of datasets that had a metadata field fixed: {fixed_datasets}.\n')
to_review_datasets = combined_sorted["to_review"].sum()
print(f'Number of datasets that need licensing or related work checks: {to_review_datasets}.\n')

print(f'Done\n---Time to run: {datetime.now() - start_time}---\n')