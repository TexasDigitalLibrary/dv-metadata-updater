import pandas as pd
import requests
import sys

# ============================================
#               WORKFLOW SET-UP
# ============================================

df = pd.read_csv('affiliation-map-primary.csv')
df_clean = df.drop_duplicates(subset=['ror']).dropna(subset=['ror'])
# Identify only unmatched ROR affiliations
df_clean_unmatched = df_clean[(df_clean['ror'].notna() & df_clean['official_name'].isna())]

if df_clean_unmatched.empty:
    print('The dataframe is empty, exiting script.\n')
    # Exits the program with an exit code of 1 (indicating an error)
    sys.exit(1) 
else:
    print(f'Retrieving ROR metadata on {len(df_clean_unmatched)} affiliations.\n')

# ============================================
#              METADATA RETRIEVAL
# ============================================

ror_url = 'https://api.ror.org/v2/organizations/'

results = []
for ror in df_clean_unmatched['ror']:
    try:
        response = requests.get(f'{ror_url}{ror}', timeout=5)
        if response.status_code == 200:
            print(f'Retrieving {ror}\n')
            results.append(response.json())
        else:
           print(f'Error retrieving {ror}, Status {response.status_code}')
    except Exception as e:
        print(f'Error retrieving {ror}')

data_ror = {'institutions': results}

data_ror_select = [] 
for item in data_ror['institutions']:
    id = item.get('id', '')
    names = item.get('names', '')
    for name in names:
        if 'ror_display' in name.get('types', []):
            official_name = name.get('value', '')
            author_entry = {
                    'ror': id,
                    'official_name': official_name
                }
            data_ror_select.append(author_entry)

df_data_ror_select = pd.json_normalize(data_ror_select)

# Merge back with original df
merged = pd.merge(df, df_data_ror_select, on='ror', how='left')
combined = df.merge(df_data_ror_select[['ror', 'official_name']], on='ror',how='left',suffixes=('_old', '_new'))
combined['official_name'] = combined['official_name_new'].fillna(combined['official_name_old'])
combined = combined.drop(['official_name_old', 'official_name_new'], axis=1)
combined.to_csv('affiliation-map_enriched.csv', index=False, encoding='utf-8-sig')