import json
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import os
import pandas as pd
import re
import shutil
from datetime import datetime
from utils import env_bool, is_recent_file, load_most_recent_file, load_nth_most_recent_file, load_file_by_date

# ============================================
#               WORKFLOW SET-UP
# ============================================

# Timestamp to calculate run time
start_time = datetime.now() 
# Current date for filenames
today = datetime.now().strftime('%Y%m%d') 

# Config file
with open('config.json', 'r') as file:
    config = json.load(file)

# Test environment (only for finding the right data)
test = env_bool('TEST_ENVIRONMENT')
# Whether to compare pre- and post-re-curation metrics
time_analysis = env_bool('TIMEFRAME_ANALYSIS')

# Get directories
script_dir = os.getcwd()
if test:
    outputs_dir = os.path.join(script_dir, 'test/outputs')
    if os.path.isdir('plots'):
        print('plots directory found - no need to recreate\n')
    else:
        os.mkdir('plots')
        print('plots directory has been created\n')
    plots_dir = os.path.join(script_dir, 'test/plots')
else:
    outputs_dir = os.path.join(script_dir, 'outputs')
    if os.path.isdir('plots'):
        print('plots directory found - no need to recreate\n')
    else:
        os.mkdir('plots')
        print('plots directory has been created\n')
    plots_dir = os.path.join(script_dir, 'plots')

# Move all older graphs to new folder
if os.path.isdir("plots/old-plots"):
    print("old plots directory found - no need to recreate\n")
else:
    os.mkdir("plots/old-plots")
    print("old plots directory has been created\n")
#move plots not created today to that folder
for filename in os.listdir('plots'):
    if os.path.isfile(os.path.join('plots', filename)) and not is_recent_file(filename):
        shutil.move(os.path.join('plots', filename), os.path.join('plots/old-plots', filename))
print('Plots older than 14 days have been moved to the old-plots subdirectory.\n')

# ============================================
#               FILE LOAD-IN
# ============================================

# Load most recent version of dataset-authors file
pattern = '_all-datasets-authors-PUBLISHED'
datasets_post = load_most_recent_file(outputs_dir, pattern)

# Load second most recent version of dataset-authors file
datasets_pre = load_nth_most_recent_file(outputs_dir, pattern, n=2)

# Load most recent version of authors file
pattern = '_all-authors-datasets-PUBLISHED'
authors_post = load_most_recent_file(outputs_dir, pattern)

# Load second most recent version of authors file
authors_pre = load_nth_most_recent_file(outputs_dir, pattern, n=2)

# ORCID format classification (correctly formatted entries only)
## This step is only necessary for reproducing the plots associated with the manuscript
_short = re.compile(r'^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$')
_long  = re.compile(r'^https?://orcid\.org/\d{4}-\d{4}-\d{4}-\d{3}[\dX]$')
_id = authors_post['author_identifier'].fillna('').str.strip()
authors_post['orcid_format'] = (
    _id.map(lambda v: 'none' if v == '' else 'short form' if _short.match(v) else 'long form' if _long.match(v) else 'not recognized')
)
authors_post['orcid_valid'] = authors_post['orcid_format'].isin(['short form', 'long form'])
_id = authors_pre['author_identifier'].fillna('').str.strip()
authors_pre['orcid_format'] = (
    _id.map(lambda v: 'none' if v == '' else 'short form' if _short.match(v) else 'long form' if _long.match(v) else 'not recognized')
)
authors_pre['orcid_valid'] = authors_pre['orcid_format'].isin(['short form', 'long form'])

# Only retain datasets published prior to re-curation began
datasets_post_filtered = datasets_post[datasets_post['doi'].isin(datasets_pre['doi'])]
## Same thing with author-level dataset
authors_post_filtered = authors_post[authors_post['doi'].isin(authors_pre['doi'])]

# List of depositor/contact names to omit from select re-curation flagging
excluded_people = config['EXCLUDED']
excluded_people_set = set(excluded_people)

# ============================================
#               DATASET SUMMARY
# ============================================

datasets_dict = {
    'pre': datasets_pre,
    'post': datasets_post_filtered
}

# Store results
results_datasets = {}

for label, df in datasets_dict.items():
    # Filter out excluded people
    df_retained = df[
        ~df['dataset_depositor'].isin(excluded_people_set) &
        ~df['dataset_contact'].isin(excluded_people_set)
    ]
    
    dataset_count = len(df_retained)
    
    # Count citations
    citation_count = df_retained['related_works_citations'].notna().sum()
    citation_proportion = round((citation_count / dataset_count), 3)
    # Count publication identifiers
    identifier_count = df_retained['related_works_dois'].notna().sum()
    identifier_proportion = round((identifier_count / dataset_count), 3)
    # Count publication URLs
    url_count = df_retained['related_works_urls'].notna().sum()
    url_proportion = round((url_count / dataset_count), 3)
    # Title formatting
    ## One for both
    title_count = (df_retained[['flag_title_space', 'flag_title_period']]).any(axis=1).sum()
    title_count_reversed = dataset_count - title_count
    title_proportion = round((title_count_reversed / dataset_count), 3)
    # Keyword formatting
    keyword_count = (~df_retained['flag_keyword']).sum()
    keyword_proportion = round((keyword_count / dataset_count), 3)
    # Aggregate author summaries
    ## ORCID
    orcid_count = (df_retained['flag_orcid']).sum()
    orcid_count_reversed = dataset_count - orcid_count
    orcid_proportion = round((orcid_count_reversed / dataset_count), 3)
    ## ROR
    ### Has to use == because column is not Boolean due to blanks
    ror_count = (df_retained['flag_ror'] == False).sum()
    ror_proportion = round((ror_count / dataset_count), 3)
    
    # Store results
    results_datasets[label] = {
        'dataframe': df_retained,
        'count': dataset_count,
        'citation_count': citation_count,
        'citation_proportion': citation_proportion,
        'identifier_count': identifier_count,
        'identifier_proportion': identifier_proportion,
        'url_count': url_count,
        'url_proportion': url_proportion,
        'title_count': title_count_reversed,
        'title_proportion': title_proportion,
        'keyword_count': keyword_count,
        'keyword_proportion': keyword_proportion,
        'orcid_count': orcid_count_reversed,
        'orcid_proportion': orcid_proportion,
        'ror_count': ror_count,
        'ror_proportion': ror_proportion
    }
    
    # Print results
    print(f'Total number of in-scope datasets ({label}): {dataset_count}.\n')
    print(f'Datasets with a related citation ({label}): {citation_count}')
    print(f'Proportion with citation ({label}): {citation_proportion}')
    print(f'Datasets with a related identifier ({label}): {identifier_count}')
    print(f'Proportion with identifier ({label}): {identifier_proportion}')
    print(f'Datasets with a related URL ({label}): {url_count}')
    print(f'Proportion with URL ({label}): {url_proportion}\n')

# Print comparison table for both
print('=' * 80)
print(f'{"Metric":<40} {"Pre":<20} {"Post":<20}')
print('=' * 80)

print(f'{"Total number of in-scope datasets":<40} {results_datasets["pre"]["count"]:<20} {results_datasets["post"]["count"]:<20}')
print()



print(f'{"Datasets with a citation field":<40} {results_datasets["pre"]["citation_count"]:<20} {results_datasets["post"]["citation_count"]:<20}')
print(f'{"Proportion with a citation field":<40} {results_datasets["pre"]["citation_proportion"]:<20} {results_datasets["post"]["citation_proportion"]:<20}')
print()

print(f'{"Datasets with an identifier":<40} {results_datasets["pre"]["identifier_count"]:<20} {results_datasets["post"]["identifier_count"]:<20}')
print(f'{"Proportion with identifier":<40} {results_datasets["pre"]["identifier_proportion"]:<20} {results_datasets["post"]["identifier_proportion"]:<20}')
print()

print(f'{"Datasets with a URL":<40} {results_datasets["pre"]["url_count"]:<20} {results_datasets["post"]["url_count"]:<20}')
print(f'{"Proportion with a URL":<40} {results_datasets["pre"]["url_proportion"]:<20} {results_datasets["post"]["url_proportion"]:<20}')
print()

print(f'{"Datasets with a properly formatted title":<40} {results_datasets["pre"]["title_count"]:<20} {results_datasets["post"]["title_count"]:<20}')
print(f'{"Proportion with properly formatted title":<40} {results_datasets["pre"]["title_proportion"]:<20} {results_datasets["post"]["title_proportion"]:<20}')
print()

print(f'{"Datasets with properly formatted keywords":<40} {results_datasets["pre"]["keyword_count"]:<20} {results_datasets["post"]["keyword_count"]:<20}')
print(f'{"Proportion with properly formatted keyword":<40} {results_datasets["pre"]["keyword_proportion"]:<20} {results_datasets["post"]["keyword_proportion"]:<20}')
print()

print(f'{"Datasets with fully formatted ORCIDs":<40} {results_datasets["pre"]["orcid_count"]:<20} {results_datasets["post"]["orcid_count"]:<20}')
print(f'{"Proportion with fully formatted ORCIDs":<40} {results_datasets["pre"]["orcid_proportion"]:<20} {results_datasets["post"]["orcid_proportion"]:<20}')
print()

print(f'{"Datasets with no missing RORs":<40} {results_datasets["pre"]["ror_count"]:<20} {results_datasets["post"]["ror_count"]:<20}')
print(f'{"Proportion with no missing RORs":<40} {results_datasets["pre"]["ror_proportion"]:<20} {results_datasets["post"]["ror_proportion"]:<20}')
print('=' * 80)
print()

# ============================================
#               AUTHOR SUMMARY
# ============================================

authors_dict = {
    'pre': authors_pre,
    'post': authors_post_filtered
}

# Store results
results_authors = {}

for label, df in authors_dict.items():
    # Filter out excluded people
    df_retained = df[
        ~df['dataset_depositor'].isin(excluded_people_set) &
        ~df['dataset_contact'].isin(excluded_people_set)
    ]
    
    author_count = len(df_retained)
    
    # Count ROR
    ror_count = (~df_retained['flag_ror']).sum()
    ror_proportion = round((ror_count / author_count), 3)
    # Count ORCID
    orcid_count = df_retained['author_identifier'].notna().sum()
    orcid_proportion = round((orcid_count / author_count), 3)
    # Count proper ORCID
    orcid_proper_count = df_retained['author_identifier_expanded'].str.contains('https://orcid.org/00').sum()
    orcid_proper_proportion = round((orcid_proper_count / author_count), 3)
    # Count valid ORCID
    orcid_valid_count = df_retained['orcid_valid'].sum()
    orcid_valid_proportion = round((orcid_valid_count / author_count), 3)
    # Count missing ORCID
    orcid_missing_count = df_retained['missing_orcid'].sum()
    orcid_missing_count_reversed = author_count - orcid_missing_count
    orcid_missing_proportion = round((orcid_missing_count_reversed / author_count), 3)
    # Count malformed names
    ## Wrong order
    author_order_count = (~df_retained['malformed_name_order']).sum()
    author_order_proportion = round((author_order_count / author_count), 3)
    ## Missing period after initial
    author_initial_count = (~df_retained['malformed_name_initial']).sum()
    author_initial_proportion = round((author_initial_count / author_count), 3)

    # Store results
    results_authors[label] = {
        'dataframe': df_retained,
        'author_count': author_count,
        'ror_count': ror_count,
        'ror_proportion': ror_proportion,
        'identifier_count': orcid_count,
        'identifier_proportion': orcid_proportion,
        'orcid_count': orcid_proper_count,
        'orcid_proportion': orcid_proper_proportion,
        'orcid_valid_count': orcid_valid_count,
        'orcid_valid_proportion': orcid_valid_proportion,
        'orcid_missing_count': orcid_missing_count,
        'orcid_missing_proportion': orcid_missing_proportion,
        'author_order_count': author_order_count,
        'author_order_proportion': author_order_proportion,
        'author_initial_count': author_initial_count,
        'author_initial_proportion': author_initial_proportion
    }

# Print comparison table for both
print('=' * 80)
print(f'{"Metric":<40} {"Pre":<20} {"Post":<20}')
print('=' * 80)

print(f'{"Total number of in-scope authors":<40} {results_authors["pre"]["author_count"]:<20} {results_authors["post"]["author_count"]:<20}')
print()

print(f'{"Authors with ROR-matched affiliation":<40} {results_authors["pre"]["ror_count"]:<20} {results_authors["post"]["ror_count"]:<20}')
print(f'{"Proportion with ROR":<40} {results_authors["pre"]["ror_proportion"]:<20} {results_authors["post"]["ror_proportion"]:<20}')
print()

print(f'{"Authors with an identifier":<40} {results_authors["pre"]["identifier_count"]:<20} {results_authors["post"]["identifier_count"]:<20}')
print(f'{"Proportion with identifier":<40} {results_authors["pre"]["identifier_proportion"]:<20} {results_authors["post"]["identifier_proportion"]:<20}')
print()

print(f'{"Authors with a proper ORCID":<40} {results_authors["pre"]["orcid_count"]:<20} {results_authors["post"]["orcid_count"]:<20}')
print(f'{"Proportion with proper ORCID":<40} {results_authors["pre"]["orcid_proportion"]:<20} {results_authors["post"]["orcid_proportion"]:<20}')
print()

print(f'{"Authors with name format in 'Last, First'":<40} {results_authors["pre"]["author_order_count"]:<20} {results_authors["post"]["author_order_count"]:<20}')
print(f'{"Proportion with name in format":<40} {results_authors["pre"]["author_order_proportion"]:<20} {results_authors["post"]["author_order_proportion"]:<20}')
print()

print(f'{"Authors with proper initial punctuation":<40} {results_authors["pre"]["author_initial_count"]:<20} {results_authors["post"]["author_initial_count"]:<20}')
print(f'{"Proportion with proper initial punctuation":<40} {results_authors["pre"]["author_initial_proportion"]:<20} {results_authors["post"]["author_initial_proportion"]:<20}')
print('=' * 80)
print()

# ============================================
#               GRAPHS
# ============================================


# List the proportions
## Order matters! The order in the two lists needs to be the same.
metric_names = ['Publication (identifier)', 'Publication (URL)', 'Publication (citation)', 'All authors w/ RORs', 'All authors w/ ORCIDs', 'Keyword (proper format)', 'Title (proper format)']
pre_proportions = [
    results_datasets['pre']['identifier_proportion'],
    results_datasets['pre']['url_proportion'],
    results_datasets['pre']['citation_proportion'],
    results_datasets['pre']['ror_proportion'],
    results_datasets['pre']['orcid_proportion'],
    results_datasets['pre']['keyword_proportion'],
    results_datasets['pre']['title_proportion']
]
post_proportions = [
    results_datasets['post']['identifier_proportion'],
    results_datasets['post']['url_proportion'],
    results_datasets['post']['citation_proportion'],
    results_datasets['post']['ror_proportion'],
    results_datasets['post']['orcid_proportion'],
    results_datasets['post']['keyword_proportion'],
    results_datasets['post']['title_proportion']
]

plot_filename = f"{today}_dataset-level-summary.png"
fig, ax = plt.subplots(figsize=(8, 6))

y_positions = np.arange(len(metric_names))

# Offset the circle to show arrowheads
circle_offset = 0.02

# Draw connector lines with arrows - offset from circle edges
for i, (pre, post) in enumerate(zip(pre_proportions, post_proportions)):
    ax.annotate('', xy=(post - circle_offset, i), xytext=(pre + circle_offset, i),
                arrowprops={'arrowstyle': '->', 'lw': 1.5, 'color': "#323A56", 
                            'mutation_scale': 15})
# Plot pre dots
ax.scatter(pre_proportions, y_positions, s=150, color="#FFC20A", 
           label='Before', zorder=3, edgecolors='black', linewidth=1.5)
# Plot post dots
ax.scatter(post_proportions, y_positions, s=150, color='#0C7BDC', 
           label='After', zorder=3, edgecolors='black', linewidth=1.5)

# Add value labels and improvement percentages
for i, (pre, post) in enumerate(zip(pre_proportions, post_proportions)):
    ax.text(pre - 0.03, i - 0.05, f'{pre:.1%}', ha='right', fontsize=9, fontweight='bold')
    ax.text(post + 0.03, i - 0.05, f'{post:.1%}', ha='left', fontsize=9, fontweight='bold')
    
    improvement = (post - pre) * 100
    mid_point = (pre + post) / 2
    ax.text(mid_point, i + 0.25, f'+{improvement:.1f}%', ha='center', 
            fontsize=9, color="#58A4DE", fontweight='bold',
            bbox={'boxstyle': 'round,pad=0.3', 'facecolor': '#E8F8F5', 'edgecolor': "#2737AE"})

ax.set_yticks(y_positions)
ax.set_yticklabels(metric_names, fontsize=10)
ax.set_xlabel('Proportion of datasets', fontsize=11, fontweight='bold')
ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
ax.set_title('Dataset-level attributes', fontsize=12, fontweight='bold', pad=5)
ax.set_xlim(-0.1, 1.15)
ax.set_ylim(-0.2, len(metric_names) - 0.5)
ax.set_facecolor('#f7f7f7')
ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.20), fontsize=10, 
          framealpha=0.9, ncol=2, frameon=True)
# plt.tight_layout()
fig.subplots_adjust(left=0.25, right=0.95, top=0.95, bottom=0.15)
plot_path = os.path.join(plots_dir, plot_filename)
plt.savefig(plot_path, dpi=1200)
print(f'\n{plot_filename} has been saved successfully at {plot_path}.\n')
# plt.show()

# Extract the three proportions
metric_names = ['ORCID (proper format)', 'Any author identifier', 'ROR-matched affiliation','Author name punctuation', 'Author name order']
pre_proportions = [
    results_authors['pre']['orcid_valid_proportion'],
    results_authors['pre']['identifier_proportion'],
    results_authors['pre']['ror_proportion'],
    results_authors['pre']['author_initial_proportion'],
    results_authors['pre']['author_order_proportion']
]
post_proportions = [
    results_authors['post']['orcid_valid_proportion'],
    results_authors['post']['identifier_proportion'],
    results_authors['post']['ror_proportion'],
    results_authors['post']['author_initial_proportion'],
    results_authors['post']['author_order_proportion']
]

plot_filename = f"{today}_author-level-summary.png"
fig, ax = plt.subplots(figsize=(8,6))

y_positions = np.arange(len(metric_names))

# Offset the circle to show arrowheads
circle_offset = 0.02


# Draw connector lines with arrows - offset from circle edges
for i, (pre, post) in enumerate(zip(pre_proportions, post_proportions)):
    ax.annotate('', xy=(post - circle_offset, i), xytext=(pre + circle_offset, i),
                arrowprops={'arrowstyle': '->', 'lw': 1.5, 'color': "#000000", 
                           'mutation_scale': 15})
# Plot pre dots
ax.scatter(pre_proportions, y_positions, s=150, color="#FFC20A", 
           label='Before', zorder=3, edgecolors='black', linewidth=1.5)
# Plot post dots
ax.scatter(post_proportions, y_positions, s=150, color='#0C7BDC', 
           label='After', zorder=3, edgecolors='black', linewidth=1.5)

# Add value labels and improvement percentages
for i, (pre, post) in enumerate(zip(pre_proportions, post_proportions)):
    ax.text(pre - 0.03, i - 0.05, f'{pre:.1%}', ha='right', fontsize=9, fontweight='bold')
    ax.text(post + 0.03, i - 0.05, f'{post:.1%}', ha='left', fontsize=9, fontweight='bold')
    
    improvement = (post - pre) * 100
    mid_point = (pre + post) / 2
    ax.text(mid_point, i + 0.25, f'+{improvement:.1f}%', ha='center', 
            fontsize=9, color="#58A4DE", fontweight='bold',
            bbox={'boxstyle': 'round,pad=0.3', 'facecolor': '#E8F8F5', 'edgecolor': "#2737AE"})

ax.set_yticks(y_positions)
ax.set_yticklabels(metric_names, fontsize=10)
ax.set_xlabel('Proportion of authors', fontsize=11, fontweight='bold')
ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
ax.set_title('Author-level attributes', fontsize=12, fontweight='bold', pad=5)
ax.set_xlim(-0.1, 1.15)
ax.set_ylim(-0.2, len(metric_names) - 0.5)
ax.set_facecolor("#f5f3f3")
ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2), fontsize=10, 
          framealpha=0.9, ncol=2, frameon=True)
# plt.tight_layout()
fig.subplots_adjust(left=0.25, right=0.95, top=0.95, bottom=0.15)
plot_path = os.path.join(plots_dir, plot_filename)
plt.savefig(plot_path, dpi=1200)
print(f'\n{plot_filename} has been saved successfully at {plot_path}.\n')
# plt.show()

# ============================================
#           ANONYMIZED EXPORT
# ============================================

# anonymized_dir = os.path.join(outputs_dir, 'anonymized')
# os.makedirs(anonymized_dir, exist_ok=True)

# # Dataset-level columns
# dataset_redact_review = [
#     'institution', 'doi', 'version_id', 'total_version', 'current_status', 'dataverse', 'parent_dataverse', 'dataset_title', 'description', 
#     'dataset_depositor', 'dataset_contact',
#     'authors_flag_name', 'authors_flag_orcid', 'authors_flag_ror',
# ]
# dataset_redact_review_blanks_ok = [
#     'related_works_citations', 'related_works_urls', 'related_works_dois',
# ]
# dataset_redact_privacy = ['dataset_id', 'dataset_email']

# # Author-level columns
# # author_affiliation and author_identifier_expanded handled separately (prefix-preserving)
# author_redact_review = [
#     'author_name', 'doi', 'ror_id', 'institution', 'version_id', 'total_version', 'current_status_y', 'dataverse', 'parent_dataverse', 'dataset_title', 'description', 'dataset_depositor', 'dataset_contact'
# ]
# author_redact_review_blanks_ok = [
#     'author_identifier',  # must preserve blanks: used in .notna() count for plots
#     'related_works_citations', 'related_works_urls', 'related_works_dois',
# ]
# author_redact_privacy = ['dataset_id', 'dataset_email']

# def redact_df(df, redact_review, redact_review_blanks_ok, redact_privacy):
#     out = df.copy()
#     for col in redact_review:
#         if col in out.columns:
#             out[col] = '[redacted for review]'
#     for col in redact_review_blanks_ok:
#         if col in out.columns:
#             out[col] = out[col].where(out[col].isna(), '[redacted for review]')
#     for col in redact_privacy:
#         if col in out.columns:
#             out[col] = '[redacted for privacy]'
#     return out

# def redact_author_df(df):
#     out = redact_df(df, author_redact_review, author_redact_review_blanks_ok, author_redact_privacy)
#     # author_affiliation: preserve ROR prefix so str.contains('https://ror.org/') still works
#     if 'author_affiliation' in out.columns:
#         out['author_affiliation'] = df['author_affiliation'].apply(
#             lambda v: 'https://ror.org/[redacted for review]' if pd.notna(v) and 'https://ror.org/' in str(v)
#             else ('[redacted for review]' if pd.notna(v) else v)
#         )
#     # author_identifier_expanded: blank → blank, ORCID → preserve prefix, other → redact
#     if 'author_identifier_expanded' in out.columns:
#         out['author_identifier_expanded'] = df['author_identifier_expanded'].apply(
#             lambda v: 'https://orcid.org/00[redacted for review]' if pd.notna(v) and 'https://orcid.org/00' in str(v)
#             else ('[redacted for review]' if pd.notna(v) else v)
#         )
#     return out

# dataset_drop_cols = ['current_status', 'version_id', 'dataset_id']
# author_drop_cols  = ['current_status_x', 'current_status_y', 'dataset_id', 'version_id']

# def drop_cols(df, cols):
#     return df.drop(columns=[c for c in cols if c in df.columns])

# anonymized_exports = {
#     f'{today}_UT-austin_all-datasets-authors-PUBLISHED_pre_anonymized.csv':
#         drop_cols(redact_df(results_datasets['pre']['dataframe'],  dataset_redact_review, dataset_redact_review_blanks_ok, dataset_redact_privacy), dataset_drop_cols),
#     f'{today}_UT-austin_all-datasets-authors-PUBLISHED_post_anonymized.csv':
#         drop_cols(redact_df(results_datasets['post']['dataframe'], dataset_redact_review, dataset_redact_review_blanks_ok, dataset_redact_privacy), dataset_drop_cols),
#     f'{today}_UT-austin_all-authors-datasets-PUBLISHED_pre_anonymized.csv':
#         drop_cols(redact_author_df(results_authors['pre']['dataframe']),  author_drop_cols),
#     f'{today}_UT-austin_all-authors-datasets-PUBLISHED_post_anonymized.csv':
#         drop_cols(redact_author_df(results_authors['post']['dataframe']), author_drop_cols),
# }

# for filename, anon_df in anonymized_exports.items():
#     export_path = os.path.join(anonymized_dir, filename)
#     anon_df.to_csv(export_path, index=False, encoding='utf-8')
#     print(f'✓ Anonymized export saved: {export_path}')

# print(f'\nAll anonymized files saved to: {anonymized_dir}\n')

# ============================================
#        ORCID/ROR TIMEFRAME ANALYSIS
# ============================================

if time_analysis:
    old_outputs_dir = os.path.join(outputs_dir, 'old-outputs')
    pattern = '_all-authors-datasets-PUBLISHED'

    # Pre-recuration snapshot, identified by its filename date
    pre_recuration_date = os.environ['RECURATION_START']
    authors_timeframe_pre = load_file_by_date(outputs_dir, old_outputs_dir, pattern, pre_recuration_date)
    authors_timeframe_pre['publication_date'] = pd.to_datetime(authors_timeframe_pre['publication_date'], errors='coerce')

    # Most recent snapshot available
    authors_timeframe_post = load_most_recent_file(outputs_dir, pattern, fallback_dir=old_outputs_dir)
    authors_timeframe_post['publication_date'] = pd.to_datetime(authors_timeframe_post['publication_date'], errors='coerce')

    # Fixed publication-date window to subset the pre-recuration snapshot
    ## This brackets the dates when the plug-ins were enabled and (temporarily) disabled
    date_start = pd.Timestamp('2025-04-17')
    date_end   = pd.Timestamp('2026-02-10')
    # Fixed cutoff for the most recent snapshot: only datasets published after re-curation began
    post_recuration_start = pd.Timestamp(os.environ['RECURATION_END'])

    timeframe_dict = {
        'pre': authors_timeframe_pre[
            (authors_timeframe_pre['publication_date'] >= date_start) &
            (authors_timeframe_pre['publication_date'] <= date_end) &
            ~authors_timeframe_pre['dataset_depositor'].isin(excluded_people_set) &
            ~authors_timeframe_pre['dataset_contact'].isin(excluded_people_set)
        ],
        'post': authors_timeframe_post[
            (authors_timeframe_post['publication_date'] > post_recuration_start) &
            ~authors_timeframe_post['dataset_depositor'].isin(excluded_people_set) &
            ~authors_timeframe_post['dataset_contact'].isin(excluded_people_set)
        ]
    }

    # Store results
    results_timeframe = {}

    for label, df_window in timeframe_dict.items():
        author_count = len(df_window)
        dataset_count = df_window['doi'].nunique()
        missing_orcid_count = df_window['missing_orcid'].sum()
        missing_ror_count   = df_window['missing_ror'].sum()
        orcid_present_count = author_count - missing_orcid_count
        ror_present_count   = author_count - missing_ror_count

        results_timeframe[label] = {
            'author_count': author_count,
            'dataset_count': dataset_count,
            'orcid_present_count': orcid_present_count,
            'ror_present_count': ror_present_count
        }

        print(f'\n--- Timeframe Analysis ({label}) ---')
        print(f'Total authors in window:  {author_count}')
        print(f'Total datasets in window: {dataset_count}')
        print(f'Authors with ORCID:       {orcid_present_count} ({orcid_present_count / author_count:.1%})')
        print(f'Authors with ROR:         {ror_present_count} ({ror_present_count / author_count:.1%})')

    # ============================================
    #   TIMEFRAME ANALYSIS - REPEAT DEPOSITORS
    # ============================================

    # Depositors listed for multiple unique DOIs, identified before date filtering
    repeat_depositors_pre = authors_timeframe_pre.groupby('dataset_depositor')['doi'].nunique()
    repeat_depositors_pre = repeat_depositors_pre[repeat_depositors_pre > 1].index
    authors_timeframe_pre_repeat = authors_timeframe_pre[authors_timeframe_pre['dataset_depositor'].isin(repeat_depositors_pre)]

    repeat_depositors_post = authors_timeframe_post.groupby('dataset_depositor')['doi'].nunique()
    repeat_depositors_post = repeat_depositors_post[repeat_depositors_post > 1].index
    authors_timeframe_post_repeat = authors_timeframe_post[authors_timeframe_post['dataset_depositor'].isin(repeat_depositors_post)]

    timeframe_dict_repeat = {
        'pre': authors_timeframe_pre_repeat[
            (authors_timeframe_pre_repeat['publication_date'] >= date_start) &
            (authors_timeframe_pre_repeat['publication_date'] <= date_end) &
            ~authors_timeframe_pre_repeat['dataset_depositor'].isin(excluded_people_set) &
            ~authors_timeframe_pre_repeat['dataset_contact'].isin(excluded_people_set)
        ],
        'post': authors_timeframe_post_repeat[
            (authors_timeframe_post_repeat['publication_date'] > post_recuration_start) &
            ~authors_timeframe_post_repeat['dataset_depositor'].isin(excluded_people_set) &
            ~authors_timeframe_post_repeat['dataset_contact'].isin(excluded_people_set)
        ]
    }

    # Store results
    results_timeframe_repeat = {}

    for label, df_window in timeframe_dict_repeat.items():
        author_count = len(df_window)
        dataset_count = df_window['doi'].nunique()
        missing_orcid_count = df_window['missing_orcid'].sum()
        missing_ror_count   = df_window['missing_ror'].sum()
        orcid_present_count = author_count - missing_orcid_count
        ror_present_count   = author_count - missing_ror_count

        results_timeframe_repeat[label] = {
            'author_count': author_count,
            'dataset_count': dataset_count,
            'orcid_present_count': orcid_present_count,
            'ror_present_count': ror_present_count
        }

        print(f'\n--- Timeframe Analysis, repeat depositors ({label}) ---')
        print(f'Total authors in window:  {author_count}')
        print(f'Total datasets in window: {dataset_count}')
        print(f'Authors with ORCID:       {orcid_present_count} ({orcid_present_count / author_count:.1%})')
        print(f'Authors with ROR:         {ror_present_count} ({ror_present_count / author_count:.1%})')

    # ============================================
    #        TIMEFRAME ANALYSIS - GRAPH
    # ============================================

    def _timeframe_proportions(results):
        pre_count  = results['pre']['author_count']
        post_count = results['post']['author_count']
        pre_props = [
            results['pre']['orcid_present_count'] / pre_count,
            results['pre']['ror_present_count'] / pre_count
        ]
        post_props = [
            results['post']['orcid_present_count'] / post_count,
            results['post']['ror_present_count'] / post_count
        ]
        return pre_props, post_props

    metric_names = ['ORCID', 'ROR']
    facets = [
        ('All depositors', results_timeframe),
        ('Repeat depositors', results_timeframe_repeat)
    ]

    plot_filename = f"{today}_timeframe-summary.png"
    fig, axes = plt.subplots(1, 2, figsize=(8, 6), sharey=True)

    x = np.arange(len(metric_names))
    bar_width = 0.35

    for ax, (facet_title, results) in zip(axes, facets):
        pre_props, post_props = _timeframe_proportions(results)

        bars_pre = ax.bar(x - bar_width / 2, pre_props, bar_width, color="#FFC20A",
                           label='Before', edgecolor='black', linewidth=1.5, zorder=3)
        bars_post = ax.bar(x + bar_width / 2, post_props, bar_width, color='#0C7BDC',
                            label='After', edgecolor='black', linewidth=1.5, zorder=3)

        for bars in (bars_pre, bars_post):
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width() / 2, height + 0.02, f'{height:.1%}',
                        ha='center', fontsize=9, fontweight='bold')

        ax.set_xticks(x)
        ax.set_xticklabels(metric_names, fontsize=10)
        ax.set_title(facet_title, fontsize=12, fontweight='bold')
        ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
        ax.set_ylim(0, 1.15)
        ax.set_facecolor('#f7f7f7')
        ax.grid(True, axis='y', color='white', linestyle='-', linewidth=1.5)

    axes[0].set_ylabel('Proportion of authors', fontsize=11, fontweight='bold')
    fig.suptitle('Author identifier completeness by depositor group', fontsize=13, fontweight='bold')
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 0.03), fontsize=10,
               framealpha=0.9, ncol=2, frameon=True)
    fig.subplots_adjust(top=0.85, bottom=0.15, wspace=0.15)
    plot_path = os.path.join(plots_dir, plot_filename)
    plt.savefig(plot_path, dpi=1200, bbox_inches='tight')
    print(f'\n{plot_filename} has been saved successfully at {plot_path}.\n')