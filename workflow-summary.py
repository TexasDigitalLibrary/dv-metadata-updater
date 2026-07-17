import json
import numpy as np
import os
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from utils import env_bool, load_most_recent_file, load_nth_most_recent_file

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

# Test environment (incomplete run, faster to complete)
test = env_bool('TEST_ENVIRONMENT')

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
metric_names = ['Related publication (identifier)', 'Related publication (URL)', 'Related publication (citation)', 'All authors w/ RORs', 'All authors w/ ORCIDs', 'Keywords properly formatted', 'Title properly formatted', ]
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
fig, ax = plt.subplots(figsize=(12, 6.75))
plot_width = 6.0  # inches
plot_height = 3.5  # inches

y_positions = np.arange(len(metric_names))

# Offset the circle to show arrowheads
circle_offset = 0.02

# Draw connector lines with arrows - offset from circle edges
for i, (pre, post) in enumerate(zip(pre_proportions, post_proportions)):
    ax.annotate('', xy=(post - circle_offset, i), xytext=(pre + circle_offset, i),
                arrowprops=dict(arrowstyle='->', lw=2.5, color="#323A56", 
                               mutation_scale=15))
# Plot pre dots
ax.scatter(pre_proportions, y_positions, s=150, color="#FFC20A", 
           label='Before', zorder=3, edgecolors='black', linewidth=1.5)
# Plot post dots
ax.scatter(post_proportions, y_positions, s=150, color='#0C7BDC', 
           label='After', zorder=3, edgecolors='black', linewidth=1.5)

# Add value labels and improvement percentages
for i, (pre, post) in enumerate(zip(pre_proportions, post_proportions)):
    ax.text(pre - 0.03, i - 0.05, f'{pre:.1%}', ha='right', fontsize=12, fontweight='bold')
    ax.text(post + 0.03, i - 0.05, f'{post:.1%}', ha='left', fontsize=12, fontweight='bold')
    
    improvement = (post - pre) * 100
    mid_point = (pre + post) / 2
    ax.text(mid_point, i + 0.25, f'+{improvement:.1f}%', ha='center', 
            fontsize=10, color="#58A4DE", fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='#E8F8F5', edgecolor="#2737AE"))

ax.set_yticks(y_positions)
ax.set_yticklabels(metric_names, fontsize=12)
ax.set_xlabel('Proportion of datasets', fontsize=14, fontweight='bold')
ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
ax.set_title('Dataset-level attributes', fontsize=16, fontweight='bold', pad=5)
ax.set_xlim(-0.1, 1.15)
ax.set_ylim(-0.2, len(metric_names) - 0.5)
ax.set_facecolor('#f7f7f7')
ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.20), fontsize=11, 
          framealpha=0.9, ncol=2, frameon=True)
# plt.tight_layout()
fig.subplots_adjust(left=0.25, right=0.95, top=0.95, bottom=0.15)
plot_path = os.path.join(plots_dir, plot_filename)
plt.savefig(plot_path, dpi=300)
print(f'\n{plot_filename} has been saved successfully at {plot_path}.\n')
# plt.show()

# Extract the three proportions
metric_names = ['ORCID (properly formatted)', 'Any author identifier', 'ROR-matched affiliation','Author name punctuation', 'Author name order']
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
fig, ax = plt.subplots(figsize=(12, 6.75))

y_positions = np.arange(len(metric_names))

# Offset the circle to show arrowheads
circle_offset = 0.02


# Draw connector lines with arrows - offset from circle edges
for i, (pre, post) in enumerate(zip(pre_proportions, post_proportions)):
    ax.annotate('', xy=(post - circle_offset, i), xytext=(pre + circle_offset, i),
                arrowprops=dict(arrowstyle='->', lw=2.5, color="#000000", 
                               mutation_scale=15))
# Plot pre dots
ax.scatter(pre_proportions, y_positions, s=150, color="#FFC20A", 
           label='Before', zorder=3, edgecolors='black', linewidth=1.5)
# Plot post dots
ax.scatter(post_proportions, y_positions, s=150, color='#0C7BDC', 
           label='After', zorder=3, edgecolors='black', linewidth=1.5)

# Add value labels and improvement percentages
for i, (pre, post) in enumerate(zip(pre_proportions, post_proportions)):
    ax.text(pre - 0.03, i - 0.05, f'{pre:.1%}', ha='right', fontsize=12, fontweight='bold')
    ax.text(post + 0.03, i - 0.05, f'{post:.1%}', ha='left', fontsize=12, fontweight='bold')
    
    improvement = (post - pre) * 100
    mid_point = (pre + post) / 2
    ax.text(mid_point, i + 0.25, f'+{improvement:.1f}%', ha='center', 
            fontsize=10, color="#58A4DE", fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='#E8F8F5', edgecolor="#2737AE"))

ax.set_yticks(y_positions)
ax.set_yticklabels(metric_names, fontsize=12)
ax.set_xlabel('Proportion of authors', fontsize=14, fontweight='bold')
ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
ax.set_title('Author-level attributes', fontsize=16, fontweight='bold', pad=5)
ax.set_xlim(-0.1, 1.15)
ax.set_ylim(-0.2, len(metric_names) - 0.5)
ax.set_facecolor("#f5f3f3")
ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2), fontsize=11, 
          framealpha=0.9, ncol=2, frameon=True)
# plt.tight_layout()
fig.subplots_adjust(left=0.25, right=0.95, top=0.95, bottom=0.15)
plot_path = os.path.join(plots_dir, plot_filename)
plt.savefig(plot_path, dpi=300)
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