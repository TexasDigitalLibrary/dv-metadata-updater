import json
import numpy as np
import os
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from utils import load_most_recent_file, load_nth_most_recent_file

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
test = config['TOGGLES']['test_environment']

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
pattern = f'_all-datasets-authors-PUBLISHED.csv'
datasets_post = load_most_recent_file(outputs_dir, pattern)

# Load second most recent version of dataset-authors file
datasets_pre = load_nth_most_recent_file(outputs_dir, pattern, n=2)

# Load most recent version of authors file
pattern = f'_all-authors-datasets-PUBLISHED.csv'
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
    # Count identifiers
    identifier_count = df_retained['related_works_dois'].notna().sum()
    identifier_proportion = round((identifier_count / dataset_count), 3)
    # Count identifiers
    url_count = df_retained['related_works_urls'].notna().sum()
    url_proportion = round((url_count / dataset_count), 3)
    
    # Store results
    results_datasets[label] = {
        'dataframe': df_retained,
        'count': dataset_count,
        'citation_count': citation_count,
        'citation_proportion': citation_proportion,
        'identifier_count': identifier_count,
        'identifier_proportion': identifier_proportion,
        'url_count': url_count,
        'url_proportion': url_proportion
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

print(f'{"Total number of in-scope authors":<40} {results_datasets["pre"]["count"]:<20} {results_datasets["post"]["count"]:<20}')
print()

print(f'{"Datasets with a citation field":<40} {results_datasets["pre"]["citation_count"]:<20} {results_datasets["post"]["citation_count"]:<20}')
print(f'{"Proportion with a citation field":<40} {results_datasets["pre"]["citation_proportion"]:<20} {results_datasets["post"]["citation_proportion"]:<20}')
print()

print(f'{"Datasets with an identifier":<40} {results_datasets["pre"]["identifier_count"]:<20} {results_datasets["post"]["identifier_count"]:<20}')
print(f'{"Proportion with identifier":<40} {results_datasets["pre"]["identifier_proportion"]:<20} {results_datasets["post"]["identifier_proportion"]:<20}')
print()

print(f'{"Datasets with a URL":<40} {results_datasets["pre"]["url_count"]:<20} {results_datasets["post"]["url_count"]:<20}')
print(f'{"Proportion with proper ORCID":<40} {results_datasets["pre"]["url_proportion"]:<20} {results_datasets["post"]["url_proportion"]:<20}')
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
    ror_count = df_retained['author_affiliation'].str.contains('https://ror.org/').sum()
    ror_proportion = round((ror_count / author_count), 3)
    # Count ORCID
    orcid_count = df_retained['author_identifier'].notna().sum()
    orcid_proportion = round((orcid_count / author_count), 3)
    # Count proper ORCID
    orcid_proper_count = df_retained['author_identifier_expanded'].str.contains('https://orcid.org/00').sum()
    orcid_proper_proportion = round((orcid_proper_count / author_count), 3)
    
    # Store results
    results_authors[label] = {
        'dataframe': df_retained,
        'author_count': author_count,
        'ror_count': ror_count,
        'ror_proportion': ror_proportion,
        'identifier_count': orcid_count,
        'identifier_proportion': orcid_proportion,
        'orcid_count': orcid_proper_count,
        'orcid_proportion': orcid_proper_proportion
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
print('=' * 80)
print()

# ============================================
#               GRAPHS
# ============================================


# Extract the three proportions
metric_names = ['Identifier listed', 'URL listed', 'Citation listed']
pre_proportions = [
    results_datasets['pre']['identifier_proportion'],
    results_datasets['pre']['url_proportion'],
    results_datasets['pre']['citation_proportion']
]
post_proportions = [
    results_datasets['post']['identifier_proportion'],
    results_datasets['post']['url_proportion'],
    results_datasets['post']['citation_proportion']
]

plot_filename = f"{today}_dataset-level-summary.png"
fig, ax = plt.subplots(figsize=(8, 4))

y_positions = np.arange(len(metric_names))

# Offset the circle to show arrowheads
circle_offset = 0.02

# Draw connector lines with arrows - offset from circle edges
for i, (pre, post) in enumerate(zip(pre_proportions, post_proportions)):
    ax.annotate('', xy=(post - circle_offset, i), xytext=(pre + circle_offset, i),
                arrowprops=dict(arrowstyle='->', lw=2, color="#325641", 
                               mutation_scale=15))
# Plot pre dots
ax.scatter(pre_proportions, y_positions, s=150, color="#F98989", 
           label='Before', zorder=3, edgecolors='black', linewidth=1.5)
# Plot post dots
ax.scatter(post_proportions, y_positions, s=150, color='#4ECDC4', 
           label='After', zorder=3, edgecolors='black', linewidth=1.5)

# Add value labels and improvement percentages
for i, (pre, post) in enumerate(zip(pre_proportions, post_proportions)):
    ax.text(pre - 0.03, i - 0.05, f'{pre:.1%}', ha='right', fontsize=10, fontweight='bold')
    ax.text(post + 0.03, i - 0.05, f'{post:.1%}', ha='left', fontsize=10, fontweight='bold')
    
    improvement = (post - pre) * 100
    mid_point = (pre + post) / 2
    ax.text(mid_point, i + 0.15, f'+{improvement:.1f}%', ha='center', 
            fontsize=10, color='#27AE60', fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='#E8F8F5', edgecolor='#27AE60'))

ax.set_yticks(y_positions)
ax.set_yticklabels(metric_names, fontsize=11)
ax.set_xlabel('Proportion of datasets', fontsize=12, fontweight='bold')
ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
ax.set_title('Related works', fontsize=14, fontweight='bold', pad=5)
ax.set_xlim(-0.1, 1.1)
ax.set_ylim(-0.2, len(metric_names) - 0.5)
ax.set_facecolor('#f7f7f7')
ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.20), fontsize=11, 
          framealpha=0.9, ncol=2, frameon=True)
plt.tight_layout()
plot_path = os.path.join(plots_dir, plot_filename)
plt.savefig(plot_path, dpi=300)
print(f'\n{plot_filename} has been saved successfully at {plot_path}.\n')
# plt.show()

# Extract the three proportions
metric_names = ['ORCID (properly formatted)', 'Any author identifier', 'ROR-matched affiliation']
pre_proportions = [
    results_authors['pre']['orcid_proportion'],
    results_authors['pre']['identifier_proportion'],
    results_authors['pre']['ror_proportion']
]
post_proportions = [
    results_authors['post']['orcid_proportion'],
    results_authors['post']['identifier_proportion'],
    results_authors['post']['ror_proportion']
]

plot_filename = f"{today}_author-level-summary.png"
fig, ax = plt.subplots(figsize=(8, 4))

y_positions = np.arange(len(metric_names))

# Offset the circle to show arrowheads
circle_offset = 0.02

# Draw connector lines with arrows - offset from circle edges
for i, (pre, post) in enumerate(zip(pre_proportions, post_proportions)):
    ax.annotate('', xy=(post - circle_offset, i), xytext=(pre + circle_offset, i),
                arrowprops=dict(arrowstyle='->', lw=2, color="#325641", 
                               mutation_scale=15))
# Plot pre dots
ax.scatter(pre_proportions, y_positions, s=150, color="#F98989", 
           label='Before', zorder=3, edgecolors='black', linewidth=1.5)
# Plot post dots
ax.scatter(post_proportions, y_positions, s=150, color='#4ECDC4', 
           label='After', zorder=3, edgecolors='black', linewidth=1.5)

# Add value labels and improvement percentages
for i, (pre, post) in enumerate(zip(pre_proportions, post_proportions)):
    ax.text(pre - 0.03, i - 0.05, f'{pre:.1%}', ha='right', fontsize=10, fontweight='bold')
    ax.text(post + 0.03, i - 0.05, f'{post:.1%}', ha='left', fontsize=10, fontweight='bold')
    
    improvement = (post - pre) * 100
    mid_point = (pre + post) / 2
    ax.text(mid_point, i + 0.15, f'+{improvement:.1f}%', ha='center', 
            fontsize=10, color='#27AE60', fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='#E8F8F5', edgecolor='#27AE60'))

ax.set_yticks(y_positions)
ax.set_yticklabels(metric_names, fontsize=11)
ax.set_xlabel('Proportion of authors', fontsize=12, fontweight='bold')
ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
ax.set_title('Author identifiers', fontsize=14, fontweight='bold', pad=5)
ax.set_xlim(-0.1, 1.1)
ax.set_ylim(-0.2, len(metric_names) - 0.5)
ax.set_facecolor('#f7f7f7')
ax.grid(True, which='both', color='white', linestyle='-', linewidth=1.5)
ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2), fontsize=11, 
          framealpha=0.9, ncol=2, frameon=True)
plt.tight_layout()
plot_path = os.path.join(plots_dir, plot_filename)
plt.savefig(plot_path, dpi=300)
print(f'\n{plot_filename} has been saved successfully at {plot_path}.\n')
# plt.show()