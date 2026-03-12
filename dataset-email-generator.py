import json
import os
import win32com.client as win32
from datetime import datetime
from pathlib import Path
from utils import load_most_recent_file

# ============================================
#               WORKFLOW SET-UP
# ============================================

# Config file
with open('config.json', 'r') as file:
    config = json.load(file)

# Import PDF with FAQs
faq_relative_path = "docs/faq.pdf"
# This resolves to the absolute path relative to the current working directory
faq_absolute_path = Path(faq_relative_path).resolve()

# Test environment (incomplete run, faster to complete)
## For this script, the test env will only generate/test emails for 10 contacts
test = config['TOGGLES']['test_email']
draft_email = config['TOGGLES']['draft_email']

# Timestamp to calculate run time
start_time = datetime.now() 
# Current date for filenames
today = datetime.now().strftime('%Y%m%d') 

# Import information about user for auto-populating drafts
user_name = config['USER']['user_name']
user_email = config['USER']['user_email']

# Get directories
script_dir = os.getcwd()
outputs_dir = os.path.join(script_dir, 'outputs')

# ============================================
#            INPUT DATA PROCESSING
# ============================================

# Load most recent version of remediated authors and datasets df
pattern = f'_final-combined-remediated.csv'
datasets = load_most_recent_file(outputs_dir, pattern)

# Explode on contact email for multi-contact datasets
datasets['dataset_contact'] = datasets['dataset_contact'].str.split('; ')
datasets['dataset_email'] = datasets['dataset_email'].str.split('; ')
datasets_exploded = datasets.explode(['dataset_contact', 'dataset_email'])
datasets_exploded_dedup = datasets_exploded.drop_duplicates(subset=['doi', 'dataset_contact', 'dataset_email']).dropna(subset=['dataset_contact'])

## Flip name if in 'Last, First' format
def flip_names(names):
    flipped = []
    for name in names.split(';'):
        parts = [part.strip() for part in name.split(',')]
        flipped.append(' '.join(parts[::-1]))
    return '; '.join(flipped)

datasets_exploded_dedup['dataset_contact_flipped'] = datasets_exploded_dedup['dataset_contact'].apply(flip_names)
if test:
    datasets_exploded_dedup = datasets_exploded_dedup.head(10)
    # datasets = datasets.drop_duplicates('dataset_email')

# Group by contact_email
grouped = datasets_exploded_dedup.groupby('dataset_email')

# ============================================
#              EMAIL DRAFTING
# ============================================

# Toggle designed for quick-testing of processing steps without creating drafts
if draft_email:
    outlook = win32.Dispatch('outlook.application')

    for recipient_email, group in grouped:
        # Get contact name (same for all rows in group)
        name = group['dataset_contact_flipped'].iloc[0]
        
        # Count number of datasets for that contact
        dataset_count = len(group)
        
        # Condition subject line based on count
        if dataset_count == 1:
            subject = "Notification of forthcoming re-curation of your published dataset in the Texas Data Repository"
        else:
            subject = "Notification of forthcoming re-curation of your published datasets in the Texas Data Repository"
        
        # Build list of datasets for each contact email
        dataset_list = ""
        for idx, row in group.iterrows():
            dataset_title = row['dataset_title']
            dataset_doi = row['doi']
            hyperlinked_doi = 'https://doi.org/' + dataset_doi
            publication_date = row['publication_date']
            dataset_list += f"<li><b>{dataset_title}</b> (DOI: <a href={hyperlinked_doi}>{dataset_doi}</a>); <i>published on {publication_date}</i></li>"
        
        # Create email
        mail = outlook.CreateItem(0)
        mail.Subject = subject
        
        if dataset_count == 1:
            htmlmessage = f"<span style='font-family:calibri; font-size:11pt'>Dear {name},<br><br>You are receiving this email in relation to a published dataset in the Texas Data Repository (TDR), titled <b>\"{dataset_title}\"</b> (DOI: <a href={hyperlinked_doi}>{dataset_doi}</a>), for which you are listed as the contact person.</span><br><br>"
        else:
            htmlmessage = f"<span style='font-family:calibri; font-size:11pt'>Dear {name},<br><br>You are receiving this email in relation to multiple published datasets in the Texas Data Repository (TDR), for which you are listed as the contact person.</span><br><br>"
        htmlmessage += (f"<span style='font-family:calibri; font-size:11pt'>UT Libraries staff are performing a repository-wide re-curation of published datasets in order to standardize and to enhance metadata during the week of March 23-27, 2026. These enhancements include:<br><br>"
        "<ul>"
        "<li>Addition of <a href='https://info.orcid.org/researchers/'>ORCID</a> identifiers</li>"
        "<li>Addition of <a href='https://ror.org/about/'>ROR,</a> identifiers</li>"
        "<li>Standardization of how author names are formatted</li>"
        "<li>Standardization of how keywords are formatted</li>"
        "<li>Addition of related scholarly outputs (e.g., articles, preprints)</li>"
        "<li>Clean-up of punctuation in titles</li>"
        "</ul>"
        "</span>")
        htmlmessage += f"<span style='font-family:calibri; font-size:11pt'> Additional information/FAQs are attached in the PDF and available online <a href='https://guides.lib.utexas.edu/research-data-services'>here</a>.</span><br><br>"
        if dataset_count > 1:
            htmlmessage += f"<span style='font-family:calibri; font-size:11pt'>The following datasets are slated for re-curated:<br><ul>{dataset_list}</ul></span><br>"
        htmlmessage += f"<span style='font-family:calibri; font-size:11pt'>These enhancements are intended to improve the standardization and quality of dataset metadata in TDR, primarily with respect to the use of persistent identifiers (PIDs), which is an area that has been highlighted as <a href='https://repository.si.edu/items/0fe77b19-f2d9-400c-8886-757d4487d907'>desirable or required by federal research agencies</a>.</span><br><br>"
        if dataset_count == 1:
            htmlmessage += f"<span style='font-family:calibri; font-size:11pt'> No action is required from your end, but please be aware that you will receive an automated email from TDR that your dataset has been published when this process is complete because it will result in the publication of a new minor version of the dataset.</span><br><br>"
        else:
            htmlmessage += f"<span style='font-family:calibri; font-size:11pt'> No action is required from your end, but please be aware that you will receive an automated email from TDR, for each dataset, that the dataset has been published when this process is complete because it will result in the publication of a new minor version of the dataset.</span><br><br>"
        htmlmessage += f"<span style='font-family:calibri, font-size:11pt'>This email has been automatically generated. If you have any questions, please reach out to Research Data Coordinator, {user_name}, at <a href='mailto:{user_email}'>{user_email}</a>.</span>"    
        mail.Attachments.Add(str(faq_absolute_path))
        mail.HTMLBody = htmlmessage
        mail.To = recipient_email
        mail.BCC = user_email
        mail.Save()
        # mail.Send()
        print(f"Draft email for {name} has been created and saved successfully.\n")

    print(f"Created {len(grouped)} email drafts.\n")