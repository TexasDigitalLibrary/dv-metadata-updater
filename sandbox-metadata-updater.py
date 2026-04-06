import json
import os
import requests

# Config file
with open('config.json', 'r') as file:
    config = json.load(file)

script_dir = os.getcwd()
# This script assumes you are using test data
json_dir = os.path.join('test/json/modified')

# Extract ONLY the metadataBlocks from the datasetVersion
SERVER_URL = 'https://dataverse-training.tdl.org'
## Specify the DOI of the dataset to be updated
DOI = 'doi:10.33536/FK2/PJ1JMT'
headers_tdr = {
    'X-Dataverse-key': config['KEYS']['sandbox_token'],
    'Content-Type': 'application/json'
}

# Target any JSON representation that you want to test with
## Does not need to be from the same dataset as the one being edited
filename = f'{json_dir}/modified-10.18738_T8_WIKCEV-dataset-metadata.json'

with open(filename, 'r', encoding='utf-8') as f:
    full_data = json.load(f)

# Get the datasetVersion
dataset_info = full_data['data']
# Check for latestVersion or datasetVersion (varies depending on whether it has been versioned or not)
if 'latestVersion' in dataset_info:
    dataset_version = dataset_info['latestVersion']
else:
    dataset_version = dataset_info['datasetVersion']

# Create payload with EVERYTHING except 'files'
payload = {k: v for k, v in dataset_version.items() if k != 'files'}

print("Payload to send:")
print(json.dumps(payload, indent=2))

# Update and put into draft status
update_url = f"{SERVER_URL}/api/datasets/:persistentId/versions/:draft?persistentId={DOI}"
response = requests.put(update_url, headers=headers_tdr, json=payload)

if response.status_code == 200:
    print("✓ Dataset metadata updated successfully.")
else:
    print(f"✗ Failed to update metadata. Status code: {response.status_code}")
    print(response.text)
    exit(1)

# # Update and publish the dataset
# publish_url = f"{SERVER_URL}/api/datasets/:persistentId/actions/:publish?persistentId={DOI}&type=minor"
# response = requests.post(publish_url, headers=headers_tdr)

# if response.status_code == 200:
#     print("✓ Dataset published successfully.")
# else:
#     print(f"✗ Failed to publish dataset. Status code: {response.status_code}")
#     print(response.text)