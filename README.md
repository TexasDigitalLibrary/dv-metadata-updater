# README

## Metadata
* *Version*: 1.1.0
* *Released*: 2026/04/06
* *Author(s)*: Bryan Gee (UT Libraries, University of Texas at Austin; bryan.gee@austin.utexas.edu; ORCID: [0000-0003-4517-3290](https://orcid.org/0000-0003-4517-3290))
* *Contributor(s)*: None
* *License*: [3-Clause BSD](https://opensource.org/license/bsd-3-clause)
* *README last updated*: 2026/04/06

## Table of Contents
1. [Purpose](#purpose)
2. [Contents](#contents)
3. [Outputs](#outputs)
4. [Requirements](#requirements)
5. [Development](#development)
6. [Versions](#versions)

## Purpose
This repository contains scripts to facilitate semi- to fully-automated metadata recuration in a Dataverse installation. It was designed in the specific context of the [Texas Data Repository](https://dataverse.tdl.org/), a multi-institutional installation, but should be easily repurposeable for other installations. Currently, it is capable of flagging and remediating missing or malformatted ORCIDs, missing ROR identifiers, malformatted keywords (entered in one semi-colon- or comma-delimited string), malformatted titles (ending in blank space or with period), and non-standardized author names (missing middle initials, not in Last, First order). It is also capable of flagging, but not remediating, non-CC0 licensing that might need to be converted from a 'Custom Terms' designation to the formal license and datasets where a related work probably exists but is not hard-coded into the metadata - these two require manual review. Any of the automated components can also be done or enhanced manually.

## Contents
There are seven scripts in this workflow, but only two or three are "necessary" depending on how you want to adopt it. They are separated in part because there are a few manual steps involved in this process and in part because not all of them are necessary depending on a local use.

| Script | Purpose |
|------|---------|
| `dataset-metadata-assessment.py` | Retrieves metadata through the Dataverse API, separates it into a dataset-level and an author-level dataframe, and creates flags based on the presence/absence and structure of target fields (e.g., no ROR ID, malformed ORCID without hyphens between four-digit blocks). Currently, runtime is ~15-20 minutes for ~1600 datasets. |
| `ror-metadata-retrieval.py` | Imports a manually edited map of freeform affiliations to ROR IDs, loops through the ROR API, and returns the display name. This script is not necessary if you add both the ROR identifier and the institution name to the mapping file. It may be deprecated in the future (especially with [forthcoming requirements for using the ROR API](https://ror.readme.io/docs/rest-api)) or substituted for the [data dump hosted on Zenodo](https://doi.org/10.5281/zenodo.6347574). Runtime should be under 2 minutes. |
| `dataset-metadata-remediation.py` | Imports the outputs from `dataset-metadata-assessment.py` and creates new columns for remediated outputs (e.g., flipped author names, reformatted ORCIDs). Each remediation component can be toggled on or off (e.g., ROR remediation but not ORCID remediation). Runtime is <30 seconds. |
| `dataset-email-generator.py` | Loops through the output of the previous files, identify all unique contact emails, identify all datasets slated for remediation for which a given email is listed, and prepares an email with the list of relevant datasets for each contact, including an FAQ attachment on the process. The email can be created in your local Drafts folder or set to auto-send upon running the script. Current runtime is <1 minute. |
| `dataset-metadata-updater.py` | Imports the final output from the previous files with the columns of remediated metadata, retrieves JSON representations of all affected datasets from the Dataverse API, substitutes the changed metadata, pushes the updated JSON through the API, and creates drafts of the new version. The script can also be set to put updated versions into review or directly publish them. As with the third script, toggles for each metadata variable can be turned on or off if you only want to do certain metadata steps. Runtime to retrieve current JSON representations of the datasets' metadata and to update them is typically ~1 minute but will depend on the number of datasets and API stability. Likewise, time to push changes back to the server will depend on API stability. |
| `sandbox-metadata-updater.py` | Sandbox script for a sandbox instance of Dataverse in order to gain familiarity on how auto-updating of metadata for datasets (either putting into draft or directly publishing) works. |
| `workflow-summary.py` | Imports the most recent and second most recent dataframes for authors and datasets with metadata flags but no remediation, then outputs text and graphical comparisons of pre- and post-processing metadata quality. Limited to graphs and summaries for RDAP 2026 Summit at present. |

| Supporting file | Purpose |
|------|---------|
| `utils.py` | Contains core functions that are used repeatedly in this workflow and/or in other UT Libraries workflows. |
| `config-template.json` | Template configuration file, which controls various toggles for the workflows, contains the Dataverse API key, and provides several other dynamic fields that will need to be customized. **In order to run the script, this file needs to be populated and renamed to *config.json*. |
| `affiliation-map-primary.csv` | Template affiliation map file, which is based on all unique affiliations for published datasets across all institutional dataverse in the Texas Data Repository as of early March 2026. If you are at a non-TDR institution, you will want to delete it and have the script create a new map because there will be minimal overlap. |

## Outputs
### `dataset-metadata-assessment.py`
| File | Content |
|------|---------|
| `{today}_{institution-name}_all-datasets-PUBLISHED.csv` | The dataset-level dataframe returned from the initial search results retrieved from the Search API endpoint. |
| `{today}_{institution-name}_all-datasets-PUBLISHED-flagged.csv` | The expanded dataset-level dataframe that includes metadata retrieved from the Native API endpoint and Boolean columns that represent metadata flags for deficient or malformatted metadata. |
| `{today}_{institution-name}_all-datasets-authors-PUBLISHED.csv` | The expanded dataset-level dataframe with select aggregated metadata flags that were merged in from the author-level analysis. |
| `{today}_{institution-name}_all-authors-PUBLISHED.csv` | The author-level dataframe returned from the metadata retrieved from the Native API endpoint. |
| `{today}_{institution-name}_all-authors-datasets-PUBLISHED.csv` | The expanded author-level dataframe with dataset-level metadata merged in.  |
| `affiliation-map-primary.csv` or `affiliation-map_TEMP.csv` | If `affiliation-map-primary.csv` does not exist, it is created. It will need to have ROR identifiers matched where appropriate in a 'ror' column. If one does exist, any new entries are concatenated to the bottom, and the file is output as `affiliation-map_TEMP.csv` to avoid overwriting the existing file. This file should then be manually inspected for new ROR matching, and then it can be saved as *affiliation-map-primary.csv* (overwrite it manually).  |

### `ror-metadata-retrieval.py`
| File | Content |
|------|---------|
| `affiliation-map_enriched.csv` | The affiliation map file with official names matched to ROR identifiers. This file should then be manually inspected for quality control, and then it can be saved as *affiliation-map-primary.csv* (overwrite it manually); you could alter the script to automatically overwrite as well.|

### `dataset-metadata-remediation.py`
| File | Content |
|------|---------|
| `{today}_final-datasets-remediated.csv` | The dataset-level dataframe with remediated titles and keywords in new columns and Boolean columns for whether a fix was implemented (to differentiate from the pre-existing 'flag' that indicates a potential remediation need). Previously collected/computed fields like flags for a possible missing related work are retained. |
| `{today}_final-authors-remediated.csv` | The author-level dataframe with remediated ORCIDs, ROR identifiers, and author names in new columns and Boolean columns for whether a fix was implemented (to differentiate from the pre-existing 'flag' that indicates a potential remediation need). When author names were remediated, ORCID identifiers were remediated, or ORCID identifiers were inferred, the action/basis is also listed in another column. Previously collected/computed fields like flags for a possible missing ORCID are retained. |
| `{today}_final-authors-remediated.csv` | The author-level dataframe with dataset-level metadata merged in. |

### `dataset-email-generator.py` 
This script does not generate any output files.

### `dataset-metadata-updater.py` 
| File | Content |
|------|---------|
| `{doi}_dataset-metadata.json` | JSON representation of the dataset's current metadata. |
| `modified-{doi}_dataset-metadata.json` | Updated JSON representation of the dataset's metadata. |
| `{today}_metadata-changes-log.csv` | Change log file with the DOI, the original author name (authorName field), what field was modified, what the original value in that field was, what the new value in that field was, what category the change was (e.g., 'added ROR'), and the timestamp. For dataset-level modifications, the original author name is replaced with 'DATASET_LEVEL'. Each change is returned as a separate row, so there may be multiple entries for one DOI and even for one author. |
| `{today}_metadata-changes-log.json` | Change log file with the same information as the CSV version. Each DOI is used as the root, with all changes to any component nested within it (i.e. single entry per DOI). |

## Requirements
This workflow mostly makes use of modules in the Python standard library: *ast*, *csv*, *datetime*, *json*, *math*, *os*, *pandas*, *re*, *requests*, *sys*, and *time*. A few other well-known modules may need to be installed: *pywin32* (only if using the `dataset-email-generator.py` script) and *rapidfuzz* (this may be deprecated in the future). The `utils.py` file with custom functions is also necessary. 

For the addition of ROR identifiers and the clean-up/addition of ORCID identifiers, the [external vocab plug-in for ORCID and ROR](https://github.com/gdcc/dataverse-external-vocab-support/blob/main/examples/authorIDandAffilationUsingORCIDandROR.md) will need to be activated for the Dataverse installation; this plug-in has its own dependencies. I have tested this process when the plug-in was installed but not activated, which seems to work fine, but I am not sure if it works the same if the plug-in is simply not installed. 

As noted in its description, `dataset-email-generator.py` is only designed for Microsoft Outlook and requires you to have the desktop application installed and logged into; I am not aware of any requirements for a specific Outlook version, operating system, or institutional configuration of Outlook but have not tested this.

The script was developed in **Python 3.12** for **Dataverse 6.5**. I have not tested backwards or forwards compatibility at this time (but I assume the forwards compatibility is okay).

### Config file fields
| File | Content |
|------|---------|
| `KEYS` | Contains your API token (do not share!).|
| `USER` | Contains your user information that will populate fields in email drafts; only necessary if you are running `dataset-email-generator.py`.|
| `INSTITUTION`| Contains fields to include institutional name in filenames, and, for TDR institutions, changing which institution to generate the report for. |
| `EXCLUDED` | Contains a custom list of people names for systematically excluding any datasets with these names in the depositor or contact fields from any re-curation. Can be blank.|
| `PEOPLE_CONDITIONAL` | Similar to the above field, but re-curation is conditional on both the occurrence of these names in either field and other metadata fields. Can be blank. |
| `TOGGLES`| Contains seven toggles that control different parts of the workflow. `test_remediate`: only for `dataset-metadata-remediation.py`, *TRUE* to create a small sample size for testing the actual re-curation process. `test_email`: only for `dataset-email-generator.py`, *TRUE* to create a small sample size for testing email design/drafting. `draft_email`: only for `dataset-email-generator.py`, *TRUE* to create email drafts in an Outlook inbox (if false, it will just run the pre-processing steps). `json_retrieval`: only for `dataset-metadata-updater.py`, *TRUE* to retrieve the current metadata for datasets. `ror_plugin_enabled`: (ideally) a temporary toggle for the edge case scenario in which a dataverse previously had the ROR plug-in enabled, disabled it, and intends to re-enable it. *TRUE* if plug-in is active. This is only relevant for ROR re-curation. `only_my_institution`: only for TDR institutions, *TRUE* to retrieve metadata for only one institution versus all institutions. `split_institution_output`: only for TDR institutions, *TRUE* to split outputs by institution when all institutions were queried.|
| `RECURATION`| Contains seven toggles that control whether to flag and remediate different metadata attributes; in order: ORCID presence/absence, ROR presence/absence, author name formatting, keyword formatting, title punctuation (extra spaces or terminal periods), related works, and license. The first five are remediations - the workflow both flags missing/malformatted entries and fixes them - while the last two are flags - the workflow flags something for manual review. *TRUE* to enable a flag/remediation.|
| `VARIABLES`| Contains parameters for the Dataverse API. These likely do not be adjusted (and page start and page increment should not be changed). The only ones that may warrant changing are `dataverse_test`, which controls the size of the retrieval for a test run.|

## Development
This workflow is intended for additional development in order to catch additional forms of malformatted metadata that can be programmatically detected and remediated. 

## Versions
* **Version 1.1.0** adds a two new scripts, one for testing with a sandbox Dataverse and the other for summarizing/graphing the results of metadata re-curation. Additional logging functionality is also added.
* **Version 1.0.2** makes several minor bug fixes to handle issues identified after the first production run by UT Austin on March 26, 2026. It also adds functionality to identify datasets without any funding metadata and functionality to write records that fail to upload through the API due to legacy metadata validation failures.
* **Version 1.0.1** makes several minor bug fixes to handle issues identified after the first production run by UT Austin on March 26, 2026. 