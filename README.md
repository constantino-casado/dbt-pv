# dbt-pv
Scripts for moving dbt lineage information into Purview. It can generate products information in purview (Metamodel data products))

## Purpose
This tool will allow to push data into purview when:
- dbt is executed and then lineage is caputred
- Anotations on Data Products will be pushed into Purview
- Relations between entities and data product is automatically gathered

## Script subprocess logic

The Script will perfrom the following actions:
1. Prepare execution (check if everything is ready)
2. Trigger dbt Entities generation
3. generate dbt docs
4. Detect lineage and write into _pv_folder
5. Detect products and write into _pv_folder
6. Push data from _pv_folder into Purview REST APIs

## Deployment
- Requires python 3.7 or later
- Requires a service principal (clientID and secret) with permissions to write in Purview

## Prerequisites
The following are requisites to make dbt and purview APIs work
- A purview account with a service principal
- Service principal ClientID and Secret as environment variables:
    -   PVClientID
    -   PVSecret

## Howto Run it
The script will run manually or triggered by some flavour of tool, orchestrator, or similar. 

## Script parameters
target => '-t', '--target'
project_dir => '--project-dir' || default value = './'
profile_name => '--profile'
