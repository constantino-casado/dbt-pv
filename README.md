# dbt-pv
Scripts for moving dbt lineage information into Purview. It can generate products information in purview (Metamodel data products))

## Purpose
This tool will allow to push data into purview when:
- dbt is executed and then lineage is caputred
- Anotations on Data Products will be pushed into Purview
- Relations between entities and data product is automatically gathered
