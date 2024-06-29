# Molecules_Project

-Interactive chembl API docs: https://www.ebi.ac.uk/chembl/api/data/docs

-To fetch chembl_id_lookup responce from api in json format: https://www.ebi.ac.uk/chembl/api/data/chembl_id_lookup.json?limit=1000&offset=0

-To fetch chembl_id_lookup responce from api in json format: 
https://www.ebi.ac.uk/chembl/api/data/molecule.json?limit=1000&offset=0

Note: it's possible to fetch responce in json, csv, xml and some other formats!
-
-ChemBL database schema:
https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest/chembl_34_schema.png

we need to ingest from it 4 tables:
1) chembl_id_lookup
2) molecule_dictionary
3) compound_properties
4) compound_structures
