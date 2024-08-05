# SFAN_Access_QAChecks
Repository with workflow to perform annual data validation routines within San Francisco Bay Area Network (SFAN) protocol Access Databases.  Performs the SQL based queries from python that are pushed to the protocol Access database.  The Pythhon SQL queries are performing automoated data quality flagging and work in conjunction with exisitng QC data validation front end forms.  Script on Object Oriented in an effort to be transferrable and usable across all SFAN Protocols where there is an existing annual Data Validations query and form workflow in place.  

With the workflow being done in a python environment this is intended to be a more efficient approach for updating of exisitng data validation routines to include autoflagging, with out being done completely within Access has either hard coded queries or Visual Basic queries within Access.


AS of August 5th, 2024 workflow has been defiend for SNPL GOGA annual data validation autoflagging routines.  

## QC_Checks_SNPLPORE.py
Methods/Functions to be used for Snowy Plover PORE Quality Control Data Validation workflow.

## QC_Checks.py
Methods/Functions to be used for general (across multiple protocols) Quality Control Validation workflow.

## generalDM.py
General Data Management workflow related methods.  Consider migrating this to a more general SFAN Data Management module.

## log_config.py
Script log file configuration script used for use in the SFAN_Access_QAChecks repository
