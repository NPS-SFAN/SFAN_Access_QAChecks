# SFAN_Access_QAChecks
Repository with workflow to perform annual data validation routines within protocol access databases.  Performs the SQL based queries from python that are pushed to the protocol Access database.  The Pythhon SQL queries are performing automoated data quality flagging and work in conjunction with exisitng QC data validation front end forms.  Script on Object Oriented in an effort to be transferrable and usable across all SFAN Protocols where there is an existing annual Data Validations query and form workflow in place.  

With the workflow being done in a python environment this is intended to be a more efficient approach for updating of exisitng data validation routines to include autoflagging, with out being done completely within Access has either hard coded queries or Visual Basic queries within Access.


AS of August 5th, 2024 workflow has been defiend for SNPL GOGA annual data validation autoflagging routines.  
