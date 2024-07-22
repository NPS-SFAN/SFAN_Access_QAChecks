"""
QC_Checks.py
QC_Checks Methods/Functions to be used for general Quality Control Validation workflow.
"""
#Import Required Dependices
import pandas as pd
import glob, os, sys
import generalDM as dm
import QC_Checks_SNPLPORE as SNPLP

class qcChecks:

    #Class Variables
    numqcChecksInstances = 0

    def __init__(self, protocol, inDBBE, inDBFE, yearLU, inUser):
        """
        Define the instantiated loggerFile attributes

        :param protocol: Name of the Protocol being processes
        :param inDBBE: Protocol Backend Access database full path
        :param inDBFE: Protocol Frontend Access database full path
        :param yearLU: Year being processed
        :param inUser: NPS UserNam

        :return: instantiated self object
        """

        self.protocol = protocol
        self.inDBBE = inDBBE
        self.inDBFE = inDBFE
        self.yearLU = yearLU
        self.inUser = inUser

        #Update the Class Variable
        qcChecks.numqcChecksInstances += 1

    def process_QCRequest(qcCheckInstance, dmInstance):

        """
        General Quality Control workflow processing workflow steps.
        Workflow will iterate throug the queries by protocol defined in the 'tbl_QCQuries' table

        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return:
        """

        # Create the data management instance to  be used to define the logfile path and other general DM attributes
        qcProtocolInstance = SNPLP.qcProtcol_SNPLPORE()

        # Get the Subset of records for the year
        outMethod = SNPLP.qcProtcol_SNPLPORE.createYearlyRecs(qcCheckInstance)
        yearlyRecDF = outMethod[0]
        inQuerySel = outMethod[1]

        filterQueryName = qcProtocolInstance.filterRecQuery
        # Push Yearly Records to be used in the QC routines back to Backend (i.e. qsel_QA_Control)
        # Only need to do this once per year being processed
        qcChecks.pushQueryToDB(inQuerySel, filterQueryName, qcCheckInstance, dmInstance)

        #Define the Queries to process
        outDFQueries = qcChecks.define_QCQueries(qcCheckInstance)

        # Iterate through the QC Queries defined in 'tbl_QCQueries' via outDFQueries
        for index, row in outDFQueries.iterrows():
            queryName_LU = row.get('QueryName')
            queryDecrip_LU = row.get('QueryDescription')
            if qcCheckInstance.protocol.lower() == 'snplpore':

                #Process each QC Routine
                processQuery = SNPLP.qcProtcol_SNPLPORE.processQuery(queryName_LU, queryDecrip_LU, yearlyRecDF,
                                                                     qcCheckInstance, dmInstance)

            elif qcCheckInstance.protocol.lower() == 'salmonids': #To Be Developed 7/16/2024
                print('Test')

            else:
                logMsg = (f'WARNING - {qcCheckInstance.protocol} - is not defined - method QC_Checks.process_QCRequest '
                          f'- Exiting script')
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                exit()


            # Message QC Check Completed
            logMsg = f'Successfully Finished QC Check Script for - {qcCheckInstance.protocol} - {queryName_LU}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)

    def define_QCQueries(qcCheckInstance):
        """
        Define the QC Queries to be processed pulling from the 'tbl_QCQueries' table

        :param qcCheckInstance: QC Check Instance

        :return:outDFQueries: dataframe with the 'tbl_QCQueries' table (i.e. queries to be processed
        """
        # Define Protocol Specific QC Queries
        inQuery = "Select * FROM tbl_QCQueries"

        inDBBE = qcCheckInstance.inDBBE
        outDFQueries = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, inDBBE)

        return outDFQueries

    def pushQueryToDB(inQuerySel, queryName, qcCheckInstance,dmInstance):
        """
        Push the passed SQL Query to the defined output query

        :param inQuerySel: SQL Query defining the query to be pushed back to the backend instance
        :param queryName: Name of query being pushed, will deleted first if exists
        :param qcCheckInstance: QC Check Instance (has Database paths, etc
        :param dmInstance: Data Management Instance

        :return existsLU: String defining if query exists ('Yes') or not ('No')
        """

        #Check if query exists first - if yes delete
        dm.generalDMClass.queryExistsDelete(queryName=queryName, inDBPath=qcCheckInstance.inDBFE)

        # Push the new query
        dm.generalDMClass.pushQuery(inQuerySel=inQuerySel, queryName=queryName, inDBPath=qcCheckInstance.inDBFE)
        #Note Using ODBC connect this requires ODBC Driver to be in place, using PYWIN32 instead.
        ####dm.generalDMClass.pushQueryODBC(inQuerySel=inQuerySel, queryName=queryName, inDBPath=qcCheckInstance.inDBFE)

        logMsg = f'Successfully pushed Query - {queryName} - to Front End Database - {qcCheckInstance.inDBFE}'
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)


    def updateQAResultsTable(queryName, queryDecrip_LU, qcCheckInstance,dmInstance):
        """
        Perform append or update query to table 'tbl_QA_Results' which resides in the SFAN Backend Databases.
        Checking queryName with '_PY" suffix added - will remove once done with development.
        :param queryName: Name of query being pushed, will deleted first if exists
        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param qcCheckInstance: QC Check Instance (has Database paths, etc
        :param dmInstance: Data Management Instance

        :return
        """

        #Read query into a dataframe allowing for creation of the dataframe to append or update
        inQuery = f'Select * FROM {queryName}'
        outDF = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, qcCheckInstance.inDBFE)

        #Create the Summary of the output Query to be pushed to 'tbl_QA_Results' - below are fields in the
        # 'tbl_QA_Results' table

        Query_Name = queryName
        Time_Frame = str(qcCheckInstance.yearLU)
        Query_Type = queryName[6]
        Query_Result = str(outDF.shape[0])

        from datetime import datetime
        now = datetime.now()
        Query_Run_Time = now.strftime('%m/%d/%y %H:%M:%S')
        Query_Description = queryDecrip_LU
        QA_User = qcCheckInstance.inUser
        Is_Done = 0
        Data_Scope = 0

        # Determine if record for 'Query_Name' and 'Time_Frame' already exists. If yes - update, else append
        # Read query into a dataframe allowing for creation of the dataframe to append or update
        inQuery = (f"Select COUNT(*) AS RecCount FROM tbl_QA_Results WHERE [Query_Name] = '{queryName}' AND" 
                   f" [Time_Frame] ='{Time_Frame}'")

        outCountDF = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, qcCheckInstance.inDBFE)
        countValue = outCountDF['RecCount'][0]

        if countValue >= 1:  # Already exists
            type = 'Update'
            #Update record in 'tbl_QA_Results'
            inQuery = (f"UPDATE tbl_QA_Results SET tbl_QA_Results.Query_Type = {Query_Type}, tbl_QA_Results.Query_Result"
                        f" = {Query_Result}, tbl_QA_Results.Query_Run_Time = #{Query_Run_Time}#, tbl_QA_Results.Query_Description"
                        f" = '{Query_Description}', tbl_QA_Results.QA_User = '{QA_User}',"
                        f" tbl_QA_Results.Is_Done = 0, tbl_QA_Results.Data_Scope = 0 WHERE"
                        f" tbl_QA_Results.Query_Name = '{queryName}' AND tbl_QA_Results.Time_Frame = '{Time_Frame}';")

        else: #Append New Record to 'tbl_QA_Results'
            type = 'Append'
            inQuery = (f"INSERT INTO tbl_QA_Results ( Query_Name, Time_Frame, Query_Type, Query_Result, Query_Run_Time,"
                       f" Query_Description, QA_User, Is_Done, Data_Scope) SELECT '{Query_Name}' AS Query_Name, {Time_Frame} AS"
                       f" Time_Frame, {Query_Type} AS Query_Type, {Query_Result} AS Query_Result, #{Query_Run_Time}# AS"
                       f" Query_Run_Time, '{Query_Description}' AS Query_Description, '{QA_User}' AS QA_User, "
                       f" {Is_Done} AS Is_Done, {Data_Scope} AS DataScope;")

        #Push the Update or Append Query
        dm.generalDMClass.excuteQuery(inQuery, qcCheckInstance.inDBBE)

        logMsg = f"Success for QC Check - {queryName} - {type} made to table tbl_QA_Results - in {qcCheckInstance.inDBBE}"
        dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
