"""
QC_Checks.py
QC_Checks Methods/Functions to be used for general Quality Control Validation workflow.
"""
#Import Required Dependices
import pandas as pd
import glob, os, sys, traceback
import generalDM as dm
import QC_Checks_SNPLPORE as SNPLP
import logging
import log_config

logger = logging.getLogger(__name__)
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

        #Configure Logging:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

        try:
            # Iterate through the QC Queries defined in 'tbl_QCQueries' via outDFQueries
            for index, row in outDFQueries.iterrows():
                queryName_LU = row.get('QueryName')
                queryDecrip_LU = row.get('QueryDescription')
                if qcCheckInstance.protocol.lower() == 'snplpore':

                    #Process each QC Routine
                    SNPLP.qcProtcol_SNPLPORE.processQuery(queryName_LU, queryDecrip_LU, yearlyRecDF,
                                                                         qcCheckInstance, dmInstance)

                elif qcCheckInstance.protocol.lower() == 'salmonids': #To Be Developed 7/16/2024
                    print('Test')

                else:
                    logMsg = (f'WARNING - {qcCheckInstance.protocol} - is not defined - method QC_Checks.process_QCRequest '
                              f'- Exiting script')
                    dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                    logging.warning(logMsg)
                    exit()

                # Message QC Check Completed
                logMsg = f'Successfully Finished QC Check Script for - {qcCheckInstance.protocol} - {queryName_LU}'
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.info(logMsg)

        except Exception as e:

            logMsg = (f'ERROR - An error occurred process_QCRequest: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

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
        #Not Using ODBC connect this requires ODBC Driver to be in place, using PYWIN32 instead.
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
        try:
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
            logging.info(logMsg)

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in UpdateQAResultsTable: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

    def applyQCFlag(queryName_LU, flagDefDf, qcCheckInstance, dmInstance):
        """
        Routine to apply the Quality Control flag to the underlying table per Quality Control check.  Information is
        being pulled from the 'tbl_QCQueries' table in the backend database.  Processing creates a temporary table
        in the backend dataabase with the records in need of data flag to be applied.  From this temporary table
        and the information in 'tbl_QCQueries' the join table (e.g. tbl_Events, etc.), join field (e.g. 'Event_ID'),
        field to apply the flag to (e.g. QCFlag) and flag value to apply (e.g. DFO, etc.) are pushed.
        If the flag already exists the flag will not be pushed. Existing flags are not overwritten with new flags
        being concatenated.

        :param queryName_LU: QueryName being processed
        :param flagDefDf: Data frame of the Flags Dictionary defining the flag field information
        :param qcCheckInstance: QC Check Instance (has Database paths, etc
        :param dmInstance: Data Management Instance

        :return
        """
        try:

            # 1a) Read record for the query in 'tbl_QCQueries
            inQuery = f"SELECT * FROM tbl_QCQueries WHERE [QueryName] = '{queryName_LU}';"
            inDBFE = qcCheckInstance.inDBFE
            outDFQCFields = dm.generalDMClass.connect_to_AcessDB_DF(query=inQuery, inDB=inDBFE)
            #Get Flag Code to Apply
            qcFlag_LU = outDFQCFields.loc[0, 'QCFlag']
            #Get Flag Field in Source Table - QC Flag will be applied here
            qcFlagFieldTable_LU = outDFQCFields.loc[0, 'FlagFieldTable']
            #Get Flag Field in the Summary Query (i.e. field in the 'queryName_LU'), will be checking this field
            #to see if the QC Flag is present
            qcFlagFieldQuery_LU = outDFQCFields.loc[0, 'FlagFieldQuery']

            inQuerySel = f"SELECT * FROM {queryName_LU}"
            # 1b) Read in the Final Query
            outDFwRecs = dm.generalDMClass.connect_to_AcessDB_DF(query=inQuerySel, inDB=inDBFE)

            # 2) Apply the QC Flag where 'DFO' doesn't exists
            outDFNoFlag = outDFwRecs[~outDFwRecs[qcFlagFieldQuery_LU].str.contains(qcFlag_LU, na=False)]
            recToFlag = len(outDFNoFlag)
            #Delete Temp Table (if exists) and apply the QC Flag if number of records >0, else no new flagging requried.
            if len(outDFNoFlag) > 0:
                # Delete Temp Table (tmpQCTable) if Exists
                dm.generalDMClass.tableExistsDelete(tableName= 'tmpQCTable', inDBPath=qcCheckInstance.inDBBE)

                # Create the Temporary Table and Populate with the dataframe records without the records
                dm.generalDMClass.createTableFromDF(outDFNoFlag, 'tmpQCTable', qcCheckInstance.inDBBE)

                #Define the Update Query no Existing QC Flag (e.g. DFO) for this iteration
                flagTable_LU = outDFQCFields.loc[0, 'FlagTable']
                joinField_LU = outDFQCFields.loc[0, 'JoinField']

                inQuery = (f"UPDATE tmpQCTable INNER JOIN {flagTable_LU} ON tmpQCTable.{joinField_LU} ="
                           f" {flagTable_LU}.{joinField_LU} SET {flagTable_LU}.{qcFlagFieldTable_LU} = IIf(IsNull([{qcFlagFieldTable_LU}])"
                           f",'{qcFlag_LU}',[{qcFlagFieldTable_LU}] & ';{qcFlag_LU}');")

                # Apply the QC Flag
                inDBBE = qcCheckInstance.inDBBE
                dm.generalDMClass.excuteQuery(inQuery=inQuery, inDBBE=inDBBE)

                logMsg = f'Applied QC Flag to {recToFlag} records without EXISTING QC Flag - {qcFlag_LU} - {queryName_LU}'
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.info(logMsg)
            else:
                outDFWithFlag = outDFwRecs[outDFwRecs[qcFlagFieldQuery_LU].str.contains(qcFlag_LU, na=False)]
                recAlreadyFlagged = len(outDFWithFlag)

                logMsg = (f'No New QC Flags applied - {queryName_LU} - Number of records already flagged '
                          f'was {recAlreadyFlagged}')
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.info(logMsg)

        except Exception as e:

            logMsg = f'ERROR - An error occurred in UpdateQAResultsTable: {e}'
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.critical(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)

if __name__ == "__name__":
    logger.info("Running QC_Checks.py")
