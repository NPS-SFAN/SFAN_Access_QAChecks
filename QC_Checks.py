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

    def __init__(self, protocol, inDBBE, inDBFE, yearLU):
        """
        Define the instantiated loggerFile attributes

        :param protocol: Name of the Protocol being processes
        :param inDBBE: Protocol Backend Access database full path
        :param inDBFE: Protocol Frontend Access database full path
        :param yearLU: Year being processed

        :return: instantiated self object
        """

        self.protocol = protocol
        self.inDBBE = inDBBE
        self.inDBFE = inDBFE
        self.yearLU = yearLU

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

        #Define the Queries to process
        outDFQueries = qcChecks.define_QCQueries(qcCheckInstance)

        # Iterate through the QC Queries defined in 'tbl_QCQueries' via outDFQueries
        for index, row in outDFQueries.iterrows():
            queryName_LU = row.get('QueryName')

            if qcCheckInstance.protocol.lower() == 'snplpore':

                # Create the data management instance to  be used to define the logfile path and other general DM attributes
                qcProtocolInstance = SNPLP.qcProtcol_SNPLPORE()

                #Get the Subset of records for the year
                outMethod = SNPLP.qcProtcol_SNPLPORE.createYearlyRecs(qcCheckInstance)
                yearlyRecDF = outMethod[0]
                inQuerySel = outMethod[1]

                filterQueryName = qcProtocolInstance.filterRecQuery
                #Push Yearly Records to Query back to Backend (i.e. qsel_QA_Control)
                outNewRecQuery = qcChecks.pushQueryToDB(inQuerySel, filterQueryName, qcCheckInstance)

                #Process each QC Routine
                processQuery = SNPLP.qcProtcol_SNPLPORE.processQuery(queryName_LU, yearlyRecDF, qcCheckInstance, dmInstance)

            elif qcCheckInstance.protocol.lower() == 'salmonids': #To Be Developed 7/16/2024
                print('Test')

            else:
                logMsg = f'WARNING - {qcCheckInstance.protocol} - is not defined - method QC_Checks.process_QCRequest - Exiting script'
                dm.generalDMClass.messageLogFile(self=dmInstance, logMsg=logMsg)
                exit()


            # Message QC Check Completed
            logMsg = f'Successfully Finished QC Check Script for - {qcCheckInstance.protocol} - {queryName_LU}'
            dm.generalDMClass.messageLogFile(self=dmInstance, logMsg=logMsg)

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

    def pushQueryToDB(inQuerySel, queryName, qcCheckInstance):
        """
        Push the passed SQL Query to the defined output query

        :param inQuerySel: SQL Query defining the query to be pushed back to the backend instance
        :param queryName: Name of query being pushed, will deleted first if exists
        :param qcCheckInstance: QC Check Instance (has Database paths, etc


        :return existsLU: String defining if query exists ('Yes') or not ('No')
        """

        #Check if query exists first - if yes delete
        dm.generalDMClass.QueryExistsDelete(queryName=queryName, inDBPath=qcCheckInstance.inDBFE)

        # Create the query
        print ('Add new query method here')


        # Close the connection

