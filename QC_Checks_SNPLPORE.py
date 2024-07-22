"""
QC_Checks_SNPLPORE.py
QC_Checks Methods/Functions to be used for Snowy Plover PORE Quality Control Validation

"""
#Import Required Libraries
import pandas as pd
import glob, os, sys

import QC_Checks as qc
import generalDM as dm


class qcProtcol_SNPLPORE:

    def __init__(self):
        """
        Define the instantiated QC Protocol instantiation attributes

        :param TBD
        :return: zzzz
        """
        # Class Variables

        numqcProtcol_SNPLPORE = 0

        #Name of the select query in the Access Front End
        self.filterRecQuery = 'qsel_QA_Control'

        numqcProtcol_SNPLPORE += 1

    def createYearlyRecs(qcCheckInstance):
        """
        Create the filter on Year with the records to be processed.  Frr SNPL PORE this is the 'qsel_QA_Control' query.

        :param qcCheckInstance: QC Check Instance

        :return: yearlyRecDF: Data Frame defining the records to be processed for the year
                 inQuery: Query to be pushed back to the backend as 'qsel_QA_Control'
        """

        yearLU = qcCheckInstance.yearLU
        inDBBE = qcCheckInstance.inDBBE

        inQuery = (f'SELECT tbl_Events.Event_ID, tbl_Locations.Location_ID, tbl_Locations.Loc_Name, tbl_Events.Start_Date,'
                   f' Year([Start_Date]) AS [Year], tbl_Events.QCFlag, tbl_Events.QCNotes, tbl_Events.Start_Time, '
                   f'tbl_Events.End_Time, tbl_Events.Created_Date, tbl_Events.Created_By, tbl_Events.Verified_Date,'
                   f' tbl_Events.Verified_By, tbl_Events.Updated_Date, tbl_Events.Updated_By,'
                   f' tbl_Events.DataProcessingLevelID, tbl_Events.DataProcessingLevelDate, '
                   f' tbl_Events.DataProcessingLevelUser FROM tbl_Locations INNER JOIN tbl_Events ON'
                   f' tbl_Locations.Location_ID = tbl_Events.Location_ID'
                   f' WHERE (((Year([Start_Date])) = {yearLU} Or (Year([Start_Date])) Is Null));')

        yearlyRecDF = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, inDBBE)

        return yearlyRecDF, inQuery

    def processQuery(queryName_LU, queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Iterate through the defined queries

        :param queryName_LU: Name of query routine being processes this is query name in the 'Query_Name' field in
         table 'tbl_QCQueries'
        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name


        :return: Pushed
        """
        try:
            if queryName_LU == "qa_a102_Unverified_Events":
                 qcProtcol_SNPLPORE.qa_a102_Unverified_Events(queryName_LU, queryDecrip_LU, yearlyRecDF, qcCheckInstance,
                                                              dmInstance)

            elif queryName_LU == "qa_f112_Incomplete_Weather":
                qcProtcol_SNPLPORE.qa_f112_Incomplete_Weather(queryName_LU, queryDecrip_LU, yearlyRecDF, qcCheckInstance
                                                              , dmInstance)
            else:
                logMsg = f'Query - {queryName_LU} - is not defined - existing script'
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                exit()

            #For all QC queries update the 'tbl_QA_Results
            qc.qcChecks.updateQAResultsTable(dmInstance.logFile, queryDecrip_LU, qcCheckInstance, dmInstance)

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            return f'Success - processQuery - {queryName_LU}'


    def qa_a102_Unverified_Events(queryName_LU, queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_a102_Unverified_Events

        :param queryName_LU: Name of query routine being processes this is query name in the 'Query_Name' field in
         table 'tbl_QCQueries'
        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: Pushed
        """

        #Single Query Check
        queryName_LU = 'qa_a102_Unverified_Events'

        #inQuerySel = f"""SELECT qsel_QA_Control.Event_ID, qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name, qsel_QA_Control.QCFlag, qsel_QA_Control.QCNotes, tlu_Data_Processing_Level.Label AS DataProcessingLevel, "frm_Data_Entry" AS varObject, "[Event_ID] = '" & [tbl_Events].[Event_ID] & "'" AS VarFilter, "" AS varSubObject, "" AS varSubFilter FROM qsel_QA_Control LEFT JOIN tlu_Data_Processing_Level ON qsel_QA_Control.DataProcessingLevelID = tlu_Data_Processing_Level.DataProcessingLevelID WHERE (((qsel_QA_Control.DataProcessingLevelID)<2 Or (qsel_QA_Control.DataProcessingLevelID) Is Null)) ORDER BY qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name;"""
        inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name,"
                      f" qsel_QA_Control.QCFlag, qsel_QA_Control.QCNotes, tlu_Data_Processing_Level.Label AS"
                      f" DataProcessingLevel, 'frm_Data_Entry' AS varObject, 'tbl_Events' AS RecTable, 'Event_ID' AS"
                      f" RecField, qsel_QA_Control.Event_ID AS RecValue FROM qsel_QA_Control LEFT JOIN"
                      f" tlu_Data_Processing_Level ON qsel_QA_Control.DataProcessingLevelID ="
                      f" tlu_Data_Processing_Level.DataProcessingLevelID WHERE"
                      f" (((qsel_QA_Control.DataProcessingLevelID)<2 Or (qsel_QA_Control.DataProcessingLevelID)"
                      f" Is Null)) ORDER BY qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name;")

        #####################################################################
        # Below are needed for all queries - Push Query, Updated Description, Append/Update to tbl_QA_Results
        #####################################################################

        # Push Yearly Records to Query back to Backend (i.e. qsel_QA_Control)
        qc.qcChecks.pushQueryToDB(inQuerySel, queryName_LU, qcCheckInstance, dmInstance)

        # Define the description for the created query
        dm.generalDMClass.queryDesc(queryName_LU, queryDecrip_LU, qcCheckInstance)


    def qa_f112_Incomplete_Weather(queryName_LU, queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_a102_Unverified_Events

        :param queryName_LU: Name of query routine being processes this is query name in the 'Query_Name' field in
         table 'tbl_QCQueries'
        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: Pushed
        """

        #Single Query Check
        queryName_LU = 'qa_f112_Incomplete_Weather'

        inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name, "
                      f"tbl_Event_Details.QCFlag AS EventDetailsQCFlag, tbl_Event_Details.QCNotes AS "
                      f"EventDetailsQCNotes, tbl_Event_Details.Incomplete_Survey, tbl_Event_Details.Wind_Spd, "
                      f"tbl_Event_Details.Wind_Max, tbl_Event_Details.Wind_Dir, tbl_Event_Details.Air_Temp, "
                      f"tbl_Event_Details.Rel_Hum, tbl_Event_Details.Cloud_Cover, tbl_Event_Details.Event_Notes, "
                      f"'frm_Data_Entry' AS varObject, 'tbl_Event_Details' AS RecTable, 'Event_ID' AS RecField,"
                      f" tbl_Event_Details.Event_ID AS RecValue FROM qsel_QA_Control LEFT JOIN tbl_Event_Details ON"
                      f" qsel_QA_Control.Event_ID = tbl_Event_Details.Event_ID WHERE"
                      f" (((tbl_Event_Details.Incomplete_Survey)=False) AND ((tbl_Event_Details.Wind_Spd) Is Null))"
                      f" OR (((tbl_Event_Details.Incomplete_Survey)=False) AND ((tbl_Event_Details.Wind_Max) Is Null))"
                      f" OR (((tbl_Event_Details.Incomplete_Survey)=False) AND ((tbl_Event_Details.Wind_Dir) Is Null))"
                      f" OR (((tbl_Event_Details.Incomplete_Survey)=False) AND ((tbl_Event_Details.Air_Temp) Is Null))"
                      f" OR (((tbl_Event_Details.Incomplete_Survey)=False) AND ((tbl_Event_Details.Rel_Hum) Is Null))"
                      f" OR (((tbl_Event_Details.Incomplete_Survey)=False) AND "
                      f" ((tbl_Event_Details.Cloud_Cover) Is Null))"
                      f" ORDER BY qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name;")

        #####################################################################
        # Below are needed for all queries - Push Query, Updated Description, Append/Update to tbl_QA_Results
        #####################################################################

        # Push Yearly Records to Query back to Backend (i.e. qsel_QA_Control)
        qc.qcChecks.pushQueryToDB(inQuerySel, queryName_LU, qcCheckInstance, dmInstance)

        # Define the description for the created query
        dm.generalDMClass.queryDesc(queryName_LU, queryDecrip_LU, qcCheckInstance)