"""
QC_Checks_SNPLPORE.py
Methods/Functions to be used SNPL PORE Quality Control Validation workflow.

"""
#Import Required Libraries
import pandas as pd
import glob, os, sys
import traceback
import QC_Checks as qc
import generalDM as dm
import logging

logger = logging.getLogger(__name__)

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

        :return:
        """

        try:
            if queryName_LU == "qa_a102_Unverified_Events_X":
                outFun = qcProtcol_SNPLPORE.qa_a102_Unverified_Events(queryDecrip_LU, yearlyRecDF, qcCheckInstance,
                                                                      dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_f112_Incomplete_Weather_X":
                outFun = qcProtcol_SNPLPORE.qa_f112_Incomplete_Weather(queryDecrip_LU, yearlyRecDF,
                                                                                       qcCheckInstance, dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_f122_CompleteSurvey_IncompleteSNPL_X":
                outFun = qcProtcol_SNPLPORE.qa_f122_CompleteSurvey_IncompleteSNPL(queryDecrip_LU,
                                                                                                  yearlyRecDF,
                                                                                                  qcCheckInstance,
                                                                                                  dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_f132_MoreCheckedSNPL_ThanTotal_X":
                outFun = qcProtcol_SNPLPORE.qa_f132_MoreCheckedSNPL_ThanTotal(queryDecrip_LU,
                                                                                  yearlyRecDF,
                                                                                  qcCheckInstance,
                                                                                  dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_f142_MoreBandedSNPL_ThanChecked_X":
                outFun = qcProtcol_SNPLPORE.qa_f142_MoreBandedSNPL_ThanChecked(queryDecrip_LU, yearlyRecDF,
                                                                              qcCheckInstance, dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_f152_StopTime_MoreThanEvent_X":
                outFun = qcProtcol_SNPLPORE.qa_f152_StopTime_MoreThanEvent(queryDecrip_LU, yearlyRecDF,
                                                                           qcCheckInstance, dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_h102_Missing_Observers_X":
                outFun = qcProtcol_SNPLPORE.qa_h102_Missing_Observers(queryDecrip_LU, yearlyRecDF,
                                                                      qcCheckInstance, dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_j102_SNPL_ObservationTime_Error_X":
                outFun = qcProtcol_SNPLPORE.qa_j102_SNPL_ObservationTime_Error(queryDecrip_LU, yearlyRecDF,
                                                                               qcCheckInstance, dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_j112_Mismatched_SNPL_Numbers":
                outFun = qcProtcol_SNPLPORE.qa_j112_Mismatched_SNPL_Numbers(queryDecrip_LU, yearlyRecDF,
                                                                               qcCheckInstance, dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_j122_Mismatched_Banded_Numbers":
                outFun = qcProtcol_SNPLPORE.qa_j122_Mismatched_Banded_Numbers(queryDecrip_LU, yearlyRecDF,
                                                                               qcCheckInstance, dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_j132_NestID_Year_Mismatch_X":
                outFun = qcProtcol_SNPLPORE.qa_j132_NestID_Year_Mismatch(queryDecrip_LU, yearlyRecDF,
                                                                               qcCheckInstance, dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_j142_Missing_Band_Totals_X":
                outFun = qcProtcol_SNPLPORE.qa_j142_Missing_Band_Totals(queryDecrip_LU, yearlyRecDF,
                                                                               qcCheckInstance, dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_j152_Missing_Band_Data_X":
                outFun = qcProtcol_SNPLPORE.qa_j152_Missing_Band_Data(queryDecrip_LU, yearlyRecDF,
                                                                               qcCheckInstance, dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_j162_Mismatched_Band_Obs":
                outFun = qcProtcol_SNPLPORE.qa_j162_Mismatched_Band_Obs(queryDecrip_LU, yearlyRecDF,
                                                                               qcCheckInstance, dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_j172_Mismatched_Band_Summary":
                outFun = qcProtcol_SNPLPORE.qa_j172_Mismatched_Band_Summary(queryDecrip_LU, yearlyRecDF,
                                                                               qcCheckInstance, dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            elif queryName_LU == "qa_j182_Predator_ActivityType_X":
                outFun = qcProtcol_SNPLPORE.qa_j182_Predator_ActivityType(queryDecrip_LU, yearlyRecDF,
                                                                            qcCheckInstance, dmInstance)
                inQuerySel = outFun[0]
                flagFieldsDic = outFun[1]

            else:
                logMsg = f'Query - {queryName_LU} - is not defined - existing script'
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
                logging.error(logMsg, exc_info=True)
                exit()

            #####################################################################
            # Below are needed for all queries - Push Query, Updated Description, Append/Update to tbl_QA_Results
            #####################################################################

            # Push Yearly Records Query back to Backend (e.g. qa_f132_MoreCheckedSNPL_ThanTotal)
            qc.qcChecks.pushQueryToDB(inQuerySel, queryName_LU, qcCheckInstance, dmInstance)

            # Define the description for the created query
            dm.generalDMClass.queryDesc(queryName_LU, queryDecrip_LU, qcCheckInstance)

            #For all QC queries update the 'tbl_QA_Results
            qc.qcChecks.updateQAResultsTable(queryName_LU, queryDecrip_LU, qcCheckInstance, dmInstance)

            #Apply QC Flag if needed
            # Convert the Raster Dictionary to a Dataframe
            flagDefDf = pd.DataFrame.from_dict(flagFieldsDic, orient='columns')
            applyFlag = flagDefDf.loc[0, 'ApplyFlag']

            if applyFlag == 'Yes':
                qc.qcChecks.applyQCFlag(queryName_LU, flagDefDf, qcCheckInstance, dmInstance)
                logMsg = f"Success Applying QC Flags for  - {queryName_LU}"
                dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)

        except Exception as e:
            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - processQuery - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()

    def qa_a102_Unverified_Events(queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_a102_Unverified_Events. Shows records that have not been marked as
        verified.  No QC Flag applied, data processing level should be Raw

        :param queryName_LU: Name of query routine being processes this is query name in the 'Query_Name' field in
         table 'tbl_QCQueries'
        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:
            #Single Query Check
            queryName_LU = 'qa_a102_Unverified_Events'

            inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name,"
                          f" qsel_QA_Control.QCFlag, qsel_QA_Control.QCNotes, tlu_Data_Processing_Level.Label AS"
                          f" DataProcessingLevel, 'frm_Data_Entry' AS varObject, 'tbl_Events' AS RecTable, 'Event_ID' AS"
                          f" RecField, qsel_QA_Control.Event_ID AS RecValue FROM qsel_QA_Control LEFT JOIN"
                          f" tlu_Data_Processing_Level ON qsel_QA_Control.DataProcessingLevelID ="
                          f" tlu_Data_Processing_Level.DataProcessingLevelID WHERE"
                          f" (((qsel_QA_Control.DataProcessingLevelID)<2 Or (qsel_QA_Control.DataProcessingLevelID)"
                          f" Is Null)) ORDER BY qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name;")

            flagFieldsDic = {'ApplyFlag': ['No']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()

    def qa_f112_Incomplete_Weather (queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_f112_Incomplete_Weather. Returns complete surveys (not marked as
        incomplete) but are missing weather condition data.  QC default value is DFO.

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:
            #Single Query Check
            queryName_LU = 'qa_f112_Incomplete_Weather'

            inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name, "
                          f"tbl_Event_Details.QCFlag AS EventDetailsQCFlag, tbl_Event_Details.QCNotes AS "
                          f"EventDetailsQCNotes, tbl_Event_Details.Incomplete_Survey, tbl_Event_Details.Wind_Spd, "
                          f"tbl_Event_Details.Wind_Max, tbl_Event_Details.Wind_Dir, tbl_Event_Details.Air_Temp, "
                          f"tbl_Event_Details.Rel_Hum, tbl_Event_Details.Cloud_Cover, tbl_Event_Details.Event_Notes, "
                          f"'frm_Data_Entry' AS varObject, 'tbl_Events' AS RecTable, 'Event_ID' AS RecField,"
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

            # Define the flag fields in the 'tbl_Event_Details' table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()

    def qa_f122_CompleteSurvey_IncompleteSNPL(queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_f122_CompleteSurvey_IncompleteSNPL. Returns surveys not marked as
        incomplete but that are missing SNPL summary data. QC dafault value is DFO

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:
            # Single Query Check
            queryName_LU = 'qa_f122_CompleteSurvey_IncompleteSNPL'

            inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name,"
                          f" tbl_Event_Details.QCFlag AS EventDetailsQCFlag, tbl_Event_Details.QCNotes AS "
                          f" EventDetailsQCNotes, tbl_Event_Details.Incomplete_Survey, tbl_Event_Details.SNPL_Adults,"
                          f" tbl_Event_Details.SNPL_Hatchlings, tbl_Event_Details.SNPL_Fledglings,"
                          f" tbl_Event_Details.SNPL_Checked_Bands, tbl_Event_Details.SNPL_Banded,"
                          f" tbl_Event_Details.Event_Notes, 'frm_Data_Entry' AS varObject, 'tbl_Events' AS RecTable,"
                          f" 'Event_ID' AS RecField, tbl_Event_Details.Event_ID AS RecValue"
                          f" FROM qsel_QA_Control LEFT JOIN tbl_Event_Details ON qsel_QA_Control.Event_ID ="
                          f" tbl_Event_Details.Event_ID WHERE (((tbl_Event_Details.Incomplete_Survey)=False) AND"
                          f" ((tbl_Event_Details.SNPL_Adults) Is Null)) OR (((tbl_Event_Details.Incomplete_Survey)=False)"
                          f" AND ((tbl_Event_Details.SNPL_Hatchlings) Is Null)) OR (((tbl_Event_Details.Incomplete_Survey)="
                          f"False) AND ((tbl_Event_Details.SNPL_Fledglings) Is Null)) OR"
                          f" (((tbl_Event_Details.Incomplete_Survey)=False) AND ((tbl_Event_Details.SNPL_Checked_Bands) Is"
                          f" Null)) OR (((tbl_Event_Details.Incomplete_Survey)=False) AND ((tbl_Event_Details.SNPL_Banded)"
                          f" Is Null)) ORDER BY qsel_QA_Control.Start_Date DESC;")

            # Define the flag fields in the 'tbl_Event_Details' table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()


    def qa_f132_MoreCheckedSNPL_ThanTotal(queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_f132_MoreCheckedSNPL_ThanTotal. Returns surveys that have more SNPL
        checked for bands than the total  SNPL (SNPL_Adult, SNPL_Hatchling,SNPL_Fledgling). QC default value is LESPC.

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:
            # Single Query Check
            queryName_LU = 'qa_f132_MoreCheckedSNPL_ThanTotal'

            inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name,"
                          f" tbl_Event_Details.QCFlag AS EventDetailsQCFlag, tbl_Event_Details.QCNotes AS"
                          f" EventDetailsQCNotes, tbl_Event_Details.SNPL_Adults, tbl_Event_Details.SNPL_Hatchlings,"
                          f" tbl_Event_Details.SNPL_Fledglings, [SNPL_Adults]+[SNPL_Hatchlings]+[SNPL_Fledglings] AS"
                          f" TotalSNPL, tbl_Event_Details.SNPL_Checked_Bands, tbl_Event_Details.SNPL_Banded,"
                          f" tbl_Event_Details.Event_Notes, 'frm_Data_Entry' AS varObject, 'tbl_Events' AS"
                          f" RecTable, 'Event_ID' AS RecField, tbl_Event_Details.Event_ID AS RecValue"
                          f" FROM qsel_QA_Control INNER JOIN tbl_Event_Details ON qsel_QA_Control.Event_ID ="
                          f" tbl_Event_Details.Event_ID WHERE ((([SNPL_Adults]+[SNPL_Hatchlings]+[SNPL_Fledglings])<"
                          f"[SNPL_Checked_Bands])) ORDER BY qsel_QA_Control.Start_Date DESC;")

            # Define the flag fields in the 'tbl_Event_Details' table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()
    def qa_f142_MoreBandedSNPL_ThanChecked(queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_f142_MoreBandedSNPL_ThanChecked. Returns surveys that have more SNPL
        with bands (SNPL_Banded)  than were checked for bands (SNPL_Checked_Bands).  QC default value is LESPB.

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:
            # Single Query Check
            queryName_LU = 'qa_f142_MoreBandedSNPL_ThanChecked'

            inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name,"
                          f" tbl_Event_Details.QCFlag AS EventDetailsQCFlag, tbl_Event_Details.QCNotes AS"
                          f" EventDetailsQCNotes, tbl_Event_Details.SNPL_Adults, tbl_Event_Details.SNPL_Hatchlings,"
                          f" tbl_Event_Details.SNPL_Fledglings, tbl_Event_Details.SNPL_Checked_Bands,"
                          f" tbl_Event_Details.SNPL_Banded, tbl_Event_Details.Event_Notes, 'frm_Data_Entry' AS"
                          f" varObject, 'tbl_Events' AS RecTable, 'Event_ID' AS RecField,"
                          f" tbl_Event_Details.Event_ID AS RecValue FROM qsel_QA_Control INNER JOIN tbl_Event_Details"
                          f" ON qsel_QA_Control.Event_ID = tbl_Event_Details.Event_ID WHERE"
                          f" (((tbl_Event_Details.SNPL_Banded)>[SNPL_Checked_Bands])) ORDER BY"
                          f" qsel_QA_Control.Start_Date DESC;")

            # Define the flag fields in the 'tbl_Event_Details' table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()

    def qa_f152_StopTime_MoreThanEvent(queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_f142_MoreBandedSNPL_ThanChecked. Returns surveys where the predator
        stop time (minutes) is longer than the total survey event time (minutes). QC default value is LEPST.

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:
            # Single Query Check
            queryName_LU = 'qa_f152_StopTime_MoreThanEvent'

            inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name,"
                          f" tbl_Event_Details.QCFlag AS EventDetailsQCFlag, tbl_Event_Details.QCNotes AS "
                          f"EventDetailsQCNotes, tbl_Event_Details.Predtor_Survey, tbl_Event_Details.Predator_Notes, "
                          f"tbl_Event_Details.PredatorStop, tbl_Event_Details.Event_Notes, DateDiff('n',[Start_Time],"
                          f"[End_Time]) AS SurveyMinutes, qsel_QA_Control.Start_Time, qsel_QA_Control.End_Time, "
                          f"'frm_Data_Entry' AS varObject, 'tbl_Events' AS RecTable, 'Event_ID' AS RecField, "
                          f"tbl_Event_Details.Event_ID AS RecValue FROM qsel_QA_Control INNER JOIN tbl_Event_Details "
                          f"ON qsel_QA_Control.Event_ID = tbl_Event_Details.Event_ID "
                          f"WHERE (((DateDiff('n',[Start_Time],[End_Time]))<[PredatorStop])) ORDER BY "
                          f"qsel_QA_Control.Start_Date DESC;")

            # Define the flag fields in the 'tbl_Event_Details' table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()
    def qa_h102_Missing_Observers(queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_h102_Missing_Observers. Returns surveys where the predator
        stop time (minutes) is longer than the total survey event time (minutes). QC default value is LEPST.

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:
            # Single Query Check
            queryName_LU = 'qa_h102_Missing_Observers'

            inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, qsel_QA_Control.Start_Date, qsel_QA_Control.Loc_Name, "
                          f"qsel_QA_Control.QCFlag AS EventQCFlag, qsel_QA_Control.QCNotes AS EventQCNotes, "
                          f"xref_Event_Contacts.Contact_ID, 'frm_Data_Entry' AS varObject, 'tbl_Events' "
                          f"AS RecTable, 'Event_ID' AS RecField, tbl_Events.Event_ID AS RecValue "
                          f"FROM qsel_QA_Control LEFT JOIN xref_Event_Contacts ON qsel_QA_Control.Event_ID = "
                          f"xref_Event_Contacts.Event_ID WHERE (((xref_Event_Contacts.Contact_ID) Is Null));")

            # Define the flag fields in the FlagTable table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()

    def qa_j102_SNPL_ObservationTime_Error(queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_j102_SNPL_ObservationTime_Error. Returns SNPL observation records where
        the observation time is outside the start or end times of survey event. Time should be entered in 24 hour
        military format. QC default value is LEOT.

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:
            # Single Query Check
            queryName_LU = 'qa_j102_SNPL_ObservationTime_Error'

            inQuerySel = (f"SELECT  tbl_SNPL_Observations.SNPL_Data_ID, qsel_QA_Control.Event_ID, "
                          f"qsel_QA_Control.Loc_Name, tbl_SNPL_Observations.QCFlag AS "
                          f"SNPLObsQCFlag, tbl_SNPL_Observations.QCNotes AS SNPLObsQCNotes, qsel_QA_Control.Start_Date, "
                          f"qsel_QA_Control.Start_Time, qsel_QA_Control.End_Time, tbl_SNPL_Observations.SNPL_Time, "
                          f"'frm_Data_Entry' AS varObject, 'tbl_Events' AS RecTable, 'Event_ID' AS RecField, "
                          f"tbl_Events.Event_ID AS RecValue FROM qsel_QA_Control INNER JOIN tbl_SNPL_Observations ON "
                          f"qsel_QA_Control.Event_ID = tbl_SNPL_Observations.Event_ID "
                          f"WHERE (((tbl_SNPL_Observations.SNPL_Time)<[Start_Time] Or "
                          f"(tbl_SNPL_Observations.SNPL_Time)>[End_Time])) ORDER BY qsel_QA_Control.Start_Date DESC;")

            # Define the flag fields in the FlagTable table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()

    def qa_j112_Mismatched_SNPL_Numbers(queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_j112_Mismatched_SNPL_Numbers. Returns records where the entered
        summarized number of adults, hatchlings, or fledglings SNPL does not match the observed sum of adult
        (Male+Female+Unk), hatchlings, or fledglings. QC default value is NSPLE.

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:

            #Ceate the Summary of SNPL for all events
            queryName_LU = 'qasub_j112_Mismatched_SNPL_Numbers'

            inQuery = (f"SELECT tbl_SNPL_Observations.Event_ID, Sum(IIf(ISNULL([SNPL_Male]), 0, [SNPL_Male]) + "
                       f"IIf(ISNULL([SNPL_Female]), 0, [SNPL_Female]) + IIf(ISNULL([SNPL_Unk]), 0, [SNPL_Unk])) AS "
                       f"Total_Adults, Sum(IIf(ISNULL([SNPL_Hatchlings]), 0, [SNPL_Hatchlings])) AS Total_Hatch, "
                       f"Sum(IIf(ISNULL([SNPL_Fledglings]), 0, [SNPL_Fledglings])) AS Total_Fledge, "
                       f"Sum(IIf(ISNULL([SNPL_Bands]), 0, [SNPL_Bands])) AS Total_Bands "
                       f"FROM tbl_SNPL_Observations GROUP BY tbl_SNPL_Observations.Event_ID;")

            # Check if query exists first - if yes delete
            dm.generalDMClass.queryExistsDelete(queryName=queryName_LU, inDBPath=qcCheckInstance.inDBFE)

            # Push query to Database
            dm.generalDMClass.pushQueryODBC(inQuerySel=inQuery, queryName=queryName_LU, inDBPath=qcCheckInstance.inDBFE)

            #############
            # Final Query
            queryName_LU = 'qa_j112_Mismatched_SNPL_Numbers2'
            # Remove NZ usage in the visual basic based query when reading back into Python via 'pd.read_sql' in method
            # connect_to_AcessDB_DF not able to create the output dataframe

            inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, qsel_QA_Control.Loc_Name,  tbl_Event_Details.QCFlag AS "
                          f"EventDetailsQCFlag, tbl_Event_Details.QCNotes AS EventDetailsQCNotes, "
                          f"qsel_QA_Control.Start_Date, "
                          f" tbl_Event_Details.SNPL_Adults, IIf(IIf(ISNULL([Total_Adults]), 0, [Total_Adults]) = "
                          f"IIf(ISNULL([SNPL_Adults]), 0, [SNPL_Adults]), '', 'Adults ') & IIf(IIf(ISNULL([Total_Hatch])"
                          f", 0, [Total_Hatch]) = IIf(ISNULL([SNPL_Hatchlings]), 0, [SNPL_Hatchlings]), '', "
                          f"'Hatchlings ')"
                          f" & IIf(IIf(ISNULL([Total_Fledge]), 0, [Total_Fledge]) = IIf(ISNULL([SNPL_Fledglings]), 0, "
                          f"[SNPL_Fledglings]), '', 'Fledglings ') AS Error, "
                          f"qasub_j112_Mismatched_SNPL_Numbers.Total_Adults, "
                          f"qasub_j112_Mismatched_SNPL_Numbers.Total_Adults AS Calc_Adults, "
                          f"tbl_Event_Details.SNPL_Hatchlings, "
                          f"qasub_j112_Mismatched_SNPL_Numbers.Total_Hatch AS Calc_Hatch, "
                          f"tbl_Event_Details.SNPL_Fledglings, "
                          f"qasub_j112_Mismatched_SNPL_Numbers.Total_Fledge AS Calc_Fledge, 'frm_Data_Entry' AS "
                          f"varObject, 'tbl_Events' AS RecTable, 'Event_ID' AS RecField, "
                          f"tbl_Events.Event_ID AS RecValue "
                          f"FROM (qsel_QA_Control INNER JOIN qasub_j112_Mismatched_SNPL_Numbers ON "
                          f"qsel_QA_Control.Event_ID = qasub_j112_Mismatched_SNPL_Numbers.Event_ID) INNER JOIN "
                          f"tbl_Event_Details ON qsel_QA_Control.Event_ID = tbl_Event_Details.Event_ID WHERE NOT "
                          f"((IIf(ISNULL([Total_Adults]), 0, [Total_Adults]) = IIf(ISNULL([SNPL_Adults]), 0, "
                          f"[SNPL_Adults])) AND (IIf(ISNULL([Total_Hatch]), 0, [Total_Hatch]) = "
                          f"IIf(ISNULL([SNPL_Hatchlings]), 0, [SNPL_Hatchlings])) AND (IIf(ISNULL([Total_Fledge]), 0, "
                          f"[Total_Fledge]) = IIf(ISNULL([SNPL_Fledglings]), 0, [SNPL_Fledglings]))) "
                          f"ORDER BY qsel_QA_Control.Start_Date;")
            
            # Define the flag fields in the FlagTable table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()

    def qa_j122_Mismatched_Banded_Numbers(queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_j122_Mismatched_Banded_Numbers. Returns records where the number of
        Banded SNPL recorded entered into the SNPL Banded summary results field is not the same as the tally of Banded
        observation records for the survey. QC default value is NSBPLE.

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:

            # Ceate the Summary of SNPL for all events
            queryName_LU = 'qasub_j112_Mismatched_SNPL_Numbers'

            inQuery = (f"SELECT tbl_SNPL_Observations.Event_ID, Sum(IIf(ISNULL([SNPL_Male]), 0, [SNPL_Male]) + "
                       f"IIf(ISNULL([SNPL_Female]), 0, [SNPL_Female]) + IIf(ISNULL([SNPL_Unk]), 0, [SNPL_Unk])) AS "
                       f"Total_Adults, Sum(IIf(ISNULL([SNPL_Hatchlings]), 0, [SNPL_Hatchlings])) AS Total_Hatch, "
                       f"Sum(IIf(ISNULL([SNPL_Fledglings]), 0, [SNPL_Fledglings])) AS Total_Fledge, "
                       f"Sum(IIf(ISNULL([SNPL_Bands]), 0, [SNPL_Bands])) AS Total_Bands "
                       f"FROM tbl_SNPL_Observations GROUP BY tbl_SNPL_Observations.Event_ID;")

            # Check if query exists first - if yes delete
            dm.generalDMClass.queryExistsDelete(queryName=queryName_LU, inDBPath=qcCheckInstance.inDBFE)

            # Push query to Database
            dm.generalDMClass.pushQueryODBC(inQuerySel=inQuery, queryName=queryName_LU,
                                            inDBPath=qcCheckInstance.inDBFE)

            #############
            # Final Query
            queryName_LU = 'qa_j122_Mismatched_Banded_Numbers'
            # Remove NZ usage in the visual basic based query when reading back into Python via 'pd.read_sql' in method
            # connect_to_AcessDB_DF not able to create the output dataframe

            inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, qsel_QA_Control.Loc_Name, tbl_Event_Details.QCFlag AS "
                          f"EventDetailsQCFlag, tbl_Event_Details.QCNotes AS EventDetailsQCNotes, "
                          f"qsel_QA_Control.Start_Date, tbl_Event_Details.SNPL_Banded, "
                          f"qasub_j112_Mismatched_SNPL_Numbers.Total_Bands AS Calc_Banded, 'frm_Data_Entry' AS "
                          f"varObject, 'tbl_Events' AS RecTable, 'Event_ID' AS RecField, tbl_Events.Event_ID AS "
                          f"RecValue FROM (qsel_QA_Control INNER JOIN qasub_j112_Mismatched_SNPL_Numbers ON "
                          f"qsel_QA_Control.Event_ID = qasub_j112_Mismatched_SNPL_Numbers.Event_ID) INNER JOIN "
                          f"tbl_Event_Details ON qsel_QA_Control.Event_ID = tbl_Event_Details.Event_ID WHERE "
                          f"(((qasub_j112_Mismatched_SNPL_Numbers.Total_Bands)>IIf([tbl_Event_Details].[SNPL_Banded] Is "
                          f"Null,0,[tbl_Event_Details].[SNPL_Banded]) Or "
                          f"(qasub_j112_Mismatched_SNPL_Numbers.Total_Bands)<IIf([tbl_Event_Details].[SNPL_Banded] Is "
                          f"Null,0,[tbl_Event_Details].[SNPL_Banded]))) ORDER BY qsel_QA_Control.Start_Date")

            # Define the flag fields in the FlagTable table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()

    def qa_j132_NestID_Year_Mismatch(queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_j132_NestID_Year_Mismatch. Returns records from SNPL_Obsevations where
        the survey year of the nest ID used does not match that in tblNestMaster. QC default value is OEYDNM.

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:

            #############
            # Final Query
            queryName_LU = 'qa_j132_NestID_Year_Mismatch'
            # Remove NZ usage in the visual basic based query when reading back into Python via 'pd.read_sql' in method
            # connect_to_AcessDB_DF not able to create the output dataframe

            inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, tbl_SNPL_Observations.SNPL_Data_ID, "
                          f"tbl_Nest_Master.Nest_ID, tbl_SNPL_Observations.QCFlag AS SNPLObsQCFlag, "
                          f"tbl_SNPL_Observations.QCNotes AS SNPLObsQCNotes, Year([Start_Date]) AS ObsYear, "
                          f"tbl_Nest_Master.Year AS NestYear, 'frm_Data_Entry' AS varObject, 'tbl_Events' AS RecTable, "
                          f"'Event_ID' AS RecField, tbl_Events.Event_ID AS RecValue FROM tbl_Nest_Master INNER "
                          f"JOIN (qsel_QA_Control INNER JOIN tbl_SNPL_Observations ON qsel_QA_Control.Event_ID = "
                          f"tbl_SNPL_Observations.Event_ID) ON tbl_Nest_Master.Nest_ID = tbl_SNPL_Observations.Nest_ID "
                          f"WHERE (((Year([Start_Date]))<>[tbl_Nest_Master].[Year]));")

            # Define the flag fields in the FlagTable table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()

    def qa_j142_Missing_Band_Totals (queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_j142_Missing_Band_Totals. Returns records where there is data in
        tblSNPLBanded for a specific observation time, but the SNPL_Bands field in tblSNPLObservations is blank or 0.
        QC default value is  ONBLE.

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:

            #############
            # Final Query
            queryName_LU = 'qa_j142_Missing_Band_Totals'
            # Remove NZ usage in the visual basic based query when reading back into Python via 'pd.read_sql' in method
            # connect_to_AcessDB_DF not able to create the output dataframe

            inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, tbl_SNPL_Observations.SNPL_Data_ID, "
                           f"qsel_QA_Control.Loc_Name, tbl_SNPL_Observations.QCFlag AS SNPLObsQCFlag, "
                           f"tbl_SNPL_Observations.QCNotes AS SNPLObsQCNotes, qsel_QA_Control.Start_Date, "
                           f"tbl_SNPL_Observations.Nest_ID, tbl_SNPL_Observations.SNPL_Time, "
                           f"tbl_SNPL_Observations.SNPL_Bands, tbl_SNPL_Banded.Left_Leg, "
                           f"tbl_SNPL_Banded.Right_Leg, tbl_SNPL_Banded.Band_Notes, 'frm_Data_Entry' AS varObject, "
                           f"'tbl_Events' AS RecTable, 'Event_ID' AS RecField, tbl_Events.Event_ID AS RecValue "
                           f"FROM (qsel_QA_Control INNER JOIN tbl_SNPL_Observations ON qsel_QA_Control.Event_ID = "
                           f"tbl_SNPL_Observations.Event_ID) INNER JOIN tbl_SNPL_Banded ON "
                           f"tbl_SNPL_Observations.SNPL_Data_ID = tbl_SNPL_Banded.SNPL_Data_ID "
                           f"WHERE (((tbl_SNPL_Observations.SNPL_Bands) Is Null Or "
                           f"(tbl_SNPL_Observations.SNPL_Bands)=0)) ORDER BY qsel_QA_Control.Start_Date;")

            # Define the flag fields in the FlagTable table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()

    def qa_j152_Missing_Band_Data(queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_j152_Missing_Band_Data. Returns records where there are no records in
        tblSNPLBanded, but where the SNPL_Bands field in tblSNPLObservations is greater than 0. QC default value is
        SNBO.

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:

            #############
            # Final Query
            queryName_LU = 'qa_j152_Missing_Band_Data'
            # Remove NZ usage in the visual basic based query when reading back into Python via 'pd.read_sql' in method
            # connect_to_AcessDB_DF not able to create the output dataframe

            inQuerySel = (f"SELECT qsel_QA_Control.Event_ID, tbl_SNPL_Observations.SNPL_Data_ID, "
                          f"qsel_QA_Control.Loc_Name, tbl_SNPL_Observations.QCFlag AS SNPLObsQCFlag, "
                          f"tbl_SNPL_Observations.QCNotes AS SNPLObsQCNotes, qsel_QA_Control.Start_Date, "
                          f"tbl_SNPL_Observations.Nest_ID, tbl_SNPL_Observations.SNPL_Time, "
                          f"tbl_SNPL_Observations.SNPL_Bands AS [Count SNPL Observations], "
                          f"tbl_SNPL_Banded.SNPL_Data_ID AS [SNPL Banded Is Null], tbl_SNPL_Banded.Left_Leg, "
                          f"tbl_SNPL_Banded.Right_Leg, tbl_SNPL_Banded.Band_Notes, 'frm_Data_Entry' AS varObject, "
                          f"'tbl_Events' AS RecTable, 'Event_ID' AS RecField, tbl_Events.Event_ID AS RecValue "
                          f"FROM (qsel_QA_Control INNER JOIN tbl_SNPL_Observations ON qsel_QA_Control.Event_ID = "
                          f"tbl_SNPL_Observations.Event_ID) LEFT JOIN tbl_SNPL_Banded ON "
                          f"tbl_SNPL_Observations.SNPL_Data_ID = tbl_SNPL_Banded.SNPL_Data_ID "
                          f"WHERE (((tbl_SNPL_Observations.SNPL_Bands) Is Not Null And "
                          f"(tbl_SNPL_Observations.SNPL_Bands)>0) AND ((tbl_SNPL_Banded.SNPL_Data_ID) Is Null)) "
                          f"ORDER BY qsel_QA_Control.Start_Date DESC;")

            # Define the flag fields in the FlagTable table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()

    def qa_j162_Mismatched_Band_Obs (queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_j162_Mismatched_Band_Obs. Returns records where there where the
        SNPL_Bands field in tblSNPLObservations does not match the number of records in tbl_SNPLBanded for the survey
        data and time. QC default value is ONBM.

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:

            #Run initial setup Summary and export to a temp table to facilitate QC Flag Update

            queryName_LU = 'qasub_j162_Mismatched_Band_Obs'

            inQuery = (f"SELECT tbl_SNPL_Observations.Event_ID, tbl_SNPL_Observations.SNPL_Data_ID, "
                       f"qsel_QA_Control.Loc_Name, qsel_QA_Control.Start_Date, tbl_SNPL_Observations.SNPL_Time, "
                       f"tbl_SNPL_Observations.Nest_ID, tbl_SNPL_Observations.SNPL_Bands, "
                       f"Count(tbl_SNPL_Banded.SNPL_Band_ID) AS CountSNPBanded, 'frm_Data_Entry' AS varObject "
                       f"FROM (qsel_QA_Control INNER JOIN tbl_SNPL_Observations ON "
                       f"qsel_QA_Control.Event_ID = "
                       f"tbl_SNPL_Observations.Event_ID) INNER JOIN tbl_SNPL_Banded ON "
                       f"tbl_SNPL_Observations.SNPL_Data_ID = tbl_SNPL_Banded.SNPL_Data_ID GROUP BY "
                       f"tbl_SNPL_Observations.Event_ID, tbl_SNPL_Observations.SNPL_Data_ID, qsel_QA_Control.Loc_Name, "
                       f"qsel_QA_Control.Start_Date, tbl_SNPL_Observations.SNPL_Time, tbl_SNPL_Observations.Nest_ID, "
                       f"tbl_SNPL_Observations.SNPL_Bands, qsel_QA_Control.Event_ID HAVING "
                       f"(((Count(tbl_SNPL_Banded.SNPL_Band_ID))<>[SNPL_Bands])) ORDER BY qsel_QA_Control.Loc_Name, "
                       f"qsel_QA_Control.Start_Date, tbl_SNPL_Observations.SNPL_Time;")


            # Check if query exists first - if yes delete
            dm.generalDMClass.queryExistsDelete(queryName=queryName_LU, inDBPath=qcCheckInstance.inDBFE)

            dm.generalDMClass.pushQueryODBC(inQuerySel=inQuery, queryName=queryName_LU, inDBPath=qcCheckInstance.inDBFE)

            #####################
            # Final Query hitting setup query qasub_j162_Mismatched_Band_Obs
            #####################
            queryName_LU = 'qa_j162_Mismatched_Band_Obs'

            inQuerySel = (f"SELECT qasub_j162_Mismatched_Band_Obs.Event_ID, qasub_j162_Mismatched_Band_Obs.SNPL_Data_ID, "
                          f"qasub_j162_Mismatched_Band_Obs.Loc_Name, tbl_SNPL_Observations.QCFlag AS SNPLObsQCFlag, "
                          f"tbl_SNPL_Observations.QCNotes AS SNPLObsQCNotes, qasub_j162_Mismatched_Band_Obs.Start_Date, "
                          f"qasub_j162_Mismatched_Band_Obs.SNPL_Time, qasub_j162_Mismatched_Band_Obs.Nest_ID, "
                          f"qasub_j162_Mismatched_Band_Obs.SNPL_Bands, "
                          f"qasub_j162_Mismatched_Band_Obs.CountSNPBanded, qasub_j162_Mismatched_Band_Obs.varObject, "
                          f"'tbl_Events' AS RecTable, "
                          f"'Event_ID' AS RecField, tbl_SNPL_Observations.Event_ID AS RecValue "
                          f"FROM qasub_j162_Mismatched_Band_Obs INNER JOIN tbl_SNPL_Observations ON "
                          f"qasub_j162_Mismatched_Band_Obs.SNPL_Data_ID "
                          f"= tbl_SNPL_Observations.SNPL_Data_ID;")

            # Define the flag fields in the FlagTable table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()


    def qa_j172_Mismatched_Band_Summary (queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_j172_Mismatched_Band_Summary. Returns records where there the number of
        banded SNPL in the event summary is not the same as the sum of records in tblSNPL_Banded for the survey ESBNA.




        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                                applied.  Additionally defines the flag to be applied
        """

        try:

            #Run initial setup Summary and export to a temp table to facilitate QC Flag Update

            queryName_LU = 'qasub_j172_Mismatched_Band_Summary'

            inQuery = (f"SELECT tbl_SNPL_Observations.Event_ID, tbl_Event_Details.SNPL_Banded, "
                       f"Count(tbl_SNPL_Observations.Event_ID) AS CountSNPLBanded FROM "
                       f"((qsel_QA_Control INNER JOIN tbl_SNPL_Observations ON qsel_QA_Control.Event_ID = "
                       f"tbl_SNPL_Observations.Event_ID) INNER JOIN tbl_SNPL_Banded ON "
                       f"tbl_SNPL_Observations.SNPL_Data_ID = tbl_SNPL_Banded.SNPL_Data_ID) INNER "
                       f"JOIN tbl_Event_Details ON qsel_QA_Control.Event_ID = tbl_Event_Details.Event_ID "
                       f"GROUP BY tbl_SNPL_Observations.Event_ID, tbl_Event_Details.SNPL_Banded;")

            # Check if query exists first - if yes delete
            dm.generalDMClass.queryExistsDelete(queryName=queryName_LU, inDBPath=qcCheckInstance.inDBFE)

            dm.generalDMClass.pushQueryODBC(inQuerySel=inQuery, queryName=queryName_LU, inDBPath=qcCheckInstance.inDBFE)

            #####################
            # Final Query hitting setup query qasub_j172_Mismatched_Band_Summary
            #####################
            queryName_LU = 'qa_j172_Mismatched_Band_Summary'

            inQuerySel = (f"SELECT qasub_j172_Mismatched_Band_Summary.Event_ID, qsel_QA_Control.Loc_Name, "
                          f"tbl_Event_Details.QCFlag AS EventDetailsQCFlag, tbl_Event_Details.QCNotes AS "
                          f"EventDetailsQCNotes, qsel_QA_Control.Start_Date, "
                          f"qasub_j172_Mismatched_Band_Summary.SNPL_Banded AS EventDetailsSNPL_Banded, "
                          f"qasub_j172_Mismatched_Band_Summary.CountSNPLBanded, 'frm_Data_Entry' AS varObject, "
                          f"'tbl_Events' AS RecTable, 'Event_ID' AS RecField, tbl_Event_Details.Event_ID AS RecValue "
                          f"FROM (qasub_j172_Mismatched_Band_Summary INNER JOIN qsel_QA_Control ON "
                          f"qasub_j172_Mismatched_Band_Summary.Event_ID = qsel_QA_Control.Event_ID) INNER JOIN "
                          f"tbl_Event_Details ON qasub_j172_Mismatched_Band_Summary.Event_ID = "
                          f"tbl_Event_Details.Event_ID WHERE "
                          f"(((qasub_j172_Mismatched_Band_Summary.SNPL_Banded)<>[CountSNPLBanded]));")

            # Define the flag fields in the FlagTable table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()



    def qa_j182_Predator_ActivityType (queryDecrip_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Query routine for validation check - qa_j182_Predator_ActivityType. Returns records where the predator survey
        activity type entered is not one of  the expected values of A, F, S, and W. QC default value is PAVU. Query
        should be redundant because an Predator Activity Lookup table has been created and assigned to the predator
        activity field in the database with referential integrity inplace.

        :param queryDecrip_LU: Query description pulled from the 'tbl_QCQueries' table
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return: inQuerySel: Final query to be pushed back to Access DB
                flagFieldsDic: Dictionary defining the Flag fields in 'tbl_Event_Details' to which flags will be
                applied.  Additionally defines the flag to be applied

        Updates:
        8/27/2024 - add logic to dynamically add all values in the tlu_Predator_Actions - Predator_Action_ID field
        to the filter in the SQL statement.

        """

        try:


            #####################
            # Final Query
            #####################
            queryName_LU = 'qa_j182_Predator_ActivityType'

            # Import the tlu_Predator_Actions table
            inQuery = f"SELECT * FROM tlu_Predator_Actions"
            # Read in the tlu_Predator_Actions table
            actionsDF = dm.generalDMClass.connect_to_AcessDB_DF(inQuery, qcCheckInstance.inDBBE)

            # Get list of value in the 'Predator_Action_ID' field
            realized_values = actionsDF['Predator_Action_ID'].dropna().unique().tolist()

            # Iterate through the realized values and create the filter in the where clause to exclude.
            filterString = f'Not in ('
            recCount = 1
            for value in realized_values:
                if recCount == 1:
                    filterString = f"{filterString}'{value}'"
                else:
                    filterString = f"{filterString},'{value}'"
                recCount += 1
            # Add closing parenthesis
            filterString = f'{filterString})'

            inQuerySel = (f"SELECT tbl_Predator_Survey."
                          f"Predator_Data_ID, tbl_Events.Event_ID, tbl_Locations.Loc_Name, "
                          f"tbl_Events.Start_Date, "
                          f"tbl_Predator_Survey.QCFlag AS PredQCFlag, tbl_Predator_Survey.QCNotes AS PredQCNotes, "
                          f"tlu_Predator_Type.Description AS Predator, tbl_Predator_Survey.GroupSize, "
                          f"tbl_Predator_Survey.BinNumber, tbl_Predator_Survey.ACT, tbl_Predator_Survey.Waypoint, "
                          f"'frm_Data_Entry' AS varObject, 'tbl_Events' AS RecTable, 'Event_ID' AS RecField, "
                          f"tbl_Events.Event_ID AS RecValue FROM tlu_Predator_Type INNER JOIN (tbl_Locations INNER JOIN "
                          f"(tbl_Events INNER JOIN tbl_Predator_Survey ON tbl_Events.Event_ID = "
                          f"tbl_Predator_Survey.Event_ID) ON tbl_Locations.Location_ID = tbl_Events.Location_ID) ON "
                          f"tlu_Predator_Type.Predator_Type_ID = tbl_Predator_Survey.Predator_Type_ID WHERE "
                          f" tbl_Predator_Survey.ACT {filterString} ORDER BY "
                          f"tbl_Events.Start_Date DESC;")

            # Define the flag fields in the FlagTable table, these are the fields to which the flag 'DFO' will be
            # applied

            flagFieldsDic = {'ApplyFlag': ['Yes']}

            return inQuerySel, flagFieldsDic

        except Exception as e:

            logMsg = (f'ERROR - An error occurred in QC_Checks_SNPLPORE - for query {queryName_LU}: {e}')
            dm.generalDMClass.messageLogFile(dmInstance, logMsg=logMsg)
            logging.error(logMsg, exc_info=True)
            traceback.print_exc(file=sys.stdout)
            exit()


    if __name__ == "__name__":
        logger.info("Running QC_Checks_SNPLPORE.py")
