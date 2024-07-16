"""
QC_Checks_SNPLPORE.py
QC_Checks Methods/Functions to be used for Snowy Plover PORE Quality Control Validation

"""
#Import Required Libraries
import pandas as pd
import glob, os, sys

import QC_Checks
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

    def processQuery(queryName_LU, yearlyRecDF, qcCheckInstance, dmInstance):
        """
        Iterate through the defined queries

        :param queryName_LU: Name of query routine being processes this is query name in the 'Query_Name' field in
         table 'tbl_QCQueries'
        :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name


        :return: Pushed
        """

        if queryName_LU == "qa_a102_Unverified_Events":

            outMethod = qcProtcol_SNPLPORE.qa_a102_Unverified_Events(queryName_LU, yearlyRecDF, qcCheckInstance, dmInstance)




        def qa_a102_Unverified_Events(queryName_LU, yearlyRecDF, qcCheckInstance, dmInstance):
            """
            Query routine for validation check - qa_a102_Unverified_Events

            :param queryName_LU: Name of query routine being processes this is query name in the 'Query_Name' field in
             table 'tbl_QCQueries'
            :param yearlyRecDF:  Dataframe with the subset of yearly records by Event to be processed
            :param qcCheckInstance: QC Check Instance
            :param dmInstance: data management instance which will have the logfile name


            :return: Pushed
            """


