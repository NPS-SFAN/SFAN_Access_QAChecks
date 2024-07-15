"""
QC_Checks.py
QC_Checks Methods/Functions to be used for general Quality Control Validation workflow.
"""
#Import Required Dependices
import pandas as pd
import glob

import os
import sys

class qcChecks:

    #Class Variables
    numqcChecksInstances = 0

    def __init__(self, queryName):
        """
        Define the instantiated loggerFile attributes

        :param queryName: Name of QC routine being processes  (e.g. 'qa_a102_Unverified_Events'|'qa_f112_Incomplete_Weather').


        :return: instantiated self object
        """

        self.queryName = queryName

        #Update the Class Variable
        qcChecks.numqcChecksInstances += 1

    def process_QCRequest(qcCheckInstance, dmInstance):

        """
        General Quality Control workflow processing workflow steps.
        Extracting needed information from the instantiated QC Check class
        Output Logfile will be in the self.fileDir\workspace directory

        :param qcCheckInstance: QC Check Instance
        :param dmInstance: data management instance which will have the logfile name

        :return:
        """

