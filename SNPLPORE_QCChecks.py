"""
QCChecksRoot.py
Root Quality Control Checks Script - setup script.

Output:

Python Environment: SNPL_QC - Python 3.11,
pywin32

Date Developed - July 2024
Created By - Kirk Sherrill - Data Scientist/Manager San Francisco Bay Area Network Inventory and Monitoring
"""

# Import Libraries
import pandas as pd
import pyodbc
import sys
import os
import session_info
import traceback
from datetime import datetime
import QC_Checks as qc
import generalDM as dm

# Protocol Being Processes
protocol = 'SNPLPORE'   #(SNPLPORE|Salmonids|...)
# Access Backend Database for the protocol
inDBBE = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\SnowyPlovers_PORE\SNPLOVER\SNPL_IM\Data\Database\Dbase_BE\PORE_SNPL_BE_20240718 - Copy.accdb'
# Access FrontEnd Database for the protocol
inDBFE = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\SnowyPlovers_PORE\SNPLOVER\SNPL_IM\Data\Database\PORE_SNPL_FrontEnd_20240718v2.accdb'
# Year Being Processed
inYear = 2023
# NPS User Name of person running the QC script.  This will be populated in the 'QA_USer' field of the 'tbl_QA_Results
inUser = 'ksherrill'

#dateNow = datetime.now().strftime('%Y%m%d')
# Output Name, OutDir, Workspace and Logfile Name
outName = f'{protocol}_{inYear}'  # Output name for excel file and logile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\SnowyPlovers_PORE\SNPLOVER\SNPL_IM\Data\Deliverable\2024'  # Directory Output Location

def main():
    try:
        session_info.show()
        # Set option in pandas to not allow chaining (views) of dataframes, instead force copy to be performed.
        pd.options.mode.copy_on_write = True

        ###############
        # Define the qcCheckInstance and dmInstance instances
        ################

        # Create the qcChecks instance
        qcCheckInstance = qc.qcChecks(protocol=protocol, inDBBE=inDBBE, inDBFE=inDBFE, yearLU= inYear, inUser = inUser)

        # Print out the name space of the instance
        print(qcCheckInstance.__dict__)

        # Logfile will be saved in the workspace directory which is child of the fileDir
        logFile = dm.generalDMClass.createLogFile(logFilePrefix=outName, workspaceParent=outDir)

        # Create the data management instance to  be used to define the logfile path and other general DM attributes
        dmInstance = dm.generalDMClass(logFile)

        ###############
        # Proceed to the Workflow to process the defined data validation routines
        ################

        # Go to QC Processing Routines
        qc.qcChecks.process_QCRequest(qcCheckInstance=qcCheckInstance, dmInstance=dmInstance)

        # Message Script Completed
        logMsg = f'Successfully Finished All QC Check Script for - {protocol}'
        dm.generalDMClass.messageLogFile(self=dmInstance, logMsg=logMsg)

    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - QCChecks.py - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()
        traceback.print_exc(file=sys.stdout)


    finally:
        exit()

def timeFun():
    try:
        b = datetime.now()
        messageTime = b.isoformat()
        return messageTime
    except:
        print(f'Failed - timeFun')
        exit()


if __name__ == '__main__':

    #################################
    # Checking for Out Directories and Log File
    ##################################
    if os.path.exists(outDir):
        pass
    else:
        os.makedirs(outDir)


    # Run Main Code Bloc
    main()
