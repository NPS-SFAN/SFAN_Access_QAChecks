"""
SNPLPORE_QCChecks.py
Script performs the Snowy Plover Quality Control Data Validation checks.

Output:



Python Environment: SNPL_QC - Python 3.11

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

# SNPL PORE Backend Database
inDBBE = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\SnowyPlovers_PORE\SNPLOVER\SNPL_IM\Data\Database\Dbase_BE'
# SNPL PORE FrontEnd Database
inDBBE = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\SnowyPlovers_PORE\SNPLOVER\SNPL_IM\Data\Database\'

#Year Being Processed
inYear = 2024

dateNow = datetime.now().strftime('%Y%m%d')
# Output Name, OutDir, Workspace and Logfile Name
outName = f'SNPLPORE_QC_'str{inYear}f'_{dateNow}'  # Output name for excel file and logile
outDir = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\SnowyPlovers_PORE\SNPLOVER\SNPL_IM\Data\Deliverable\2024'  # Directory Output Location
workspace = f'{outDir}\\workspace'  # Workspace Output Directory
logFileName = f'{workspace}\\{outName}_{dateNow}.LogFile.txt'  # Name of the .txt script logfile which is saved in the workspace directory

def main():
    try:
        session_info.show()
        # Set option in pandas to not allow chaining (views) of dataframes, instead force copy to be performed.
        pd.options.mode.copy_on_write = True

        # Create the data management instance to  be used to define the logfile path and other general DM attributes
        dmInstance = dm.generalDMClass(logFile)

        ################
        # Define the Quality Control Procedures to be processed.
        ################

        inQuery = "Select * FROM tbl_QCQueries"
        outDFQueries = dm.connect_to_AcessDB_DF(inQuery, inDBBE)

        # Iterate through the QC Queries defined in 'tbl_QCQueries'
        for index, row in outDFQueries.iterrows():
            queryName_LU = row.get('QueryName')


            #Create the qcChecks instance
            qcCheckInst = qc.qcChecks(queryName_LU)

            # Print out the name space of the instance
            print(qcCheckInst.__dict__)





            messageTime = timeFun()
            scriptMsg = 'Successfully Completed QC Query: ' + queryName_LU + ' - '+ messageTime

            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()


    except:
        messageTime = timeFun()
        scriptMsg = "Exiting Error - SNPLPORE_QCChecks.py - " + messageTime
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

    if os.path.exists(workspace):
        pass
    else:
        os.makedirs(workspace)

    # Check if logFile exists
    if os.path.exists(logFileName):
        pass
    else:
        logFile = open(logFileName, "w")  # Creating index file if it doesn't exist
        logFile.close()

    # Run Main Code Bloc
    main()
