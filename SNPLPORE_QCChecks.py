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

# SNPL PORE Database
inDB = r'C:\Users\KSherrill\OneDrive - DOI\SFAN\VitalSigns\PlantCommunities\Data\Database\SFAN_PlantCommunities_BE_20240306.accdb'

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

        ################
        # Define the Quality Control Procedures to be processed.
        ################

        inQuery = "Select * FROM tbl_QCQueries"
        outFun = connect_to_AcessDB(inQuery, inDB)
        if outFun[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function connect_to_AcessDB - NAWMADataset" + messageTime + " - Failed - Exiting Script")
            exit()

        messageTime = timeFun()
        print(f'Success: connect_to_AcessDB - tbl_QCQueries - {messageTime}')
        outDFQueries = outFun[1]

        # Iterate through the QC Queries defined in 'tbl_QCQueries'
        for index, row in outDFQueries.iterrows():
            qcQuery_LU = row.get('QueryName')

            ####################
            #
            ####################



            messageTime = timeFun()
            scriptMsg = 'Successfully Completed QC Query: ' + qcQuery_LU + ' - '+ messageTime

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



# Connect to Access DB and perform defined query - return query in a dataframe
def connect_to_AcessDB(query, inDB):
    try:
        connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + inDB + ";")
        cnxn = pyodbc.connect(connStr)
        queryDf = pd.read_sql(query, cnxn)
        cnxn.close()

        return "success function", queryDf
    except:
        print(f'Failed - connect_to_AccessDB')
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
