"""
generalDM.py
General Data Management workflow related methods.  Consider migrating this to a more general SFAN_Data Management module.
"""
import os.path
import sys
from datetime import datetime
import pyodbc
import pandas as pd
import win32com.client

class generalDMClass:

    dateNow = datetime.now().strftime('%Y%m%d')

    def __init__(self, logFile):
        """
        Define the instantiated general Data Management instantiation attributes

        :param logFile: File path and name of .txt logFile
        :return: zzzz
        """

        self.logFileName = logFile

    def createLogFile(logFilePrefix, workspaceParent):
        """
        Creates a .txt logfile for the defined Name and in the defined Workspace directory. Checks if workspace exists
        will create if not.

        :param logFilePrefix:  LogFile Prefix Name
        :param workspaceParent: Parent directory for the workspace, will add a child 'workspace' in which the logfile
         will be created
        :return: logFile: Full Path Name of the created logfile
        """

        #################################
        # Checking for working directories
        ##################################
        workspace = workspaceParent + "\\workspace"
        if os.path.exists(workspace):
            pass
        else:
            os.makedirs(workspace)

        logFile = f'{workspace}\\{logFilePrefix}_logFile_{generalDMClass.dateNow}.txt'  # Name of the .txt script logfile which is saved in the workspace directory

        # Check if logFile exists
        if os.path.exists(logFile):
            pass
        else:
            logFile = open(logFile, "w")  # Creating index file if it doesn't exist
            logFile.close()

        return logFile


    def messageLogFile(self, logMsg):

        """
        Write Message to Logfile - routine add a date/time Now string time stamp

        :param self:  generalDM Instance
        :param logMsg: String with the logfile message to be appended to the Self.logFileName
        :return:
        """


        #logFileName_LU = self.logFileName.name
        logFileName_LU = self.logFileName

        #Get Current Time
        messageTime = generalDMClass.timeFun()
        logMsg = f'{logMsg} - {messageTime}'
        print(logMsg)

        logFile = open(logFileName_LU, "a")
        logFile.write(logMsg + "\n")
        logFile.close()

    def timeFun():

        """
        Returns a date time now isoformat string using in error messages

        :return: messageTime: String with ISO format date time
        """

        from datetime import datetime
        b = datetime.now()
        messageTime = b.isoformat()

        return messageTime

    def connect_DB_Access(self):
        """
        Create connection to Access Database Via PYODBC connection.

        :param instance: Will pull the full path name to the database from the passed instance

        :return: cnxn: ODBC connection to access database
        """

        inDB = self.inDBBE
        connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + inDB + ";")
        cnxn = pyodbc.connect(connStr)
        return cnxn

    def getLookUpValueAccess(self, cnxn, lookupTable, lookupField, lookupValue, lookupFieldValueFrom):
        """
        Find value in a lookup table using the passed variables, using a distinct clause expecting this to be used for
        a lookup table situation.

        :param instance: pass instance with relevant information
        :param cnxn: ODBC connection to access database
        :param lookupTable: lookup table in the cnxn database
        :param lookupField: field in the lookupTable to lookup
        :param lookupValueOut: values in the lookupField to be looked up
        :param lookupFieldValueFrom: field in the lookupTable from which to pull the lookup value

        :return: lookupValueOut: String with the lookup value
        """
        query = f"Select Distinct {lookupFieldValueFrom} from {lookupTable} Where {lookupField} = '{lookupValue}';"

        #Should be single value so will be series
        lookupValueDF = pd.read_sql(query, cnxn)

        #No value returned in lookup table - exit script
        if lookupValueDF.shape[0] == 0:
            logMsg = f'WARNING - No value returned in lookup table for - {lookupValue} - EXITING script at - getLookUpValueAccess'
            generalDMClass.messageLogFile(self=self, logMsg=logMsg)
            sys.exit()

        #Convert lookup to series
        lookupValueSeries = lookupValueDF.iloc[0]

        #Convert the series values to a string
        lookupValueSeriesStr = lookupValueSeries.astype(str)

        lookupValueOut = lookupValueSeriesStr.str.cat(sep=', ')

        return lookupValueOut

    def connect_to_AcessDB_DF(query, inDB):
    # Connect to Access DB and perform defined query - return query in a dataframe
        """
        Connect to Access DB and perform defined query - return query in a dataframe

        :param query: query to be processed
        :param inDB: path to the access database being hit

        :return: queryDf: query output dataframe
        """

        try:
            connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + inDB + ";")
            cnxn = pyodbc.connect(connStr)
            queryDf = pd.read_sql(query, cnxn)
            cnxn.close()

            return queryDf
        except:
            print(f'Failed - connect_to_AccessDB_DF')
            exit()


    def QueryExistsDelete(queryName, inDBPath):
        """
        Check if query exists in the database if yes delete, using pywin32 to hit the Access COM interface, ODBC
        doesn't have permissions to hit the 'MSYS' variables

        :param queryName: Name of query being pushed, will deleted first if exists
        :param inDBPath: path to database

        :return:
        """
        # Initialize the Access application
        access_app = win32com.client.Dispatch('Access.Application')

        # Open the Access database
        access_app.OpenCurrentDatabase(inDBPath)

        # Get the current database object
        db = access_app.CurrentDb()

        try:
            # Check if the query exists before attempting to delete it
            query_exists = False
            for query in db.QueryDefs:
                if query.Name == queryName:
                    query_exists = True
                    break

            if query_exists:
                # Delete the query
                db.QueryDefs.Delete(queryName)
                print(f"Query '{queryName}' has been deleted from the database.")
            else:
                print(f"Query '{queryName}' does not exist in the database.")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            # Close the database and quit Access
            access_app.CloseCurrentDatabase()
            access_app.Quit()

        # Clean up COM objects
        del access_app

    def PushQuery(inQuerySel, queryName, inDBPath):
        """
        Push the pass query in 'inQuerySel' to the output query 'queryName'

        :param inQuerySel: SQL Query defining the query to be pushed back to the backend instance
        :param queryName: Name of query being pushed, will deleted first if exists
        :param inDBPath: path to database

        :return:
        """
        # Initialize the Access application
        access_app = win32com.client.Dispatch('Access.Application')

        # Open the Access database
        access_app.OpenCurrentDatabase(inDBPath)

        # Get the current database object
        db = access_app.CurrentDb()
