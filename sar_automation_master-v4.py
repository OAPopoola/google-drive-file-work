###############################################################################################################################
"""#This code is worker code for the SAR process
#Authors: Olukunmi Popoola and David Edwards
#Date: 15/01/2019
#Background:
#The current DSAR system is cumbersome and requires the end user accessing the 
#back end of systems that are best left to API programmers to access
#There is a lot of folder work involved in servicing one DSAR request and this 
#code does all the folder work 
#
#This code does the following tasks:
##############################################################################################################################
#Checking....
#Check to see that the required parent folders are in place
#check to see that the required template spreadsheet files are in place
#check to see that the required input spreadsheet files are in place
##############################################################################################################################
#Folder work
#Create a new DSAR subfolder (based on a new reference number) in the parent DSAR folder if request is a DSAR request
#Create a new Delete subfolder (based on a new reference number) in the parent Delete folder if request is a Delete request
#create a copy of the relevant template file for the new request (SAR, Delete or both)
#Rename the copy template file(s) to reflect the new reference Id
#copy the renames template file to the newly created subfolder
##############################################################################################################################
#Spreadsheet work
#Modify the headers of the template files to reflect the details of the new request i.e Reference number, name, email address, 
#	date due, identity confirmed?
#insert the required processing details into the input spreadsheet files awaiting retrieval by cron jobs
###############################################################################################################################"""

## import all the necessary libraries
import os,requests,multiprocessing,configparser,csv,boto3
import gspread,json,getpass,sys
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.http import MediaFileUpload
from datetime import datetime
from datetime import timedelta

################################################################################################################################
########################Function definitions#####################################

# Function to connect to the Google Service API
def get_google_service(api_name, api_version, scopes, key_file_location):
    """Get a service that communicates to a Google API.
        Args:
            api_name: The name of the api to connect to.
            api_version: The api version to connect to.
            scopes: A list auth scopes to authorize for the application.
            key_file_location: The path to a valid service account JSON key file.
        Returns:
            A service that is connected to the specified Google API.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        key_file_location, scopes=scopes)
    # Build the service object.
    service = build(api_name, api_version, credentials=credentials)
    return service

#GSpread Authentication setup
def authorizeGSpread(pathToGSpreadConfigFile,scopes):
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        pathToGSpreadConfigFile,SCOPES)
    gs = gspread.authorize(creds)
    return gs

def createGDriveSubFolder(gService,parentFolderId,subFolderName):
    #this code create a subfolder on google drive and takes
    #google service handle,
    #parentFolderId
    #subFolderName
    #as input
    #it returns the new subfolder handle where we can get the subfolder id
    fmeta = {
	'name' : subFolderName,
	'mimeType' : 'application/vnd.google-apps.folder',
	'parents' : [parentFolderId]
	}
    newDir = gService.files().create(body=fmeta,
                                     fields='id').execute()
    
    return newDir

def copyAndRenameGDriveFile(gService,fileToCopyId,NewFileName):
    '''This folder makes a copy of the specified Google drive file
    and renames it in one go. The new copy will still reside in the
    same location as the file that was copied
    it takes
    google service handle
    id of the file to copy
    new file name of the copied file
    as input'''
    
    
    copied_file = {'name' : NewFileName}
    newFile = gService.files().copy(
                         fileId=fileToCopyId,
                         body=copied_file).execute()
    
    #return just the file Id
    fileId = newFile.get('id')
    return fileId


def getTimeString():
    return datetime.strftime(datetime.now(),"%d/%m/%y %H:%M:%S")



def moveGDriveFile(gService,sourceFolderId,destinationFolderId,fileId):
    '''#this code moves a file from one google drive folder to another
    specified by folder ids
    it takes
    google service handle
    Id of the current location
    id of the destination folder
    id of the file to be moved'''
    
    pfile = gService.files().get(fileId=fileId,
                                 fields='parents').execute()
    previous_parents = ",".join(pfile.get('parents'))

    file = gService.files().update(fileId=fileId,
                                    addParents=destinationFolderId,
                                    removeParents=previous_parents,
                                    fields='id, parents').execute()

   
def doFolderWork(service,subjectName,subjectEmail,subjectSarReference,sarAction):
    '''this function encapsulates all the folder work described above'''

     
    if (sarAction == 'Access'):
        sarFolderId = config['Folder Ids']['sarsParentFolderId']        
        sarTemplateId = config['Template Ids']['sarSpreadheetTemplateId']
        
        #constitute the new Folder name 
        newFolderName = 'S' + str(subjectSarReference) + ' - Open'
        #make the new template filename
        newTemplateFileName = 'DSR Reference Number: ' + 'S' + str(subjectSarReference)
        
    if (sarAction == 'Delete'):
        sarFolderId = config['Folder Ids']['deleteParentFolderId']  
        sarTemplateId = config['Template Ids']['deleteSpreadsheetTemplateId']
        
        #constitute the new Folder name 
        newFolderName = 'D' + str(subjectSarReference) + ' - Open'
        #make the new template filename
        newTemplateFileName = 'DSR Reference Number: ' + 'D' + str(subjectSarReference)
    
    #create the sub folder and keep the folder handle
    newFolderHandle = createGDriveSubFolder(service,sarFolderId,newFolderName)

    #get the new subfolder id
    newFolderId = newFolderHandle.get('id')
    
    #create and rename the new template file
    templateCopyId = copyAndRenameGDriveFile(service,sarTemplateId,newTemplateFileName)
    
    #get the new file id
    #newtemplateFileId = templateCopyHandle.get('id')

    #move the new template file to the correct subfolder
    moveGDriveFile(service,sarFolderId,newFolderId,templateCopyId)
    
    return templateCopyId

def connectToWorkbookSheet(gsAuth,workbookId,sheetIndex):
    '''connect to spreadsheet by spreadsheet ID
    this function takes an authenthicated gspread handle,
    the sheet to connect to, and the index of the particular worksheet'''
    wB = gsAuth.open_by_key(workbookId)
    #connect to the particular worksheet by index
    activeWorkSheet = wB.get_worksheet(sheetIndex)
    return activeWorkSheet


def setTemplateFileHeader(gsAuth,templateSheetId,userFullname,userEmail,userTemplateRef,sarReceiveDate,sarDueDate,userIdConfirmed):
    '''this function puts user information in the header of the user's template
    it is hardcoded and if the template file is changed this code must be updated
    otherwise it will unintentionally overwrite other data in the spreadsheet

    we need to connect to the spreadsheet
    get the handle of the first sheet in the spreadsheet (it only has one)
    then start a series of update calls'''

    aS = connectToWorkbookSheet(gsAuth,templateSheetId,0)
    
    #update template reference value 
    aS.update_acell(
        config['Header Info Locations']['referenceValueCell'],userTemplateRef)
    
    #update user name in template sheet
    aS.update_acell(
        config['Header Info Locations']['nameValueCell'], userFullname)
    
    #update user email address
    aS.update_acell(
        config['Header Info Locations']['emailValueCell'], userEmail)
        
    #update Date Received Cell
    aS.update_acell(
        config['Header Info Locations']['datereceivedValueCell'], sarReceiveDate)
    
    #update Due Date Cell
    aS.update_acell(
        config['Header Info Locations']['datedueValueCell'], sarDueDate)
    
    #update whether user id has been confirmed (usually 'No')
    aS.update_acell(
        config['Header Info Locations']['identityconfirmedValueCell'], userIdConfirmed)


def getNextFillRow(sheetHandle):
    '''this function takes a worksheet handle and returns the next empty row
    all it does is count the rows filled out for column 1. Google forms will fill
    the spreasheet sequentially with no gaps. The first column in the spreadsheet is the
    system generated timestamp so no chance of the column being blank which could
    cause this function to be unpredictable'''
    return (len(sheetHandle.col_values(1)) + 1)


def writeToLog(authSheetService,logSheetId,whatToWrite):
    '''This function writes info to the bespoke log file which is a google sheet. This was done
    because there will not be access to the standard logs on AWS
    It takes an authenticate qspread connector, the id of the log file and the what to write as input'''
    #connect to the log file
    logSheet = connectToWorkbookSheet(authSheetService,logSheetId,0)
    #get the next row number to write to
    nextRowNumber = getNextFillRow(logSheet)
    #write to the first column - hence the '1'
    logSheet.update_cell(str(nextRowNumber),'1',whatToWrite)


def fillInputSheet(authService,SubjectDataList,inputSheetId,inputSheetName):
    #this function fills out the input sheet for the cron code to process
    
    #connect to the input spreadsheet
    hSheet = connectToWorkbookSheet(authService,inputSheetId,0)
    
    #start a loop through all the data
    for item in SubjectDataList:
        #get the next row to fill in the spreadsheet
        nextRowNumber = getNextFillRow(hSheet)

        #constitute the ref number
        if (item['Action Required:'] == 'Access To Information'):
            strRef = 'S' + str(item['If DSAR Please Enter Next S-Number:'])
            
        if (item['Action Required:'] == 'Deletion (Deletion Of Information)'):
            strRef = 'D' + str(item['If Deletion Please Enter Next D-Number:'])
            
        #For deletions and access only fill the sheet as if we want access
        if (item['Action Required:'] == 'Both (Access and Deletion)'):
            strRef = 'S' + str(item['If DSAR Please Enter Next S-Number:'])
        
        #test for Formstack
        if inputSheetName == 'FormStack':
            hSheet.update_cell(str(nextRowNumber),'1', strRef)
            hSheet.update_cell(str(nextRowNumber),'2',item['Enter DSR Email Address:'])
            
        #test for Temppen Sheet
        #these 3 tables need the same info in the same cells
        if inputSheetName in ('TempPen','Zuora','EventBrite'):
            hSheet.update_cell(str(nextRowNumber),'1',item['Enter DSR Email Address:'])
            hSheet.update_cell(str(nextRowNumber),'2', strRef)
            
        #test for BiqQuery and Datalake
        if inputSheetName in ('BigQuery','DataLake', 'OneOff') and len(str(item['Enter Identity ID:'])) > 0:
            hSheet.update_cell(str(nextRowNumber),'1',strRef)
            hSheet.update_cell(str(nextRowNumber),'2', item['Enter Identity ID:'])
            
        #test for Braze. Braze input sheet takes 3 inputs
        if inputSheetName == 'braze':
            #write the ref. There will always be a ref in the input sheet
            hSheet.update_cell(str(nextRowNumber),'1',strRef)
            #check if there is an identity id - there may not be one
            if len(str(item['Enter Identity ID:'])) > 0:
                hSheet.update_cell(str(nextRowNumber),'2', item['Enter Identity ID:'])
            # there will always be an email address - the subject emailed us the start the process
            hSheet.update_cell(str(nextRowNumber),'3',item['Enter DSR Email Address:'])
                
        

        
def fillAllInputSheets(gc,recordsList):
    #fill input files - collect ids from config file
    
    temppenSheetId = config['Input File Ids']['temppenInputSheetId']
    formstackSheetId = config['Input File Ids']['formstackInputSheetId']
    zuoraInputSheetId = config['Input File Ids']['zuoraInputSheetId']
    eventbriteInputSheetId = config['Input File Ids']['eventbriteInputSheetId']
    bigQueryInputSheetId = config['Input File Ids']['bigqueryInputSheetId']
    datalakeInputSheetId = config['Input File Ids']['datalakeInputSheetId']
    brazeInputSheetId = config['Input File Ids']['brazeSheetId']
    

    fillInputSheet(gc,recordsList,temppenSheetId,'TempPen')
    fillInputSheet(gc,recordsList,formstackSheetId,'FormStack')
    fillInputSheet(gc,recordsList,zuoraInputSheetId,'Zuora')
    fillInputSheet(gc,recordsList,eventbriteInputSheetId,'EventBrite')
    fillInputSheet(gc,recordsList,bigQueryInputSheetId,'BigQuery')
    fillInputSheet(gc,recordsList,datalakeInputSheetId,'DataLake')
    fillInputSheet(gc,recordsList,oneoffInputSheetId,'OneOff')
    fillInputSheet(gc,recordsList,brazeInputSheetId,'braze')

def checkDataConsistency(recList):
    if recList['Action Required:'] == 'Both (Access and Deletion)':
        if len(str(recList['If Deletion Please Enter Next D-Number:'])) > 0 and len(str(recList['If DSAR Please Enter Next S-Number:'])) > 0:
            return True
    
    if recList['Action Required:'] == 'Access To Information':
        if  len(str(recList['If DSAR Please Enter Next S-Number:'])) > 0:
            return True
                   
    if recList['Action Required:'] == 'Deletion (Deletion Of Information)':
        if len(str(recList['If Deletion Please Enter Next D-Number:'])) > 0:
            return True
    return False                        

def updateSpreadsheetRecord(sheetConn,totalRecords,numRecsToProcess,recIndex):
    #this function updated the procesed column in the spreadsheet
    #it is column 3 for gSpread
    #we want to use the passed numbers to find the row number to write to
    
    rowNo = (totalRecords - numRecsToProcess + recIndex) + 1
    sheetConn.update_cell(rowNo,3,'Processed '+ getTimeString())

    
    
           

######################Initialisation Routines################################################################################

# Google Services of interest that we're interested in
SCOPES = ['https://www.googleapis.com/auth/drive',                                                       
          'https://www.googleapis.com/auth/drive.file',                                                  
          'https://www.googleapis.com/auth/spreadsheets']

paramsPath = os.path.join(os.path.expanduser('~'),'ConfigFiles','params_olu.cfg')

#set up access to the config file
config = configparser.RawConfigParser()

#read the config
config.read(paramsPath)

gSpreadkey = config['GSpread Details']['gspread_key_file']
gSpreadKeyPath = os.path.join(os.path.expanduser('~'),'ConfigFiles',gSpreadkey)

googlekey = config['Google Drive']['google_key_file']
googleKeyPath = os.path.join(os.path.expanduser('~'),'ConfigFiles',googlekey)

#authenthicate for Google File Service
gService = get_google_service(
        api_name='drive',
        api_version='v3',
        scopes=SCOPES,
        key_file_location=googleKeyPath)

#Authenthicate for GSpread Use
gc = authorizeGSpread(gSpreadKeyPath,SCOPES)

###########################Calling Defs start here##########################################################################

#see that we can write to the log file
logFileId = config['LogFiles ID']['sarAutomationMaster']
writeToLog(gc,logFileId, getTimeString() + '  Process Started')

#Get the spreadsheet ID from the config file
#this workbook contains
#the actual data on sheet 0
dsarInputFormSheetId = config['DSAR Form Sheet']['dsarInputSheetId']

#Connect to the spreadsheet and specific worksheets
fs1 = connectToWorkbookSheet(gc,dsarInputFormSheetId,0)

#old_records is a record of where the last SAR processing stopped
#new_records is the current number of records in the spreadsheet

#numRecInSheet = int(fs2.acell('a1').value)
numRecInSheet = len(fs1.col_values(1)) - 1 #-1 to allow for the header row
if numRecInSheet == 0:
    #diff can be 0 if the systen is up to date
    writeToLog(gc,logFileId,'No New SARs...Up to date')
    writeToLog(gc,logFileId,'Process Finished at ' + getTimeString())
    writeToLog(gc,logFileId,'***************')
    sys.exit()

#get new records as a list of a dictionary
#recordsList = fs1.get_all_values()
recordsList = fs1.get_all_records()
totalRecordsInSheet = len(recordsList)
#we now have a dictionary or the rows in a list
#next we extract the rows to process from all rows in the spreadsheet
newList = []
for rec in recordsList:
    if rec['Processed?'] == '':
        newList.append(rec)

numRecsToProcess = len(newList)
if numRecsToProcess == 0:
    #Despite there being records in the spreadsheet none need processing
    writeToLog(gc,logFileId,'No New SARs...Up to date')
    writeToLog(gc,logFileId,'Process Finished at ' + getTimeString())
    writeToLog(gc,logFileId,'***************')
    sys.exit()


#if code gets here we have SARs to process
writeToLog(gc,logFileId,'New SARs: ' + str(len(newList)))

#Now we want to check for data integrity. We want to avoid a situation where
#there is an action but no Reference Number given. So things can still fall
#apart at this stage
for item in newList:
    if checkDataConsistency(item) == False:
        writeToLog(gc,logFileId,'Inconsistent Data...Fields missing')
        writeToLog(gc,logFileId,'Process Finished at ' + getTimeString())
        sys.exit()
        

#now we will begin a loop to
#1. create new subdirectory for the new SAR
#2. create a copy of the SAR template for the new SAR
#3. rename the copy
#4. move the copy into the new subdirectory
#5. fill out the header of the new SAR file

i = 1 # starting index
for item in newList:
    #code will loop through all the requests picked up
    #carry out the folder work as described above
    
    #subject requested for both action. The code will call the folderwork
    #process and the set template header process twice
    #one for access and one for delete

    #set up the refs
    delRef = 'D' + str(item['If Deletion Please Enter Next D-Number:'])
    aRef = 'S' + str(item['If DSAR Please Enter Next S-Number:'])
        
    if (item['Action Required:'] == 'Both (Access and Deletion)'):
        writeToLog(gc,logFileId,'Creating Folder And Summary Template For ' + aRef)
        tFileId = doFolderWork(gService,
                               item['Requester\'s Name:'],
                               item['Enter DSR Email Address:'],
                               item['If DSAR Please Enter Next S-Number:'],
                               'Access'
                               )
        #set the template file headers
        writeToLog(gc,logFileId,'Writing Header Info For ' + aRef)
        setTemplateFileHeader(gc,
                          tFileId,
                          item['Requester\'s Name:'],
                          item['Enter DSR Email Address:'],
                          aRef,
                          item['Received Date:'],    
                          item['Due Date:'],
                          'No') #default
        
        #now do the same for the delete part
        writeToLog(gc,logFileId,'Creating Folder And Summary Template For ' + delRef)
        tFileId = doFolderWork(gService,
                               item['Requester\'s Name:'],
                               item['Enter DSR Email Address:'],
                               item['If Deletion Please Enter Next D-Number:'],
                               'Delete'
                               )
        #set the template file headers
        writeToLog(gc,logFileId,'Writing Header Info For ' + delRef)
        setTemplateFileHeader(gc,
                          tFileId,
                          item['Requester\'s Name:'],
                          item['Enter DSR Email Address:'],
                          delRef,
                          item['Received Date:'],    
                          item['Due Date:'],
                          'No')

    #this is the process just for the access
    if (item['Action Required:'] == 'Access To Information'):
        writeToLog(gc,logFileId,'Creating Folder And Summary Template For ' + aRef)
        tFileId = doFolderWork(gService,
                               item['Requester\'s Name:'],
                               item['Enter DSR Email Address:'],
                               item['If DSAR Please Enter Next S-Number:'],
                               'Access'
                               )
        #set the template file headers
        writeToLog(gc,logFileId,'Writing Header Info For ' + aRef)
        setTemplateFileHeader(gc,
                          tFileId,
                          item['Requester\'s Name:'],
                          item['Enter DSR Email Address:'],
                          aRef,
                          item['Received Date:'],    
                          item['Due Date:'],
                          'No')
        
    #this is the process just for the delete
    if (item['Action Required:'] == 'Deletion (Deletion Of Information)'):
        writeToLog(gc,logFileId,'Creating Folder For ' + delRef)
        tFileId = doFolderWork(gService,
                               item['Requester\'s Name:'],
                               item['Enter DSR Email Address:'],
                               item['If Deletion Please Enter Next D-Number:'],
                               'Delete'
                               )
        #set the template file headers
        writeToLog(gc,logFileId,'Writing Header Info For ' + delRef)
        setTemplateFileHeader(gc,
                          tFileId,
                          item['Requester\'s Name:'],
                          item['Enter DSR Email Address:'],
                          delRef,
                          item['Received Date:'],    
                          item['Due Date:'],
                          'No')

    updateSpreadsheetRecord(fs1,totalRecordsInSheet,numRecsToProcess,i)
    i+=1 #increase the counter for the next process
        
#After the file work and template header work is done,
#we now need to fill out the input sheets in readiness for the cron
#proceses to pick up and process
        
writeToLog(gc,logFileId,'Filling Input Sheets')

fillAllInputSheets(gc,newList)

writeToLog(gc,logFileId, getTimeString() + '  Process Ended')
writeToLog(gc,logFileId, '******************') #separator

##############THE END##############################################


 
        
        
    
    













