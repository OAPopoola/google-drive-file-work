# google-drive-file-work
This python code copies a template file, renames it, moves it to a new folder and fill out its header information
This code runs as a cron job on the virtual machine but does the work in the google drive. It used oauth authenthication for google drive and gspread. 
The code reads a google sheet to see whether something needs doing. After it completes the task it writes back the same google sheet filling the 'Processed:' field with a date and time the task was completed
