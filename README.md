# Balloon Data Download 

The purpose of this program is to read data from the wspr.live website and put it into a SQLite database table.

The program reads data for each flight to be tracked from a control table (Control) in the same SQLite data base. The data is filtered by the correct band/time slot and then placed into the rawWSPR table of the database.  By entering the correct data into the control table the data for a past flight will be downloaded in 24 hour chunks or smaller if the time to current utc is less.  Data down load will continue until the current utc or the endTime in the control table.

It is recommended the SQLiteStudio be used to investigate the raw data and modify the control table.  This product has a form view that can be used to crud individual records in the control table.

The main program is ReadWSPRData.py. You will need to define the locaton of the database in this file.
dbLocation = "./sqlite/WSPRstats.db"

Dates in the rawWSPR tble are stored as utc timestamps.  Here is an example query:

SELECT
date(Timestamp,'unixepoch') 
,time(Timestamp,'unixepoch')
,datetime(Timestamp,'unixepoch')
from rawWSPR
where date(Timestamp,'unixepoch')  > date('2024-02-30')

