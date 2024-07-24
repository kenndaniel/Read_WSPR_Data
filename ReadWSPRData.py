import urllib.request
import requests
import json
from datetime import datetime as dt
from datetime import timedelta, timezone
import sqlite3
from sqlite3 import Error
import time as tm
import datetime

from dbInterface import *
from filterSpots import *


#SpotID Timestamp RecCall  RepotGrid SNR Freq SendCall Maid Pwr Drift Distance Azimuth Band Ver Code
  #  Spot ID
  # A unique integer identifying the spot which otherwise carries no information. Used as primary key in the database table. Not all spot numbers exist  and the files may not be in spot number order
  # Timestamp
  # The time of the spot in unix time() format (seconds since 1970-01-01 00:00 UTC). To convert to an excel date value  use =time_cell/86400+"1/1/70" and then format it as a date/time.
  # Reporter
  # The station reporting the spot. Usually an amateur call sign  but several SWLs have participated using other identifiers. Maximum of 10 characters.
  # Reporter's Grid
  # Maidenhead grid locator of the reporting station  in 4- or 6-character format.
  # SNR
  # Signal to noise ratio in dB as reported by the receiving software. WSPR reports SNR referenced to a 2500 Hz bandwidth; typical values are -30 to +20dB.
  # Frequency
  # Frequency of the received signal in MHz
  # Call Sign
  # Call sign of the transmitting station. WSPR encoding of callsigns does not encode portable or other qualifying (slash) designators  so the call may not represent the true location of the transmitting station. Maximum of 6 characters.
  # Grid
  # Maidenhead grid locator of transmitting station  in 4- or 6-character format.
  # Power, timezone
  # Power  as reported by transmitting station in the transmission. Units are dBm (decibels relative to 1 milliwatt; 30dBm=1W). Typical values are 0-50dBm  though a few are negative (< 1 mW).
  # Drift
  # The measured drift of the transmitted signal as seen by the receiver  in Hz/minute. Mostly of use to make the transmitting station aware of systematic drift of the transmitter. Typical values are -3 to 3.
  # Distance
  # Approximate distance between transmitter and receiver along the great circle (short) path  in kilometers. Computed form the reported grid squares.
  # Azimuthcreate_connection
  # Approximate direction  in degrees  from transmitting station to receiving station along the great circle (short) path.
  # BandSpotID = 0 
Timestamp =1  
RecCall=2
RepotGrid=3
SNR=4 
Freq=5 
SendCall=6 
SendGrid=7 
Pwr=8 
Drift=9 
Distance=10 
Azimuth=11 
Band=12 
Ver=13 
Code=14
  # Band of operation  computed from frequency as an index for fasteimport pandas as pdr retrieval. This may change in the future  but at the moment  it is just an integer representing the MHz component of the frequency with a special case for LF (-1: LF  0: MF  1: 160m  3: 80m  5: 60m  7: 40m  10: 30m  ...).
  # Version
  # Version string of the WSPR software in use by the receiving station. May be bank  as versions were not reported until version 0.6 or 0.7  and version reporting is only done through the realtime upload interface (not the bulk upload).
  # Code
  # Archives generated after 22 Dec 2010 have an additional integer Code how do you put a time delay in a python program without using import time?field. Non-zero values will indicate that the spot is likely to be erroneous (bogus callsign  appears to be wrong band  appears to be an in-band mixing product  etc. When implemented  the specific codes will be documented here.

# When data is found it is placed in this table.
# SQLlite Table that is used
# CREATE TABLE "rawWSPR" (
#     SpotID    BIGINT      CONSTRAINT SpotIDNotNull NOT , timezoneNULL
#                           CONSTRAINT UniqueWSPRSpotID UNIQUE ON CONFLICT IGNORE,
#     Timestamp INTEGER     CONSTRAINT TimestampNotNull NOT NULL,
#     RecCall   STRING (9),
#     RepotGrid STRING (9),
#     SNR       REAL,
#     Freq      REAL,
#     SendCall  STRING (9),
#     SendGrid  STRING (9),how do you put a time delay in a python program without using import time?
#     Pwr       INTEGER,
#     Drift     INTEGER,
#     Distance  REAL,
#     Azimuth   REAL,
#     Band      INTEGER,
#     Ver       STRING (10),
#     Code      STRING (10),
#     Flight    STRING      DEFAULT none
# );
#  get data from https://db1.wspr.live

columns = ['key','SpotID', 'Timestamp', 'RecCall','RepotGrid','SNR','Freq','SendCall','Pwr','Drift','Distance','Band']
Bands = [ '10', '14', '18', '21', '24', '28']
dbLocation = "./sqlite/WSPRstats.db"



# SpotID = 0 
# Timestamp =1  
# RecCall=2
# RepotGrid=3
# SNR=4 
# Freq=5 
# SendCall=6 
# SendGrid=7 
# Pwr=8 
# Drift=9 
# Distance=10 
# Azimuth=11 
# Band=12 Qtx_sign
# Ver=13 
# Code=14


# rawData:
#"id"0,"time"1,"band"2,"rx_sign"3,"rx_lat"4,"rx_lon"5,"rx_loc"6,"tx_sign"7,"tx_lat"8,"tx_lon"9,"tx_loc"10,
#"distance"11,"azimuth"12,"rx_azimuth"13,"frequency"14,"power"15,"snr"16,"drift"17,"version"18,"code"19

#7371736804,"2024-03-24 00:34:00",14,"WA2TP",40.854,-73.042,"FN30lu","K9YO",42.271,-87.958,
#"EN62ag",1249,92,282,14097105,0,-22,0,"WD_3.1.5",1create_connection
def convertWSPRLive(line):
    rawData = line.split(",")
    rawData[1] = rawData[1].replace('\"','')
    spotTime = dt.fromisoformat(rawData[1])
    unixTime = spotTime.timestamp()
    rawData[3] = rawData[3].replace('\"','')
    rawData[6] = rawData[6].replace('\"','')
    rawData[7] = rawData[7].replace('\"','')
    rawData[10] = rawData[10].replace('\"','')
    rawData[18] = rawData[18].replace('\"','')
    data = [1]*15
    data[SpotID] = int(rawData[0])
    data[Timestamp] = int(unixTime) 
    data[RecCall]= rawData[3]
    data[RepotGrid]= rawData[6]
    data[SNR]= int(rawData[16])
    data[Freq]=int(rawData[14]) 
    data[SendCall]=rawData[7]
    data[SendGrid]=rawData[10]
    data[Pwr]=int(rawData[15]) 
    data[Drift]=int(rawData[17]) 
    data[Distance]=int(rawData[11]) 
    data[Azimuth]=int(rawData[13]) 
    data[Band]= int(rawData[2])
    data[Ver]=rawData[18]
    data[Code]=rawData[19]
    for i in range(0,15):
        data[i] = str(data[i])

    #print(",".join(data))
    return data, spotTime
  
def processResponse(response):
    # orchistrate the processing of each line of the response
    lines=response.text.split('\n')
    numLines = len(lines)

    recordCnt = 0
    con = create_connection(dbLocation)
    data = []
    first = True
    for line in lines:
        print(line)
        if first == True:
            first = False
            continue
        if len(line)< 90 : continue
        if 'The database contains' in line: break # no response form db
        data, spotTime = convertWSPRLive(line)
        if data[12] != str(band): continue  # filter on band
        store = findSpot(data, spotTime, timeSlot,callsign, stdTelemCall, telemCall1, telemCall2)
        if store == True : 
            insertDB(data,flight,con)
            recordCnt += 1

    close_connection(con)
    print(' Number of response lines ' + str(numLines))
    print(' Number of records saved ' + str(recordCnt))
    return recordCnt

def getData(Callsign,startTime,endTime):
    # Get the data from wspr.live
    # Define the URL and the payload, timezone
    url = 'https://wspr.live/wspr_downloader.php'
    payload = {
        'start': startTime.strftime("%Y/%m/%d %H:%M:%S"),
        'end': endTime.strftime("%Y/%m/%d %H:%M:%S"),
        'tx_sign':Callsign,
        'format' :'CSV'
    }

    # Send the POST request
    response = requests.post(url, data=payload)
    # Check if the request was successful
    if response.status_code == 200:
        print('http response received '), timezone
    else:
        print('An error occurred.')
        print(response.text)
    return response 



#*************Begin Main Program ********************************

# Example Data
    # stdTelemCall = "Q5"
    # timeSlot = 6
    # ballID = 9  # Balloon ID for special telemetry messages (Todd)
    # band = 14
    # callsign = "NE4JJ"
    # flight = '\'2024-3-2 Jim\''  # Name of flight - put in the Flight filed of each record stored in the rawWSPR table.
    # startTime = '2024-03-23 16:00:00'
    # endTime = '2024-03-23 20:00:00'

conn = create_connection(dbLocation)
rows = getContol(conn) # Retrieve download control information
requestHrs = 24 # get data for this many hours per chunk

# Read control informaton
for row in rows[:]:
    flight = row[0] # name of flight
    print(flight)
    startTimeLocal = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S') 
    startTime = startTimeLocal.replace(tzinfo=timezone.utc) 
    endTimeLocal = datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
    endTime = endTimeLocal.replace(tzinfo=timezone.utc) 
    band = row[3]
    callsign = row[4]
    stdTelemCall = row[5]
    timeSlot = row[6]
    ballID = row[7] # Balloon designator - for Todd messages
    freqChannel = row[8]
    lastProcessed = datetime.strptime(row[9], '%Y-%m-%d %H:%M:%S')

    Qtx_sign = stdTelemCall[0]+'_'+stdTelemCall[1]+'%'
    telemCall1 = "T1"  # telemetry prefix call sign for Todd 1
    telemCall2 = "T9"  # telemetry prefix call sign for Todd 2
    tCall1tx_sign = telemCall1+str(ballID)
    tCall2tx_sign = telemCall2+str(ballID)

    if lastProcessed.timestamp() >= endTime.timestamp():
        continue # finished getting data for this flight
   
    if lastProcessed.timestamp() < startTime.timestamp():
        lastProcessed = startTime
    endProcessing = lastProcessed + timedelta(hours=requestHrs) # Try to process xx hrs
    utcNow =  datetime.now(timezone.utc)-timedelta(minutes=30) # Data will be processed up to 30 min before current time
    if endProcessing.timestamp() > utcNow.timestamp():
        endProcessing = utcNow - timedelta(minutes=1)  # make sure processing goes at least once
    # Process messages until the current time is reached or the flight end time is reached.
    while((lastProcessed.timestamp() < utcNow.timestamp()) and (endProcessing != utcNow)) and (lastProcessed.timestamp() <= endTime.timestamp()):
        for times in range(2):  # the request is done twice for reliability reasons - needs testing, might not be needed
            endProcessing = lastProcessed + timedelta(hours=requestHrs)
            if endProcessing.timestamp() > utcNow.timestamp(): 
                endProcessing = utcNow            # process the last littel chunk
            print("**********processing "+callsign+" " + flight+" "+str(times) +" "+ lastProcessed.strftime('%m-%d %H:%M') +' ************')

            # process standard call
            print("processing "+callsign+" " + flight+" "+str(times) +" "+ lastProcessed.strftime('%m-%d %H:%M'))
            response = getData(callsign,lastProcessed,endProcessing)
            numStoredRec = processResponse(response)
            tm.sleep(10)
            if numStoredRec == 0: continue # No callsign records found
            # process telemetry call
            print("processing "+Qtx_sign+" " + flight+" "+str(times) +" "+ lastProcessed.strftime('%m-%d %H:%M'))
            response = getData(Qtx_sign,lastProcessed,endProcessing)
            processResponse(response)
            tm.sleep(10)
            # # process first special call for Todd
            print("processing " + tCall1tx_sign+" " + flight+" "+str(times) +" "+ lastProcessed.strftime('%m-%d %H:%M'))
            response = getData(tCall1tx_sign,lastProcessed,endProcessing)
            numStoredRec    = processResponse(response)
            if numStoredRec == 0: continue # No special records found
            tm.sleep(10)
            # process second special call for Todd
            print("processing " + tCall2tx_sign+" " + flight+" "+str(times) +" "+ lastProcessed.strftime('%m-%d %H:%M'))
            response = getData(tCall2tx_sign,lastProcessed,endProcessing)
            processResponse(response)
            tm.sleep(10)

        lastProcessed = endProcessing
        updateControl(lastProcessed,flight,conn)

close_connection(conn)



    