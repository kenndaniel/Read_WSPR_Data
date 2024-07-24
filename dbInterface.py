
from datetime import datetime
#import pandas as pd
import sqlite3
from sqlite3 import Error
#SpotID Timestamp RecCall  RepotGrid SNR Freq SendCall Maid Pwr Drift Distance Azimuth Band Ver Code
  #  Spot ID
  # A unique integer identifying the spot which otherwise carries no information. Used as primary key in the database table. Not all spot numbers exist  and the files may not be in spot number order
  # Timestamp
  # The time of the spot in unix time() format (seconds since 1970-01-01 00:00 UTC). To convert to an excel date value  use =time_cell/86400+"1/1/70" and then format it as a date/time.
  # Reporter
  # The station reporting theSendCall spot. Usually an amateur call sign  but several SWLs have participated using other identifiers. Maximum of 10 characters.
  # Reporter's Grid
  # Maidenhead grid locator of the reporti#import pandas as pdng station  in 4- or 6-character format.
  # SNR
  # Signal to noise ratio in dB as reported by the receiving software. WSPR reports SNR referenced to a 2500 Hz bandwidth; typical values are -30 to +20dB.
  # Frequency
  # Frequency of the received signal in MHz
  # Call Sign
  # Call sign of the transmitting station. WSPR encoding of callsigns does not encode portable or other qualifying (slash) designators  so the call may not represent the true location of the transmitting station. Maximum of 6 characters.
  # Grid
  # Maidenhead grid locator of transmitting station  in 4- or 6-character format.
  # Power
  # Power  as reported by transmitting station in the transmission. Units are dBm (decibels relative to 1 milliwatt; 30dBm=1W). Typical values are 0-50dBm  though a few are negative (< 1 mW).
  # Drift
  # The measured drift of the transmitted signal as seen by the receiver  in Hz/minute. Mostly of use to make the transmitting station aware of systematic drift of the transmitter. Typical values are -3 to 3.
  # Distance
  # Approximate distance between transmitter and receiver along the great circle (short) path  in kilometers. Computed form the reported grid squares.
  # Azimuth
  # Approximate direction  in degrees  from transmitting station to receiving station along the great circle (short) path.
  # Band
  # Band of operation  computed from frequency as an index for faster retrieval. This may change in the future  but at the moment  it is just an integer representing the MHz component of the frequency with a special case for LF (-1: LF  0: MF  1: 160m  3: 80m  5: 60m  7: 40m  10: 30m  ...).
  # Version
  # Version string of the WSPR software in use by the receiving station. May be bank  as versions were not reported until version 0.6 or 0.7  and version reporting is only done through the realtime upload interface (not the bulk upload).
  # Code
  # Archives generated after 22 Dec 2010 have an additional integer Code field. Non-zero values will indicate that the spot is likely to be erroneous (bogus callsign  appears to be wrong band  appears to be an in-band mixing product  etc. When implemented  the specific codes will be documented here.

# When data is found it is placed in this table.
# SQLlite Table that is used
# CREATE TABLE "rawWSPR" (
#     SpotID    BIGINT      CONSTRAINT SpotIDNotNull NOT NULL
#                           CONSTRAINT UniqueWSPRSpotID UNIQUE ON CONFLICT IGNORE,
#     Timestamp INTEGER     CONSTRAINT TimestampNotNull NOT NULL,
#     RecCall   STRING (9),
#     RepotGrid STRING (9),
#     SNR       REAL,
#     Freq      REAL,
#     SendCall  STRING (9),
#     SendGrid  STRING (9),
#     Pwr       INTEGER,
#     Drift     INTEGER,
#     Distance  REAL,
#     Azimuth   REAL,
#     Band      INTEGER,
#     Ver       STRING (10),
#     Code      STRING (10),
#     Flight    STRING      DEFAULT none
# );



SpotID = 0 
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

insert = 'INSERT INTO k9yoSpots (SpotID,Timestamp,RecCall,RepotGrid,SNR,Freq,SendCall,SendGrid,Pwr,Drift,Distance,Azimuth,Band,Ver,Code,Flight)VALUES ('
Bands = [ '10', '14', '18', '21', '28']
def StdLoadDict(data, spotDict):
    # Load a Standard message dict
    key = data[Timestamp]+data[RecCall]
    spotDict.update({               \
        'key': key,                  \
        'SpotID':data[SpotID],      \
        'Timestamp':data[Timestamp], \
        'RecCall':data[RecCall],     \
        'RepotGrid':data[RepotGrid], \
        'SNR':data[SNR],            \
        'Freq':data[Freq],          \
        'SendCall':data[SendCall],  \
        'Pwr':data[Pwr],            \
        'Drift':data[Drift],        \
        'Distance':data[Distance],  \
        'Band':data[Band]   })
    return

def TelemLoadDict(data, spotDict):
    # Load a Telementry message dict
    seconds = int(data[Timestamp])-120
    key = str(seconds)+data[RecCall]
    spotDict.update({               \
        'key':key,
        'TelemCall':data[SpotID],   \
        'TelemLoc':data[RepotGrid], \
        'TelemPwr':data[Pwr],   \
        'TelemFreq':data[Freq],     \
        'Timestamp':data[Timestamp],     \
        'Match':False,     \
        'TelemRecCall':data[RecCall] }) 
    return

def SpotJoin(StdDict,telemDict):
    # Find matching Stdandard Message
    stdSpotDict = StdDict.get(telemDict['key'])
    if (stdSpotDict is None) :
        telemDict['StdCall']=None
        return False
    else:
        telemDict['StdCall']=stdSpotDict
        telemDict['Match']=True
    return True

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    #conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def close_connection(conn):
    conn.close()
    return


def insertDB(data,flight,conn):
    if conn == None: print(" Need to call create_connecton ")
    data[SendCall] = '\''+data[SendCall]+'\''
    data[RecCall] = '\''+data[RecCall]+'\''
    data[RepotGrid] = '\''+data[RepotGrid]+'\''
    data[SendGrid] = '\''+data[SendGrid]+'\''
    data[Code] = '\''+data[Code]+'\''
    data[Ver] = '\''+data[Ver]+'\''
    dataString = ",".join(data)

    insertCmd = insert+dataString+','+'\''+flight+'\'' ')'
    #print(insertCmd)
    try:
        cur = conn.cursor()
        cur.execute(insertCmd)
        conn.commit()
    except Error as e:
        print(e)
        print(insertCmd[90:])

    return 


def getContol(conn):
    """
    Get locations from rawWSPR Table
    """
    cur = conn.cursor()

    select = ('select flight, startTime, endTime,band, callsign, stdTelemCall, '
              'timeSlot,ballDesi,freqChannel,lastProcessed '
              'from Control '
              'where lastProcessed < endTime '
              ' order by lastProcessed, callsign'
                )

    cur.execute(select)

    rows = cur.fetchall()

    return rows

def updateControl(lastProcessed,flight,conn):
    if conn == None: print(" Need to call create_connecton ")

    update = ('update control set lastProcessed = \'' 
              + lastProcessed.strftime('%Y-%m-%d %H:%M:%S') +
              '\' where flight = \'' + flight +'\'')
    
    try:
        cur = conn.cursor()
        cur.execute(update)
        conn.commit()
    except Error as e:
        print(e)
        print(update[90:])

    return 

def getNewestRecord(conn):
    """
    Get locations from rawWSPR Table
    """
    cur = conn.cursor()

    select = ('select flight, startTime, endTime,band, callsign, stdTelemCall, '
              'timeSlot,ballDesi,freqChannel,lastProcessed '
              'from Control '
              'where lastProcessed < endTime '
              ' order by lastProcessed, callsign'
                )

    cur.execute(select)

    rows = cur.fetchall()

    return rows
