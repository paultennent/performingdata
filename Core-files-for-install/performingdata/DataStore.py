#todo - fix bulk reads (just read a certain amount each time and send straight out)
#todo: fix multiple attempts to record same stream/port etc. - ie. don't open multiple datareceivers for the same data
# 
from DataStoreReader import DataStoreReader,DataStoreCommandReader

from DataReceiver import DataReceiver
from ChildDataSender import ChildDataSender
from SenderParent import SenderParent
from collections import deque
# use newest pysqlite2
import pysqlite2.dbapi2 as sqlite3
#import sqlite3
import sys
import time
import threading
import xml.dom.minidom 
#data table has: 
#_id (PKEY, automatic), stream ID, timestamp, value 
#indices: stream ID, (stream ID,timestamp)

#streams table has:

#_id (PKEY), friendly name, ip address, port,dataTable

#replay needs:
# a)get a chunk of data by stream ( identify by hostalias, port, data table) and timestamp
# b)stream data from a time (in real time?) (thin out data or anything?)

# request with parameters

# get data from host:port
# get data from host@<streamData mintime=? maxtime=?/>
# get data from host@<dumpData mintime=? maxtime=? />

# queries are xml format: <replay table='' streamID='' minTime='' maxTime='' delay='' />
# or                xml format: <dump table='' streamID='' minTime='' maxTime='' />

# also need: list the streams in the database
# <liststreams/> - returns xml with the table,streamID,query,host etc. stuff, for each stream, plus min / max times

#set which streams are recording to the database
# - to stop recording a stream, make live = 0
#<recordstreams>
#<stream hostAddress='127.0.0.1' hostAlias='processor' query='1' port='49991' friendlyName='test-vilistus1' dataTable='BIG_DATA' live='1' /> 
#</recordstreams>
# on startup, try to record from all currently live streams - this way if the db dies, we get data back quickly
#
# to find out min and max times for streams use:
# <streaminfo id="" table=""/>
# and you should get back
# <streaminfo id="" table="" minTime="" maxTime=""/>


class DataStore():
    def __init__(self,dbName,port):
        self.inStreams=[]
        self.dbName=dbName
        self.port=port

    def run(self):
        print "run db engine"
        self.dbEvent = threading.Event()
        self.dbWrites=deque([])
        self.dbReads=deque([])

        self.dbStreamTransactions=deque([])
        
        self.initialTimestamp = time.time()
        self.initialClock = time.clock()
        
        #create or open database
        self.dbConnection=sqlite3.connect(self.dbName)        
        # set the journal mode to persist (ie. stop recreating and deleting the journal file)
        self.dbConnection.execute('PRAGMA journal_mode=WAL')
#        self.dbConnection.execute('PRAGMA locking_mode=EXCLUSIVE')
#        self.dbConnection.execute('PRAGMA cache_size=100000')
#        self.dbConnection.execute('PRAGMA read_uncommitted=TRUE')
        self.dbConnection.execute('PRAGMA synchronous=OFF')
        # store temporary tables in memory  (use them for reading from for speed reasons)
#        self.dbConnection.execute('PRAGMA temp_store=memory')
#        self.dbConnection.execute('PRAGMA temp.journal_mode=OFF')
        # make sure the streams table exists
        self.createStreamsTableIfNeeded()

        self.dataSenders=[]
        self.senderParent=SenderParent(self.port,self)
        self.senderParent.start()

        # restart all live streams to record from wherever they last connected to:- we can just do this by getting the stream list:
        #currentStreams=self.getStreamList()
        #print currentStreams
        #self.createStreamsFromXML(currentStreams)

        readCursor=self.dbConnection.cursor()
        readCursor.arraysize=256        
        
        # do all database reads and writes on this thread        
        startTime=time.clock()
        endTime=time.clock()
        numWrites=0
        while True:
            # do reads in the same loop / transaction as writes, so as to avoid locking problems - we don't care about consistency at this level, we just want to read stuff quickly
            with self.dbConnection:
                while endTime-startTime<5:
                    count=0
                    readNum=len(self.dbReads)
                    writeNum=len(self.dbWrites)
                    metadataNum=len(self.dbStreamTransactions)
                    if readNum==0 and writeNum==0 and metadataNum==0: 
                        self.dbEvent.wait()
                    while (readNum>=1 or writeNum>=1 or metadataNum>=1) and count<20000:
                        if writeNum>=1:
                            (dataTable,streamID,timestamp,dataLine) = self.dbWrites.popleft()
                            writeNum=writeNum-1
                            self.dbConnection.execute('insert into %s (stream_id,timestamp,value) values(?,?,?)'%dataTable,(streamID,timestamp,dataLine))
                        if readNum>=1:
                            (sql,dbReader)=self.dbReads.popleft()
                            try:
                                readCursor.execute(sql)
                                dbReader.onDBRead(readCursor,sql)
                            except sqlite3.IntegrityError,e:
                                print "table already exists"
                                None
                            readNum=readNum-1
                        if metadataNum>=1:
                            (xml,sender)=self.dbStreamTransactions.popleft()
                            self.handleMetadataXML(xml,sender)
                            metadataNum=metadataNum-1
                        count=count+1
                    self.dbEvent.clear()
                    numWrites+=count
                    endTime=time.clock()
                    liveReaders=[]
                    for c in self.dataSenders:
                        if not c[1].finished:
                            liveReaders.append(c)
                    self.dataSenders=liveReaders
            print "%d writes in %f seconds (%f/sec) [%d DBreader,%d DBwriter]"%(numWrites,endTime-startTime,numWrites/(endTime-startTime),len(self.dataSenders),len(self.inStreams))
            numWrites=0
            startTime=endTime
#            if endTime-self.initialClock>10:
#                break
            

    def getAttributeInt(self,c,name,defVal):
        try:
            retVal = c.getAttribute(name)
            retVal = int(retVal)
        except ValueError,v:
            retVal=defVal
        return retVal
            
    def getAttributeString(self,c,name,defVal):
        retVal=c.getAttribute(name)
        if len(retVal)==0:
            retVal = defVal
        return retVal

    def getStreamInfo(self,stream,table):
        # <streaminfo id="" table="" minTime="" maxTime=""/>
        minTime=-1
        maxTime=-1
        cur=self.dbConnection.execute("SELECT min(timestamp) from %s where stream_id=%d"%(table,stream))
        for thisTime in cur:
            minTime=thisTime[0]
        cur=self.dbConnection.execute("SELECT max(timestamp) from %s where stream_id=%d"%(table,stream))
        for thisTime in cur:
            maxTime=thisTime[0]
        retVal="<streaminfo id='%d' table='%s' minTime='%d' maxTime='%d'/>"%(stream,table,minTime,maxTime)
        return retVal
        
        
    def getStreamList(self):
        retVal="<recordstreams>\n"
        #  the stream list from the database and makes it into xml
        cur=self.dbConnection.execute("SELECT _id,hostAddress,hostAlias,query,port,friendlyName,dataTable,live FROM streams")
        for (id,hostAddress,hostAlias,query,port,friendlyName,dataTable,live) in cur:
            retVal+="  <stream id='%s' hostAddress='%s' hostAlias='%s' query='%s' port='%d' friendlyName='%s' dataTable='%s' live='%d' />\n"%(id,hostAddress,hostAlias,query,port,friendlyName,dataTable,live)
        retVal+="</recordstreams>\n"
        return retVal

    def handleMetadataXML(self,queryString,sender):
        document=xml.dom.minidom.parseString(queryString)
        responseData=""
        alterstreams=document.getElementsByTagName("recordstreams")
        streamInfos=document.getElementsByTagName("streaminfos")

        # <streaminfos>
        #<streaminfo id="" dataTable="" minTime="" maxTime=""/>
        if len(streamInfos)>0:
            responseData="<streaminfos>"
            for c in streamInfos[0].getElementsByTagName("streaminfo"):
                tableName = self.getAttributeString(c,'dataTable','')
                id = self.getAttributeInt(c,'id',-1)
                responseData+=self.getStreamInfo(id,tableName)
            responseData+="</streaminfos>"                
        # <alterrecordingstreams 
        if len(alterstreams)>0:
            for c in alterstreams:
                self.createStreamsFromXMLNode(c)
            responseData+=self.getStreamList()

        # sender with results from the liststreams / alterstreams queries
        if len(responseData)>0:
            sender.pushData(responseData)
        sender.closeAfterPush()
    
        
    # this is called by senderparent to get a datasender for a particular query
    # queries are xml format: <replay table='' streamID='' minTime='' maxTime='' delay='' />
    # or                xml format: <dump table='' streamID='' minTime='' maxTime='' />
    def getMultiStream(self,queryString):
        print "connect multistream %s"%queryString
        document=xml.dom.minidom.parseString(queryString)

        # alterrecordingstreams ...etc
        # on startup, try to record from all currently live streams - this way if the db dies, we get data back recording quickly
        alterstreams=document.getElementsByTagName("recordstreams")
        streamInfos=document.getElementsByTagName("streaminfos")        
        if len(alterstreams)>0 or len(streamInfos)>0:
            sender = ChildDataSender(0,self)
            sender.setPush(True)
            sender.start()
            self.dbStreamTransactions.append((queryString,sender))
            self.dbEvent.set()
            return sender

        liststreams=document.getElementsByTagName("liststreams")
        for c in liststreams:
            responseData+=self.getStreamList()

        # return a sender that has a cursor into a stream
        # which can be controlled using backchannel data commands
        streams = document.getElementsByTagName("cursor")
        for c in streams:
            tableName = self.getAttributeString(c,'dataTable','')
            streamID = self.getAttributeInt(c,'id',-1)
            if streamID!=-1 and len(tableName)>0:
                sender = ChildDataSender(0,self)
                sender.setPush(True)
                dbReader=DataStoreCommandReader(self.dbName,tableName,int(streamID),sender)
                self.dataSenders.append((sender,dbReader))
                sender.start()
                dbReader.start()
                # can only return one stream per connection so ignore any further tags
                return sender            
        streams = document.getElementsByTagName("replay")
        for c in streams:
            tableName = self.getAttributeString(c,'dataTable','')
            streamID = self.getAttributeInt(c,'id',-1)
            minTime = self.getAttributeInt(c,'minTime',-1)
            maxTime = self.getAttributeInt(c,'maxTime',-1)
            delay = self.getAttributeInt(c,'delay',-1)
            #print (tableName,int(streamID),int(minTime),int(maxTime),int(delay),True)
            if streamID!=-1 and len(tableName)>0:
                sender = ChildDataSender(0,self)
                sender.setPush(True)
                dbReader=DataStoreReader(self.dbName,tableName,int(streamID),int(minTime),int(maxTime),int(delay),True,sender,self)
                self.dataSenders.append((sender,dbReader))
                sender.start()
                dbReader.start()
                # can only return one stream per connection so ignore any further tags
                return sender
        streams = document.getElementsByTagName("dump")
        for c in streams:
            tableName = self.getAttributeString(c,'dataTable','')
            streamID = self.getAttributeInt(c,'id',-1)
            minTime = self.getAttributeInt(c,'minTime',-1)
            maxTime = self.getAttributeInt(c,'maxTime',-1)
            delay = self.getAttributeInt(c,'delay',-1)
#            print (tableName,int(streamID),int(minTime),int(maxTime),int(delay),True)
            if streamID!=-1 and len(tableName)>0:
                sender = ChildDataSender(0,self)
                sender.setPush(True)
                dbReader=DataStoreReader(tableName,int(streamID),int(minTime),int(maxTime),int(delay),False,sender,self)
                self.dataSenders.append((sender,dbReader))
                sender.start()
                dbReader.start()
                # can actually only return one stream per connection
                return sender

                
                
        return None
            
    # static list of non alphabetic characters used in removeNonAlphabetCharacters below
    NON_ALPHA_CHARS = ''.join(c for c in map(chr, range(256)) if (not c.isalnum() and not c=='_'))
        
    #sanitise a table name (because we can't use SQL parameterisation on it)
    def removeNonAlphabetCharacters(self,name):
        return name.translate(None,DataStore.NON_ALPHA_CHARS)
        
    def checkTableExists(self,tableName):
        dbCur = self.dbConnection.cursor()
        dbCur.execute("SELECT name FROM sqlite_master WHERE name=?",(tableName,))
        if dbCur.fetchone() == None:
            print "table %s doesn't exist"%tableName
            return False
        else:
            #print "table %s does exist"%tableName
            return True
        
    def createStreamsTableIfNeeded(self):
        if not self.checkTableExists("streams"):
            with self.dbConnection:
                self.dbConnection.execute("create table 'streams' (_id integer primary key,hostAddress varchar(256),hostAlias varchar(256),query varchar(256),port integer,friendlyName varchar(256),dataTable varchar(64),live integer)")
            
    def createdataTableIfNeeded(self,tableName):
        tableName=self.removeNonAlphabetCharacters(str(tableName))
        if not self.checkTableExists(tableName):
            with self.dbConnection:
                self.dbConnection.execute('create table %s (_id integer primary key,stream_id integer,timestamp integer,value text)'%tableName)
                self.dbConnection.execute('create index %s on %s (timestamp,stream_id)'%(tableName+"_idx_timestamp_stream_id",tableName))
        
# read some xml and do createstreams from it in order to start reading these streams in
#<xml>
#    <stream hostAlias="" hostAddress="" port="" dataTable="" friendlyName=""/>
#</xml>
    def createStreamsFromXML(self,xmlString):
        document=xml.dom.minidom.parseString(xmlString)
        self.createStreamsFromXMLNode(document)

    def createStreamsFromXMLNode(self,node):
        streams = node.getElementsByTagName("stream")
        #print streams
        for c in streams:
            # if we have an ID attribute, then we need to alter that stream in the table and ignore the datatable / hostalias / query?
            id = self.getAttributeInt(c,'id',-1)
            dataTable = self.getAttributeString(c,'dataTable','')
            hostAddress = self.getAttributeString(c,'hostAddress','')
            hostAlias = self.getAttributeString(c,'hostAlias','')
            query = self.getAttributeString(c,'query','')
            friendlyName = self.getAttributeString(c,'friendlyName','')
            port = self.getAttributeInt(c,'port',0)
            # default is that the stream we are being told about is live
            live = self.getAttributeInt(c,'live',-1)
#            print (id,friendlyName,hostAddress,hostAlias,query,port,dataTable,live)
            startTime= self.getAttributeInt(c,'startTime',0)
            self.createStream(id,friendlyName,hostAddress,hostAlias,query,port,dataTable,live,startTime)

    # startTime is the first time to actually record anything - ie. time to go live
    # used so you can set a load of streams recording in sync
    def createStream(self,streamID,friendlyName,hostAddress,hostAlias,query,port,dataTable,live,startTime):
        dataTable=self.removeNonAlphabetCharacters(str(dataTable))
        # search for streams table for existing stream ID, or create that stream ID etc in table
        self.createStreamsTableIfNeeded()
        currentID=-1
        dbCur = self.dbConnection.cursor()
        if streamID==-1:
            if hostAlias=="" or port==0 or dataTable=="":
                print "Help: trying to create stream without the right parameters"
                sys.exit(-1)
            # find a stream with matching host alias, port and data table - friendly name is just for information, and host address is just the current connection address
            dbCur.execute("select _id from streams where hostAlias=? and port = ? and query= ? and dataTable=?",(hostAlias,port,query,dataTable))
            currentID=dbCur.fetchone()
            if currentID == None:
                if live==-1:
                    # default live to 1 for new streams
                    live = 1
                # this with means that it automatically commits the changes on success
                with self.dbConnection:
                    self.dbConnection.execute("insert into streams(hostAddress,hostAlias,port,query,friendlyName,dataTable,live) values(?,?,?,?,?,?,1)",(hostAddress,hostAlias,port,query,friendlyName,dataTable))
                dbCur.execute("select _id from streams where hostAlias=? and port = ? and query=? and dataTable=?",(hostAlias,port,query,dataTable))
                currentID= dbCur.fetchone()
                if currentID == None:
                    print "Help, couldn't insert new stream into streams table"
                    sys.exit(-1)
            currentID =int(currentID[0])
        else:
            # check that this stream exists
            #print streamID
            dbCur.execute("select _id,hostAddress,dataTable,live,query,hostAlias,port,friendlyName from streams where _id=%d"%(streamID))
            dbRow=dbCur.fetchone()
            if dbRow == None:
                print "Help: trying to alter a stream (id=%d) which doesn't exist"%currentID
                sys.exit(-1)
            currentID=dbRow[0]
            # if we haven't posted a host address, then add this one
            if len(hostAddress)==0:
                hostAddress=dbRow[1]
            #data table forces to the same table
            dataTable=dbRow[2]
            #  liveness is read from the table if it isn't set explicitly
            if live==-1:
                live=int(dbRow[3])
            #read in the query always - don't allow query changes without making a new stream
            query=dbRow[4]
            #read in the host alias always - don't allow query changes without making a new stream
            hostAlias=dbRow[5]
            #read in the port  always - don't allow port  changes without making a new stream
            port=int(dbRow[6])
            if len(friendlyName)==0:
                friendlyName=dbRow[7]

        #print "ID:",currentID

        # update the stream host address, friendly name and liveness in the streams table in case they have changed and commit
        with self.dbConnection:
            #print (friendlyName,hostAddress,hostAlias,port,query,dataTable,live)
            if len(friendlyName)>0:
                self.dbConnection.execute("update streams set friendlyName=? where _id=?",(friendlyName,currentID))
            if len(hostAddress)>0:
                self.dbConnection.execute("update streams set hostAddress=? where _id=?",(hostAddress,currentID))
            if live!=-1:
                self.dbConnection.execute("update streams set live=%d where _id=%d"%(live,currentID))

#            self.dbConnection.execute("update streams set friendlyName=?,hostAddress=?,live=? where _id=?",(friendlyName,hostAddress,live,currentID))
        self.createdataTableIfNeeded(dataTable)
        # open connection reader to ip address, port
        if len(query)>0:
            hostAddress=query+'@'+hostAddress
        if live==1:
            # see if a receiver already exists
            alreadyReading=False
            for c in range(0,len(self.inStreams)):            
                (rec,fri,tab,id,oldTime)=self.inStreams[c]
                if id==currentID:
                    alreadyReading=True
                    #set a new start time
                    self.inStreams[c]=(rec,fri,tab,id,startTime)
            if not alreadyReading:
                receiver=DataReceiver(hostAddress,port, self,(dataTable,currentID,startTime))
                self.inStreams.append( (receiver,friendlyName,dataTable,currentID,startTime) )
                receiver.start()
                print "Opened reader for %s at %s[%s]:%d"%(friendlyName,hostAlias,hostAddress,port)
            else:
                print "Already reading %s at %s[%s]:%d"%(friendlyName,hostAlias,hostAddress,port)
        else:
            foundPos=-1
            foundReceiver=None
            for c in range(0,len(self.inStreams)):
                (rec,fri,tab,id,oldStartTime) =self.inStreams[c]
                if id==currentID:
                    foundPos=c
                    foundReceiver=rec
            if foundPos!=-1:
                self.inStreams.pop(foundPos)
                foundReceiver.stopReceiving()
                #print "Stream for %s at %s[%s]:%d stopped - set non-live"%(friendlyName,hostAlias,hostAddress,port)
            #else:
            #    print "Stream for %s at %s[%s]:%d non-live"%(friendlyName,hostAlias,hostAddress,port)
        
    def setData(self,dataLine,streamInfo,timeFn=time.clock):
        (dataTable,streamID,startTime)=streamInfo
        # timestamp is integer unix time in millionths of a second
        # we need to get this in a funny way - using clock as well as time in order to get sub ms accuracy on windows (possibly on unix too)
        timestamp = timeFn() - self.initialClock + self.initialTimestamp
        timestamp = int(timestamp*1000000.0)
        if timestamp>=startTime:
            self.dbWrites.append((dataTable,streamID,timestamp,dataLine))
            self.dbEvent.set()
        
    def addDBRead(self,sql,callback):
        self.dbReads.append((sql,callback))
        self.dbEvent.set()
        

def main(dbname,port):
    store = DataStore(dbname,port)
    store.run()

if __name__ == '__main__': 
#    import yappi
#    yappi.start()
    main(sys.argv[1],int(sys.argv[2]))
#    yappi.print_stats()
