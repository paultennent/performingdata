from collections import deque
import threading
import time
import random
# use newest pysqlite2
import pysqlite2.dbapi2 as sqlite3

import itertools,operator

class ReadDBConnection(threading.Thread):
    def __init__(self,dbName,callback):
        threading.Thread.__init__(self)
        self.statements=[]
        self.callback=callback
        self.dbName=dbName
        self.event=threading.Event()
        self.start()
        
    def addDBRead(self,statement,woo):
        self.statements.append(statement)
        self.event.set()
            
    def run(self):
        self.dbConnection=sqlite3.connect(self.dbName)
        # don't sync writes to disk (we don't really care as we're only reading stuff anyway)
        self.dbConnection.execute('PRAGMA synchronous=OFF')
        while(True):
            if len(self.statements)>0:
                statement=self.statements.pop()
                cursor=self.dbConnection.execute(statement)
                self.callback.onDBRead(cursor,statement)
            else:
                self.event.wait()
                self.event.clear()
            
        
    

class DataStoreReader(threading.Thread):
    
    def __init__(self,dbName,tableName,streamID,minTimestamp,maxTimestamp,timeDelay,streaming,sender,callback,useCopyTable=False,useConnectionThread=True):
        threading.Thread.__init__(self)
        self.streaming=streaming
        self.timeDelay =timeDelay
        self.maxTimestamp=maxTimestamp
        self.minTimestamp=minTimestamp
        self.callback=callback
        self.tableName=tableName
        self.copyTableName=None
        self.streamID=streamID
        self.sender=sender
        self.useCopyTable=useCopyTable
        self.useConnectionThread=useConnectionThread
        if useConnectionThread:
                self.readDBThread=ReadDBConnection(dbName,self)
                self.callback=self.readDBThread
        self.lastTimestamp= None
        self.outQueue=deque()
        self.dbReadPending=False
        self.sender.setPush(True)
        self.finished=False
        if self.timeDelay!=-1:
            nowTimestamp= int(time.time() * 1000000.0)
            delayedTimestamp = nowTimestamp - self.timeDelay
            self.minTimestamp=max(delayedTimestamp,self.minTimestamp)
        
    def isFinished(self):
        return self.finished
        
    # called when a read request has been done - don't take too long here, or else it will slow database writing down
    def onDBRead(self,cursor,sql):
        if sql[:6]=="create" or sql[:6]=="insert":
            print sql
            return
        finished=self.finished
        rowCount=0
        if not self.finished:
            # if we are streaming this should be a set of value / timestamp pairs
            for row in cursor:
                rowCount+=1
                timestamp=row[0]
                value=row[1]
                if self.maxTimestamp==-1 or timestamp<self.maxTimestamp:
                    self.outQueue.append((timestamp,value))
                else:
                    #stop sending if we've reached maximum timestamp
                    finished=True
 #                   print "finished"
                if timestamp>self.lastTimestamp:
                    self.lastTimestamp=timestamp
            if rowCount==0:
#                print "no rows"
                finished=True
        self.finished=finished
        self.dbReadPending=False

        
    # loop round reading stuff from the database and outputting things at the right time
    def run(self):
        connected=False
        # this only allows one connection, and then quits and kills the connection
        while self.finished!=True and (connected==False or self.sender.isConnected()==True): 
            if self.sender.isConnected()==True:
                connected=True
            if len(self.outQueue)>0:
                # send stuff
                nextDataPoint = self.outQueue.popleft()
                if self.streaming:
                    # if we are streaming, wait until the right time to send stuff out
                    nowTimestamp= int(time.time() * 1000000.0)                    
                    dataPointTime=nextDataPoint[0]
                    if self.timeDelay == -1:
                        self.timeDelay = 1000000+nowTimestamp-dataPointTime
                    delayedTimestamp = nowTimestamp - self.timeDelay

 #                   print "sleeping"
                    if dataPointTime>delayedTimestamp:
                        # sleep until the right time
                        sleepTime=dataPointTime-delayedTimestamp
                        sleepTime = sleepTime * (1.0/1000000.0)
                        time.sleep(sleepTime)
                    
                    if (delayedTimestamp-dataPointTime)>1000000.0:
                        # dump the point until we catch up
                        self.lastTimestamp = max(self.lastTimestamp,delayedTimestamp)
                    else:
#                print "point %s"%nextDataPoint[1]
                        self.sender.pushData(nextDataPoint[1])
                # check if we are about to send something > max timestamp, at which point we close the connection?
            if self.dbReadPending==False and len(self.outQueue)<256:
                # add database reads to get some more data into the stream queue
                # work out timestamp to fetch from
                if self.lastTimestamp==None:
                    self.lastTimestamp=self.minTimestamp
                if self.useCopyTable:
                    if self.copyTableName==None:
                        # use a temporary table
                        sql="create temp table if not exists temp_%d (timestamp integer primary key,value text)"%self.streamID
                        self.callback.addDBRead(sql,self)
                        sql="insert or fail into temp_%d select timestamp,value from %s where stream_id=%d"%(self.streamID,self.tableName,self.streamID)
                        self.callback.addDBRead(sql,self)
                        self.copyTableName="temp_%d"%self.streamID
                    #                print "send more",sql
                    sql="select timestamp,value from %s where timestamp>%d order by timestamp asc limit 256"%(self.copyTableName,self.lastTimestamp)
                    self.dbReadPending=True
                    self.callback.addDBRead(sql,self)
                else:
#                print "send more",sql
                    sql="select timestamp,value from %s where timestamp>%d and stream_id=%d order by timestamp,stream_id asc limit 256"%(self.tableName,self.lastTimestamp,self.streamID)
                    self.dbReadPending=True
                    self.callback.addDBRead(sql,self)
        self.finished=True
        self.sender.stopSending()

# This is a datastore that reads from a particular stream, but doesn't actually send anything until it receives commands
class DataStoreCommandReader(threading.Thread):
    
    def __init__(self,dbName,tableName,streamID,sender):
        threading.Thread.__init__(self)
        self.dbName=dbName
        self.tableName=tableName
        self.streamID=streamID
        self.sender=sender

    def debugPrintSQL(self,sqlTuple):
        parsed=sqlTuple[0]
        for c in sqlTuple[1]:
            pos=parsed.find('?')
            if pos!=-1:
                parsed=parsed[0:pos]+str(c)+parsed[pos+1:]
        print parsed
        
    def processCommand(self,command):
        #print command
        rowCount=0
    
    
        # commands are like:
        # OFFSET timeFrom numPoints
        # SEEK timeFrom timeTo maxValuesBefore readAhead
        # SEEK (go to absolute time <offset>) returning the values in between (with a max of <maxValues>),
        # and then readAhead values after
        # nb: if seeking will return >maxValues points, it only returns the ones closest to the final point
        # OFFSET (go <offset> data points in the database, returning them all) 
        commandSplit=command.upper().split(' ')
        commandType=commandSplit[0]
        startTime=float(commandSplit[1])
        startTime=int(startTime*1000000.0)
        # JM note: this code assumes we only have one value per timestamp, or
        # don't care about multiple values at a timestamp
        # I thought this was probably better than assuming that the database
        # times were always in the right order (by using the database rowid)
        # particularly given timestamps are microsecond accurate
        rowCount=0
        timeStart=time.clock()
        if commandSplit[0]=='OFFSET':                        
            offsetPoints=int(commandSplit[2])
            if offsetPoints>0:
                sql=("select substr(timestamp,0,length(timestamp)-5)||'.'||substr(timestamp,-6)||','||value from %s where timestamp>? and stream_id=? order by timestamp asc limit ?"%(self.tableName),(startTime,self.streamID,offsetPoints))
            else:
                sql=("select substr(timestamp,0,length(timestamp)-5)||'.'||substr(timestamp,-6)||','||value from %s where timestamp<? and stream_id=? order by timestamp desc limit ?"%(self.tableName),(startTime,self.streamID,-offsetPoints))
#                        cursor.execute(*sql)
            cursor=self.dbConnection.execute(*sql)
            values=cursor.fetchall()
            cursor.close()
            self.sender.pushMultipleData(zip(*values)[0])
            rowCount+=len(values)
        elif commandSplit[0]=='SEEK':
            newPos=float(commandSplit[2])
            newPos=int(newPos*1000000.0)
            maxValuesBefore=int(commandSplit[3])
            maxValuesAfter=int(commandSplit[4])
            if newPos<startTime:
                # seek backwards
                # the nested select is because we want to return them in reverse time order,
                # but only the last maxPoints
                sql1=("select substr(timestamp,0,length(timestamp)-5)||'.'||substr(timestamp,-6)||','||value from (select timestamp,value from %s where timestamp>=? and timestamp<? and stream_id=? order by timestamp asc limit ?) order by timestamp desc"%(self.tableName),(newPos,startTime,self.streamID,maxValuesBefore))
                sql2=("select substr(timestamp,0,length(timestamp)-5)||'.'||substr(timestamp,-6)||','||value from %s where timestamp<? and stream_id=? order by timestamp desc limit ?"%(self.tableName),(newPos,self.streamID,maxValuesAfter))
            else:
                # seek forwards
                # the nested select is because we want to return them in time order,
                # but only the last maxPoints
#                sql1=("select substr(timestamp,0,length(timestamp)-5)||'.'||substr(timestamp,-6)||','||value from (select timestamp,value from %s where timestamp>? and timestamp<=? and stream_id=? order by timestamp desc limit ?) order by timestamp asc"%(self.tableName),(startTime,newPos,self.streamID,maxValuesBefore))
                sql1=("select timestamp,value from %s where timestamp>? and timestamp<=? and stream_id=? order by timestamp desc limit ?"%(self.tableName),(startTime,newPos,self.streamID,maxValuesBefore))
                
#                sql2=("select substr(timestamp,0,length(timestamp)-5)||'.'||substr(timestamp,-6)||','||value from %s where timestamp>? and stream_id=? order by timestamp asc limit ?"%(self.tableName),(newPos,self.streamID,maxValuesAfter))
                sql2=("select timestamp,value from %s where timestamp>? and stream_id=? order by timestamp,stream_id asc limit ?"%(self.tableName),(newPos,self.streamID,maxValuesAfter))
#            self.debugPrintSQL(sql1)
#            self.debugPrintSQL(sql2)
            cursor=self.dbConnection.execute(*sql1)
            values=cursor.fetchall()
            cursor.close()
            rowCount+=len(values)
            self.sender.pushMultipleData(zip(*values)[0])
            cursor=self.dbConnection.execute(*sql2)
            values=cursor.fetchall()
            cursor.close()
            self.sender.pushMultipleData(zip(*values)[0])
            rowCount+=len(values)
        else:
            print "Unknown command %s"%commandSplit[0]                    
        # tell the receiver that this is the end of a response
        self.sender.pushData("END,END")
        timeEnd=time.clock()
        if timeEnd-timeStart>1.0:
            print "Long command: %s"%(command)
        print "end",command
        print "rows:",rowCount,timeEnd-timeStart
        return timeEnd-timeStart
        
    def run(self):
        print "Cmd Reader"
        self.dbConnection=sqlite3.connect(self.dbName,isolation_level=None)
        # don't sync writes to disk (we don't really care as we're only reading stuff anyway)
        self.dbConnection.execute('PRAGMA synchronous=OFF')

#        cursor=self.dbConnection.cursor()
        connected=False
        # this only allows one connection, and then quits and kills the connection
        while (connected==False or self.sender.isConnected()==True):
            if self.sender.isConnected()==True:
                connected=True
                (command,source)=self.sender.readBackChannelData(10.0)
                if command!=None:
                
                    self.processCommand(command)
                        
            else:
                time.sleep(0.1)
        print "DataStoreReader closed"

if __name__ == '__main__':  
    class FakeSender:
        def __init__(self):
            self.backChannelData=deque()
            self.event=threading.Event()
            
        def isConnected(self):
            return True
            
        def addBackChannel(self,data):
            self.backChannelData.append(data)
            self.event.set()
            
        def readBackChannelData(self,timeout):
            if len(self.backChannelData)==0:
                self.event.wait(timeout)
            self.event.clear()
            if len(self.backChannelData)>0:
                return (self.backChannelData.popleft(),("127.0.0.1"))
                
        def pushMultipleData(self,values):
            None
        def pushData(self,values):
            None

    fs=FakeSender()
            
  #  cr=DataStoreCommandReader("leerosy.sqlite3",'SAVE_SESSION_201111231200',9,fs)
  #  cr.dbConnection=sqlite3.connect(cr.dbName,isolation_level=None)


  #  totalTime=0
  #  for c in range(0,100):
  #      searchTime=1322051250.581997+random.uniform(0,50.0)
  #      totalTime+=cr.processCommand("SEEK %f %f 1280 1280"%(searchTime,searchTime+100))
  #  print totalTime
    
    import apsw
    con=apsw.Connection("leerosy.sqlite3",flags=apsw.SQLITE_OPEN_READONLY,statementcachesize=100)
    
    cursor=con.cursor()
    cursor.execute("PRAGMA synchronous=OFF")
    totalTime=0
    for c in range(0,100):
        rowCount=0
        timeStart=time.clock()
#        cursor.execute("select timestamp,cast(value as real) from SAVE_SESSION_201111231200 where timestamp>1322051293621305 and timestamp<=1322051393621305 and stream_id=9 order by timestamp desc limit 1280")
        cursor.execute("select timestamp,cast(value as real) from SAVE_SESSION_201111231200 limit 1280")
        values=cursor.fetchall() 
#        fs.pushMultipleData(zip(*values)[0])
        rowCount+=len(values)
        cursor.execute("select timestamp,cast(value as real) from SAVE_SESSION_201111231200 limit 1280")
#        cursor.execute("select timestamp,cast(value as real) from SAVE_SESSION_201111231200 where timestamp>1322051393621305 and stream_id=9 order by timestamp,stream_id asc limit 1280")
#        print type(values[0][1])
        values=cursor.fetchall() 
#        fs.pushMultipleData(zip(*values)[0])
        rowCount+=len(values)
        timeEnd=time.clock()
        print rowCount,timeEnd-timeStart
        totalTime+=timeEnd-timeStart
    print totalTime