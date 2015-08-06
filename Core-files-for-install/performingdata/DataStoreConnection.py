# class to connect to a remote datastore


import os,socket,time

from collections import deque
import xml.dom.minidom 
import subprocess

class DataStoreConnection:
    # if dbFilename is not none, then we ignore host, and instead spawn a local database as a child process
    # if autoport==False, then we ignore port also and just use a free port between 1024 and 2048
    def __init__(self,host,port,dbFilename=None,autoPort=True):
        self.host=host
        self.port=port
        if dbFilename!=None:
            self.dbFilename=os.path.abspath(dbFilename)
        else:
            self.dbFilename=None
        self.autoPort=autoPort
        if self.dbFilename!=None:
            self.dbProcess=self.spawnLocalDBForFile()        
        else:
            self.dbProcess=None
        self.importSockets={}
        
    # run a local database to playback from a particular file, using an arbitrary port from 1025-2048
    # we don't use an completely random port, because we don't want to get in the way of ports used by 
    # vicarious stuff
    def spawnLocalDBForFile(self):
        self.host="127.0.0.1"
        # find a free port 
        if self.autoPort:
            sparePort=1024
            while True:
                try:
                    sparePort+=1
                    tempsocket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    tempsocket.bind(('',sparePort))
                    break
                except socket.error, msg:
                    tempsocket.close()
            tempsocket.close()
            print "Spare port:",sparePort
            self.port=sparePort
        else:
            sparePort=self.port
        thisFilePath =  os.path.dirname(__file__)
        spawnCommand=thisFilePath+os.sep+"fastdatastore.exe %s %d "%(self.dbFilename,sparePort)
        return subprocess.Popen(spawnCommand, shell=True)
        
        
    def __del__(self):
        self.kill()
        
    def kill(self):
        if self.dbProcess!=None:
            # kill the db process
            try:
                self.dbProcess.kill()
            except WindowsError,e:
                print "Fail to kill db:",e
        self.dbProcess=None

    def getDescription(self):
        if self.dbFilename!=None:            
            return "%s (local port:%d)"%(os.path.basename(self.dbFilename),self.port)
        else:
            return "%s:%d (remote)"%(self.host,self.port)
            
    def isLocal(self):
        if self.dbFilename!=None:
            return True
            
    def getAutoPort(self):
        return self.autoPort
            
    def getFilename(self):
        return self.dbFilename
            
    def getHost(self):
        return self.host
        
    def getPort(self):
        return self.port

    def checkConnection(self):
        sessions=self.listSessions()
        if sessions==None:
            return False
        else:
            return True
        
    def sendQuery(self,query,returnSocket=False):
#        print "query:",query
        allData=deque()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.settimeout(10)
            s.connect((self.host,self.port))
            s.send(query)
            response=""
            gotLine=False
            while gotLine==False and len(response)<20:
                response+=s.recv(1)
                if len(response)>0 and response[-1]=='\n':
                    gotLine=True
            if response!="QUERYGOOD\n":
                s.close()
                return None
            if returnSocket:
                return s
            else:
                endMarker='\n***DISCONNECT***\n'
                endMarkerPos=0
                done=False
                fullResponse=""
                while not done:
                    buf=s.recv(4096)
                    bufPos=0
                    for c in buf:
                        fullResponse+=c
                        if c==endMarker[endMarkerPos]:
                            endMarkerPos+=1
                            if endMarkerPos==len(endMarker):
                                done=True
                                fullResponse=fullResponse[0:-len(endMarker)]
                                break
                        else:
                            endMarkerPos=0
        except socket.error,msg:
            return None
        return fullResponse

            
    #returns list of sessionID,name tuples 
    # if streamID is not None, then only lists sessions which include this stream
    #or None in case of error
    def listSessions(self,streamID=None):
        if streamID!=None:
            results=self.sendQuery("<listsessions streamID='%d'/>"%streamID)
        else:
            results=self.sendQuery("<listsessions/>")
        if results==None:
            return None
        document=xml.dom.minidom.parseString(results)
        sessions=document.getElementsByTagName("session")
        retVal=[]
        for c in sessions:
            retVal.append((c.getAttribute("id"),c.getAttribute("name")))
        return retVal
        
    #returns list of dictionaries with stream IDs, names, tables etc. in session (or all streams if None)
    def listSessionStreams(self,session=None):
        if session==None:
            results=self.sendQuery("<liststreams/>")
        else:
            results=self.sendQuery("<liststreams sessionID='%d'/>"%session)        
        if results==None:
            return None
#        print results
        document=xml.dom.minidom.parseString(results)
        streams=document.getElementsByTagName("stream")
        retVal=[]
        for c in streams:
            dict={}
            for i in range(0,c.attributes.length):
                attrib=c.attributes.item(i)
                dict[attrib.name]=attrib.nodeValue
            retVal.append(dict)
        return retVal

        # get min/max times for a list of streams - streamPairs = tuples of (id,dataTable)
    def getStreamTimes(self,streams):
        query="<streaminfos>"
        for id in streams:
            query+="<streaminfo id='%d'/>"%id
        query+="</streaminfos>"    
        results=self.sendQuery(query)
        if results==None:
            return None
        document=xml.dom.minidom.parseString(results)
        streams=document.getElementsByTagName("streaminfo")
        retVal=[]
        for c in streams:
            retVal.append((c.getAttribute("id"),float(c.getAttribute("minTime")),float(c.getAttribute("maxTime"))))
        return retVal
        
    #returns session ID
    def createSession(self,name):
        results=self.sendQuery("<updatesession name='%s' />"%name)
        if results==None:
            return None        
        document=xml.dom.minidom.parseString(results)
        sessions=document.getElementsByTagName("session")
        if len(sessions)==0:
            return None
        return int(sessions[0].getAttribute("id"))
        
    # gets the attributes of a session    
    def getSessionAttributes(self,sessionID):
        results=self.sendQuery("<sessioninfo id='%d' />"%sessionID)
        if results==None:
            return None        
        document=xml.dom.minidom.parseString(results)
        sessions=document.getElementsByTagName("session")
        retVal={}
        if len(sessions)>0:
            name=sessions[0].getAttribute("name")
        else:
            return None
        params=document.getElementsByTagName("param")
        for par in params:
            text=""
            for t in par.childNodes:
                text+=t.nodeValue
            retVal[par.getAttribute("key")]=text
        retVal["name"]=name
        return retVal
        
        
    def setSessionAttributes(self,sessionID,attributes,newName=None):
        if newName!=None:
            query="<updatesession id='%d' name='%s'>\n"%(sessionID,newName)
        else:
            query="<updatesession id='%d'>"%(sessionID)
        for (key,value) in attributes.iteritems():
            query+="<param key='%s'>%s</param>\n"%(key,value)
        query+="</updatesession>\n"
        results=self.sendQuery(query)
        if results==None:
            return False
        return True
                

                
    # streamList is a list of dictionaries, with keys - which are basically unpacked to xml
    # and passed into a database recordstreams command
    #        <stream id='%d' hostAddress='%.256s' hostAlias='%.256s' query='%.512s' port='%d'
    #     friendlyName='%.512s' dataTable='%.512s' live='%d' />\n",
    # returns the same thing as liststreams, except only the ones that have been modified
    # (so you can get the stream IDs out here)
    # If a dictionary has an "attributes" sub-dictionary, then it will be unpacked as param key/values inside the stream
    # tag (written into the streamattributes table)
    def setRecordStreams(self,streamList,sessionID):
        #print streamList
        if sessionID!=None:
            query="<recordstreams sessionID='%d'>"%sessionID
        else:
            query="<recordstreams>"
        for stream in streamList:
            query+="<stream "
            for (key,value) in stream.iteritems():
                if key!="attributes":
                    query+="%s='%s' "%(str(key),str(value))
            if stream.has_key('attributes'):
                query+=">\n"
                for (key,value) in stream['attributes'].iteritems():
                    query+="<param key='%s'>%s</param>\n"%(key,value)
                query+="</stream>"
                #print query
            else:
                query+="/>\n"
        query+="</recordstreams>"
        #print query
        results=self.sendQuery(query)
        if results==None:
            return None
        #print results
        document=xml.dom.minidom.parseString(results)
        streams=document.getElementsByTagName("stream")
        retVal=[]
        for c in streams:
            dict={}
            for i in range(0,c.attributes.length):
                attrib=c.attributes.item(i)
                dict[attrib.name]=attrib.nodeValue
            retVal.append(dict)
        return retVal

    def setImportStreams(self,streamList,sessionID):
        return self.setRecordStreams(self,streamList,sessionID)

        
    def setStreamAttributes(self,streamID,attributes):
        return self.setRecordStreams([{"id":streamID,"attributes":attributes}],None)
        
    # the helper methods below are designed to be easier than calling setRecordStreams directly    
        
    # create a set of streams in a named session, and start them recording, returns session ID
    # streamList = tuples of (host ,port,friendly name,hostAlias) -
    # query is included in host if used, 
    # table name is auto-generated based on session name
    # all streams start recording at the same time (startTime)
    # startTime is set as the startTime attribute for the session
    def startSessionRecording(self,sessionName,startTime,streamList):
        # table is always time & date stamped
        tableName=time.strftime("SESSION_%Y%m%d_%H%M%S",time.gmtime(startTime))
        recordList=[]
        for (host,port,friendlyName,hostAlias) in streamList:
            if host.find("@")!=-1:
                hostSplit=host.split('@')
                host=hostSplit[1]
                query=hostSplit[0]
            else:
                query=""
            dict={}
            dict["hostAddress"]=host
            dict["query"]=query
            dict["port"]=port
            dict["friendlyName"]=friendlyName
            dict["hostAlias"]=hostAlias
            dict["dataTable"]=tableName
            if port>=0:
                dict["live"]=1
            else:
                # this is something like a video stream, where it doesn't
                # actually get recorded to the database, we just put a placeholder in
                # so that it can be retrieved
                dict["live"]=0
            dict["startTime"]=startTime
            recordList.append(dict)
        #print recordList
        sessionID=self.createSession(sessionName)
        self.setSessionAttributes(sessionID,{"startTime":str(startTime),"dataTable":str(tableName)})
        self.setRecordStreams(recordList,sessionID)
        return sessionID

    def startSessionImporting(self,startTime,streamNames,existingSessionID=None,sessionName=None):
        tableName=time.strftime("IMPORT_%Y%m%d_%H%M%S",time.gmtime(startTime))
        recordList=[]
        for name in streamNames:
            dict={}
            dict["hostAddress"]="0.0.0.0"
            dict["query"]=""
            dict["port"]="1"
            dict["friendlyName"]=name
            dict["hostAlias"]="import"
            dict["dataTable"]=tableName
            # never record to an import stream
            dict["live"]=0
            recordList.append(dict)
        if sessionName==None and existingSessionID==None:
            print "Must have either a new session name or an existing session ID"
            return False
        elif existingSessionID!=None:
            sessionID=existingSessionID
        else:        
            sessionID=self.createSession(sessionName)
        if sessionID==None:
            return None,[]
        self.setSessionAttributes(sessionID,{"startTime":str(startTime),"dataTable":str(tableName)})
        recordStreamList=[]
        recordStreams=self.setRecordStreams(recordList,sessionID)        
        for c in recordStreams:
            recordStreamList.append(c["id"])
        return sessionID,recordStreamList
        
    def getStreamInfos(self,streamIDList):
        # call set recordstreams without any attributes to not actually change the stream
        # but to return the info
        streamList=[]
        for c in streamIDList:
            streamList.append({"id":c})
        return self.setRecordStreams(streamList,None)
        
    # stops all streams in given session recording    
    def stopSessionRecording(self,sessionID,newName):
        streams=self.listSessionStreams(sessionID)
        streamList=[]
        for c in streams:
            streamList.append({"id":c["id"],"live":"0"})
        self.setRecordStreams(streamList,None)
        endTime=time.time()
        #print newName
        self.setSessionAttributes(sessionID,{"endTime":str(endTime)},newName)

    def stopSessionImporting(self,sessionID):
        None
        
        
    def getStreamReplayAddress(self,streamID):
        return ("<cursor id='%d'/>@%s"%(streamID,self.host),self.port)
        
    # write import data in 16 bit format as used by EDF (with scaling, offset etc.)    
    def write16BitImportData(self,streamID,dataBuf,timeStart,timePerSample,preOffset=0.0,scaling=1.0,postOffset=0.0):
        if self.importSockets.has_key(streamID):
            # we have an existing socket open, importing into this stream - try and send to it
            try:
                (sock,thisTimeStart,thisTimePerSample)=self.importSockets[streamID]            
                if timePerSample==thisTimePerSample and abs(thisTimeStart-timeStart)<0.1:
                    sock.sendall(dataBuf)
                    self.importSockets[streamID]=(sock,timeStart+timePerSample*len(dataBuf),timePerSample)
                    return True
            except socket.error,e:
                # couldn't send - try reconnect
                Pass
        print "New Socket"
        query="<importdata streamID='%d' timeStart='%f' timePerSample='%f' valuePreOffset='%f' valuePostOffset='%f' valueMultiplier='%f' type='int16_raw'/>"%(streamID,timeStart,timePerSample,preOffset,postOffset,scaling)
        try:
            sock=self.sendQuery(query,returnSocket=True)
            if sock!=None:
                sock.sendall(dataBuf)
                self.importSockets[streamID]=(sock,timeStart+timePerSample*len(dataBuf),timePerSample)
                return True
        except socket.error,e:
            Pass
        return False
            
            
    # write import data from timestamped list of tuples [(timestamp,dataString)]
    def writeTimestampedImportData(self,streamID,dataList):
        if self.importSockets.has_key(streamID):
            # we have an existing socket open, importing into this stream - try and send to it
            try:
                (sock,thisTimeStart,thisTimePerSample)=self.importSockets[streamID]            
                if -1==thisTimePerSample and -1==thisTimeStart:
                    for (time,data) in dataList:
                        str= "%f,%s\n"%(time,data)
                        sock.sendall(str)
                    return True
            except socket.error,e:
                # couldn't send - try reconnect
                Pass
        query="<importdata streamID='%d' type='timestamped' />"%(streamID)
        try:
            sock=self.sendQuery(query,returnSocket=True)
            if sock!=None:
                for (time,data) in dataList:
                    str= "%f,%s\n"%(time,data)
                    sock.sendall(str)
                self.importSockets[streamID]=(sock,-1,-1)
                return True
        except socket.error,e:
            Pass
        return False
        
            
if __name__ == '__main__': 
    # open a datastore connection to this host
    d=DataStoreConnection("127.0.0.1",1025)
    print d.getStreamTimes([632])
