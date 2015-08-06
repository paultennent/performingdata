import sys, threading
import socket

class DataReceiver(threading.Thread):
    
    def __init__(self,host,port,parent,parentData):
        threading.Thread.__init__(self)
        self.port = port
        self.host = host
        self.parent = parent
        self.queryString=None
        atPoint=self.host.find('@')
        if atPoint!= -1:
            self.queryString=self.host[0:atPoint]
            self.host=self.host[atPoint+1:]
            #print "Query:%self.s Host:%self.s"%(self.queryString,self.host)
        # some data that is passed to the parent in the callback
        self.parentData= parentData
        self.receiving = True                
        self.event = threading.Event()   
        self.oneConnectionOnly=False
        self.s=None
        self.connected=False
        self.connectionCount=0

    def waitData(self,timeout=None):
        if self.receiving==False:
            return
        self.event.wait()
        self.event.clear()
        
    def stopReceiving(self):
        self.receiving = False
        self.event.set()
    
    def getConnectionCount(self):
        return self.connectionCount
        
    def isConnected(self):
        return (self.connected)
        
    def startReceiving(self):
        self.receiving = True

    def shutdownAfterConnection(self,value):
        self.oneConnectionOnly=True

    #send data back to the sender - nb: this is ONLY intended for control / sync etc. messages
    #that alter what the sender is doing to create the stream coming in,
    #not for sending data streams in two directions. If you want to do that
    #then use a pair of sender/receivers 
    def backchannelSendData(self,value):
        if self.s!=None and self.connected==True:
            try:
                self.s.send(value+"\n")
            except socket.error,msg:
                print "Couldn't send backchannel data"
        
    def run(self):
        self.s=None
        lineBuffer=""
        callback=self.parent.setData
        parentData=self.parentData
        event=self.event
        while (self.receiving):
            try:
                if self.s==None:
                    self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.s.connect((self.host, self.port))
                    # 60 second timeout in case of dropped connections
                    self.s.settimeout(60)
                    if self.queryString!=None:
                        self.s.send(self.queryString+'\n')
                        gotLine=False
                        response=""
                        while gotLine==False:
                            response+=self.s.recv(1)
                            if len(response)>0 and response[-1]=='\n':
                                gotLine=True
                        if response!="QUERYGOOD\n":
                            self.s.close()
                            self.s=None
                            continue
                    self.connectionCount+=1
                    self.connected=True
                data = self.s.recv(4096)
                if not data: break
                if len(data)==0:
                    print "no data"
                if lineBuffer!=None:
                    data=lineBuffer+data
                    lineBuffer=None
                dataSplit=data.split('\n')
                if data[-1]!='\n':
                    lineBuffer=dataSplit[-1]
                # whatever happens we want to knock the end one off now, as it is an empty string if last char is \n
                for line in dataSplit[0:-1]:
                    # need manual disconnect due to threading weirdies
                    if line=="***DISCONNECT***":
                        self.s.close()
                        self.s=None
                        # this is a setting to make it stop retrying if it gets one successful connection which was then disconnected (for running queries)
                        if self.oneConnectionOnly:
                            self.stopReceiving();
                            return
                    else:
                        callback(line,parentData)
                event.set()
            except socket.error,msg:
                self.connected=False
                self.s.close()
                self.s=None
        if self.s!=None:
            self.s.close()
        self.parent=None
        self.parentData=None
