from DataReceiver import DataReceiver
from collections import deque
import sys

class Consumer():
    
    class ConsumerException(Exception):
        def __init__(self, msg):
            self.msg = msg        
    
    def __init__(self, host,port, parent,runOnlyOnce=False):
        self.host =host
        self.port = port
        self.inqueue = deque([],10000)    
        self.parent = parent
        self.datareceiver = DataReceiver(self.host,self.port, self,None)
        if runOnlyOnce:
            self.stopAfterNextConnection()
        self.datareceiver.start()
        self.hasShutdown=False

#    def __del__(self): 
#        print 'consumer ', self.host, 'died'
        
        
    def shutdownNow(self):
        self.hasShutdown=True
        self.datareceiver.stopReceiving()
        self.parent = None
        self.inqueue=None

        
    def stopAfterNextConnection(self):
        self.datareceiver.shutdownAfterConnection(True)
        
    def waitData(self,timeout=None):
        while len(self.inqueue)==0:
            self.datareceiver.waitData(timeout)
            if self.hasShutdown:
                return
            if len(self.inqueue)==0 and self.datareceiver.receiving==False:
                # only raise an exception once all data has been eaten up
                raise Consumer.ConsumerException("Receiver stopped")
        
        
    def getData(self):
        if(len(self.inqueue) > 0):
            return self.inqueue.popleft()
        else:
            return None
            
    def getLatest(self):
        if(len(self.inqueue) > 0):
            return self.inqueue[-1]
        else:
            return None
                
    def setData(self,data,callbackData):     
        if not self.hasShutdown:
            self.inqueue.append(data)
            if len(self.inqueue)>9990:
                #print "Consumer overflow",self.host,self.port
                pass
                
    def getConnectionCount(self):
        return self.datareceiver.getConnectionCount()
            
    def backchannelSendData(self,data):
        self.datareceiver.backchannelSendData(data)
            

