import sys, time, threading
import socket,select

from collections import deque


# basic data sender - just listens on one port and sends the data out on all things connected to that port
#
# there is also a ChildDataSender and a SenderParent which allows multiple different datasenders to live on one port - connections are passed to the correct sender when the first packet sent to the port is decoded

class DataSender(threading.Thread):
  
    def __init__(self,port,parent):
        threading.Thread.__init__(self)
        self.host='' # localhost
        self.port = port
        print "Sender: %d"%self.port
        self.initialiseListenSocket()
        self.connections=[]
        self.alive = True
        self.parent = parent
        self.event = threading.Condition()
        self.push   =  False
        self.doCloseAfterPush=False
        self.pushQueue=deque([],1000)
        # we need to lock access to the connection list, as connections are given to us by the parent
        # in child senders, and closed if back channel read throws an exception
        self.connectionLock=threading.Lock()

    def initialiseListenSocket(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind((self.host, self.port))
        self.s.listen(100)
        
    def setPush(self,push):
        self.push=push
        
    def stopSending(self):
        with self.event:
            self.alive = False;        
            self.event.notifyAll()
        
    def startSending(self):
        self.alive = True;
        
    def dataReady(self):
        with self.event:
            self.event.notifyAll()
        
    def pushData(self,data):
        with self.event:
            self.pushQueue.append(data)
            self.event.notifyAll()

    def pushMultipleData(self,data):
        with self.event:
            self.pushQueue.extend(data)
            self.event.notifyAll()
            
            
    def closeAfterPush(self):
        with self.event:
            self.doCloseAfterPush=True
            self.event.notifyAll()
        
    def isConnected(self):
        if len(self.connections)>0:
            return True
        return False
    
    def tryListenForConnection(self):
        # if we're already servicing connections we need this not to block, otherwise wait for first connection
#        print "listen"
        if len(self.connections)>0:
            self.s.setblocking(0)
        else:
            self.s.settimeout(1.0)                
        counter=0
        try:
            conn, addr = self.s.accept()
            conn.setblocking(0)
            conn.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
            with self.connectionLock:
                self.connections.append((conn,addr))
            print "connect"
        except socket.error, msg:
            # no new connection
            None
    
    # read a single line of back channel data
    # nb. this is quite inefficient - so don't send tons of data
    # over the back channel 
    def readBackChannelData(self,timeout=0):
        with self.connectionLock:
            socketList=zip(*self.connections)[0]
        curSocket=None
        try:
            inputReady,outputReady,exceptReady = select.select(socketList,[],socketList,timeout)
            if len(inputReady)>0:
                curSocket=inputReady[0]
                retVal=""
                while True:
                    char=inputReady[0].recv(1)
                    if char=='\n':
                        break
                    retVal+=char    
                return (retVal,inputReady[0])
        except socket.error, msg:
            if curSocket!=None:
                with self.connectionLock:
                    liveConnections=[]
                    for (c,a) in self.connections:
                        if c!=curSocket:
                            liveConnections.append((c,a))
                    self.connections=liveConnections
                curSocket.close()
            # probably closed - set the event so we become disconnected
        with self.event:
            self.event.notifyAll()
        return (None,None)
    
    def run(self):
        lastConnectTime=time.clock()-1.0
        counter=0
        while(self.alive == True):
            counter=counter+1
            curTime=time.clock()
            if curTime-lastConnectTime>1.0:
                self.tryListenForConnection()
                lastConnectTime=curTime
            if len(self.connections)>0:
                if not self.push:
                    data = self.parent.getData(self.port)
                else:
                    if len(self.pushQueue)>0:
                        data = self.pushQueue.popleft()
                    else:
                        data=None
                        if self.doCloseAfterPush:
                            break
                liveConnections=[]
                if data!=None:
                    with self.connectionLock:
                        for (c,a) in self.connections:
                            try:
                                inputready,outputready,exceptready = select.select([],[c.fileno()],[])
                                c.send(data+'\n')
                                liveConnections.append((c,a))
                            except socket.error:
                                c.close()
                        self.connections=liveConnections
                else:
                    with self.event:
                        if len(self.pushQueue)==0:
                            self.event.wait()
        for (c,a) in self.connections:
            c.send("***DISCONNECT***\n")
            c.shutdown(socket.SHUT_RDWR)
            c.close()
