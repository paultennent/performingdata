import sys, time, threading
import socket

from collections import deque

from DataSender import DataSender


# childdatasender - lives on one port with a sender parent
# basic data sender - just listens on one port and sends the data out on all things connected to that port
#
# there is also a ChildDataSender and a SenderParent which allows multiple different datasenders to live on one port - connections are passed to the correct sender when the first packet sent to the port is decoded

class ChildDataSender(DataSender):
  
    # parent here is the thing that sends the data, not the sender parent (which doesn't know about it's child streams)
    def __init__(self,port,dataparent):
        DataSender.__init__(self,port,dataparent)

        # we don't need to listen for a socket here
    def initialiseListenSocket(self):
        None

    def kill(self):
        self.alive = False

    def onNewConnection(self,connection,address):
        with self.connectionLock,self.event:
            self.connections.append((connection,address))
            self.event.notifyAll()
        
    # listening for connections is now done by the senderparent
    def tryListenForConnection(self):
        None

    def run(self):
        counter=0
        while(self.alive == True):
            counter=counter+1
            if len(self.connections)>0:
                if not self.push:
                    data = self.parent.getData(self.port)
                else:
                    if len(self.pushQueue)>0:
                        data = self.pushQueue.popleft()
#                        if data=="END,END":
#                            print "**send end**"
                    else:
                        data=None
                        if self.doCloseAfterPush:
                            break
                liveConnections=[]
                if data!=None:
                    with self.connectionLock:
                        for (c,a) in self.connections:
                            try:
                                c.send(data+'\n')
                                liveConnections.append((c,a))
                            except socket.error:
                                c.close()
                        self.connections=liveConnections
                else:
                    with self.event:
                        if len(self.pushQueue)==0:
                            self.event.wait()
            else:
                # wait until connected (add connection calls this)
                with self.event:
                    self.event.wait()
        for (c,a) in self.connections:
            c.send("***DISCONNECT***\n")
            c.close()
        print "sender closed",self.alive
