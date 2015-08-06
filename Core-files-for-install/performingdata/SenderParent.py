import sys, time, threading
import socket

from collections import deque

from ChildDataSender import ChildDataSender

class SenderParent(threading.Thread):


    def __init__(self,port,parent):
        threading.Thread.__init__(self)
        self.host='' # localhost
        self.port = port
        self.parent=parent
        print "Sender Parent: %d"%self.port
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind((self.host, self.port))
        self.s.listen(100)

    class ConnectionHandlerThread(threading.Thread):
        def __init__(self,socket,addr,parent):
            threading.Thread.__init__(self)
            self.conn=socket
            self.parent=parent
            self.addr=addr
            self.start()
            
        def run(self):
            try:
                self.conn.setblocking(1)
                self.conn.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
                self.conn.settimeout(10.0)
                # now read from this port until we reach a newline
                # or 10 seconds passes
                initStr=""
                bufFinished=False
                #print "Sender parent reading query"
                while not bufFinished:
                    newData=self.conn.recv(256)
                    if len(newData)==0:
                        # connection closed
                        self.conn.close()
                        self.conn=None
                        bufFinished=True
                    else:
                        initStr+=newData
                        if initStr[-1]=='\n':
                            bufFinished=True
                #remove the \n
                if self.conn!=None:
                    initStr=initStr[0:-1]
    #                    print "Sender parent needs datasender for stream %s"%initStr
                    stream = self.parent.getMultiStream(initStr)
                    # we have to do a handshake here (the QUERYGOOD and QUERYERROR messages, because for some reason on windows, this early close of the connection
                    # doesn't get detected by the receiver - goodness knows why! - if I move it to before the query string is read, it closes fine!
                    if stream!=None: 
                        self.conn.send("QUERYGOOD\n")
                        stream.onNewConnection(self.conn,self.addr)
                        #print "Connection done to multistream"
                    else:
                        print "Bad query string - close connection"
                        self.conn.send("QUERYERROR\n")
                        self.conn.close()
            except socket.error, msg:
                print "error :",msg
                # no new connection
                None
        
        
    def run(self):
        self.s.setblocking(1)
        while True:
            try:
                conn, addr = self.s.accept()
                thd=SenderParent.ConnectionHandlerThread(conn,addr ,self.parent)
            except socket.error, msg:
                print "error :",msg
                # no new connection
                None
        