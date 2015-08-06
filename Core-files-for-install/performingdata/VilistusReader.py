from collections import deque
import socket
import time

class VilistusReader():

    def sendAndWaitResponse(self,strData):
        print "Send: ",strData
        self.s.send(strData+"\r\n")
#        print self.s.recv(1024).replace("\r","\n")

    def __init__(self,vilistusHost,vilistusPort):
        self.host=vilistusHost
        self.port=vilistusPort
        self.s=None
        self.connected=0
        self.sync=0
        self.inQueue=deque([],1000)
        self.pollString="RING\n"

    def connect(self):
        # connect to vilistus if it isn't
        if not self.isConnected():
            self.s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            try:
                self.s.connect((self.host,self.port))
                print "connected - wait for response"
                print self.s.recv(8)
                time.sleep(0.1)
                self.s.send("$$$")
                time.sleep(0.5)
                #print self.s.recv(1024)
                print "Param set"

                self.sendAndWaitResponse("set broadcast interval 0") # turn off udp broadcast
                self.sendAndWaitResponse("set com time 1000") # set maximum wait between buffers
                self.sendAndWaitResponse("set com size 700") # set buffer size
#                self.sendAndWaitResponse("set ip flags 7") #  TCP retries
                self.sendAndWaitResponse("set ip flags 3") #  TCP retries off
                time.sleep(0.5)
                self.sendAndWaitResponse("exit")        
                time.sleep(0.5)
                print "poll"
                self.s.send(self.pollString)
                print "poll"
                self.s.send(self.pollString)
                print "connected okay"
                self.s.settimeout(5.0)
                self.sync=0
                self.connected=1
            except socket.error,e:
                self.connected=0
                self.s.close()
                self.s=None

    def isConnected(self):
            # if we're connected:
            if self.s == None or self.connected == 0:
                return False
            else:
                return True
        
    def getData(self):
        try:
            # see if we have some data buffered already
            # sync up if we have dropped data
            haveBuffer=False
            buffer=[]
            while not haveBuffer:
                while len(self.inQueue)>0 and self.sync==0:
                    self.sync=ord(self.inQueue.popleft())&0x80
                    #print "resync"
                if len(self.inQueue)>14:
                    for c in range(0,14):
                        buffer.append(self.inQueue.popleft())
                    self.sync=ord(buffer[-1])&0x80
                    haveBuffer=True
                else:
                    self.inQueue.extend(self.s.recv(700))
            # split the data up
            dataReturn = [0,0,0,0,0,0,0,0]
            for c in range(0,8):
                if (c&1) == 0:
                    # even channel
                    lowBits=ord(buffer[(c / 2)*3 +2])
                    highBits=(ord(buffer[(c / 2)*3 +2+2])<<3)&0b1110000000
                    dataReturn[c]= lowBits|highBits
                else:
                    # odd channel
                    lowBits=ord(buffer[(c / 2)*3 +2 + 1])
                    highBits=(ord(buffer[(c / 2)*3 +2+2])<<7)&0b1110000000
                    dataReturn[c]= lowBits|highBits
            return dataReturn
        except socket.error,e:
            print "Vilistus Connection lost",e
            self.s.close()
            self.s=None
            self.connected=0
            return None
    
