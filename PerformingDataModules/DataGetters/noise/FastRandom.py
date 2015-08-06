import sys, random,time
import socket

# very fast demo collector source - use this for things like testing
# database writing etc. works up to about 30000 sends per second (use something other than python for faster stuff maybe!)
# 
# 
class FastCollector():

    def removeConnection(self,connection):
        liveConnections=[]
        for (conn,addr) in self.connections:
            if conn!=connection:
                liveConnections.append((conn,addr))
        self.connections=liveConnections
        print "%d connections"%len(self.connections)
    
    def run(self,port,frequency):
        self.port = port
        
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind(('', self.port))
        self.s.listen(100)

        self.connections=[]
        
        timeStart = time.clock()
        timePerSample = 4.0/float(frequency)
        numSamples=0
        nextTime=timeStart+timePerSample

        lastConnectTime=time.clock()-5.0

        curConnection=None
        #while True:
        #    try:
        #        self.s.setblocking(1)
        #        conn,addr= self.s.accept()
        #        while True:
        #            time.sleep(0)
        #            conn.send("0\n")
        #    except socket.error, msg:
        #        None
            
        while True:
            try:    
                while True:
                    for (curConnection,addr) in self.connections:
                        conn.send("0\n1\n0\n1\n")
                    curConnection=None    
                    curTime=time.clock()
                    if curTime<nextTime:
                        time.sleep(nextTime-curTime)
                    elif curTime-nextTime>5.0:
                        print "Overrun"
                        nextTime=curTime+timePerSample
                    nextTime+=timePerSample

                    # only try connect 1 per second once we have one connection
                    if curTime-lastConnectTime>1.0:
                        if len(self.connections)>0:
                            self.s.setblocking(0)
                            lastConnectTime=curTime
                        else:
                            self.s.setblocking(1)
                        conn, addr = self.s.accept()
                        conn.setblocking(1)
                        conn.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
                        self.connections.append((conn,addr))
                        curTime=time.clock()
                        nextTime=curTime+timePerSample
                        print "%d connections"%len(self.connections)
            except socket.error, msg:
                if curConnection!=None:
                    self.removeConnection(curConnection)
            
    
        
        
def main(port,frequency):
    c = FastCollector()
    c.run(port,frequency)
    
if __name__ == '__main__': main(int(sys.argv[1]),int(sys.argv[2]))
