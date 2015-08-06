import serial
import time
import sys

MODE_DUMP=0
MODE_LIVE=1
MODE_REPLAY=2
MODE_FAKE=3

curMode=MODE_LIVE

replayFile=None
for arg in sys.argv[1:]:
    if arg=='-d':
        curMode=MODE_DUMP
    elif arg=='-r':
        curMode=MODE_REPLAY
    elif arg=='-f':
        curMode=MODE_FAKE
    else:
        replayFile=arg
        
def showPacketValues(packet):
    #    print "packet[%d]"%len(packet),
    #    for val in packet:
    #        print "%02x"%val,
    #    print ""
        packetLen = packet[0]*2+6
        if len(packet)!=packetLen:
            return
    #    for c in range(0,packetLen):
    #        print "%02x"%packet[c],
    #    print ""
        if len(packet)>=40:
    #'       print "%x %x"%(packet[0],packet[1]),
            # print out 10 raw channels
            values=[0,0,0,0,0,0,0,0,0,0,0]
            for c in range(0,10):
               values[c] = packet[c*3+2] + (packet[c*3+3]<<8) + (packet[c*3+4]<<16)
    #           print "%06d"%(values[c]>>8),
#            print z"%06d"%(values[0]>>8),",FRAME",
            print "%06d"%(values[0]),",FRAME,",
            if len(packet)>=64:
                # packets come in at 128 samples per second, but ECG/EKG inputs sample at 2048 sps.
                # This means there are 15 in between values for ECG / EKG samples 
                # they start at position 32 and appear to be 30 bytes long in total
                for d in range(0,15):
                    val = (packet[32+d]<<8)+packet[32+d+15]
                    if val>32767:
                        val-=65536
                    print "%d,"%val,
#                for d in range(0,30):
#                    print "%d,"%packet[32+d],
#                    offset=packet[32+d*2] + (packet[32+1+d*2]<<8)
                    # signed
#                    if offset>32767: 
#                        offset-=65536
#                    print "%06d"%(offset)
            print ""
            # sub packet accuracy for ECG/EKG
#                    print "%06d"%((offset)>>8)
    #                values[0]+=offset
    #                print "%4x"%((values[0]+offset)>>8)
    #                print "%4x"%(values[0]>>8)                
                
    #    else:
    #        for c in packet:
    #            print "%02x"%c,
    #        print ""
 
 
def makeStringFromHex(hexString):
    str=""
    for c in hexString.split(" "):
        try:
            num=int("0x%s"%c,16)
            str+=chr(num)
        except ValueError:
            None
    return str

def makeValuesFromHex(hexString):
    valueList=[]
    for c in hexString.split(" "):
        try:
            num=int("0x%s"%c,16)
            valueList.append(num)
        except ValueError:
            None    
    return valueList

def writePortBytes(bytes,display=True):
    realString=bytearray(bytes)
    if display and curMode!=MODE_DUMP:
        print "Write: %d"%len(realString),bytes
    port.write(realString)

    
def writePort(hexString,display=True):
    realString=makeStringFromHex(hexString)
    if display and curMode!=MODE_DUMP:
        print "Write: %d"%len(realString),hexString
    port.write(realString)

# returns a response to a packet    
def fakeResponse(packet):
#    print "In:",packet
    packetLen = packet[0]*2+4
    if len(packet)!=packetLen:
        print "bad packet length received",packet,packetLen
        return ""
    # start receiving data
    if packet[1]==0x05:
        print "start receive",packet
        return "aa aa 10 02 0e 00 04 00 01 00 00 08 73 12 51 37 00 00 08 00 22 08 12 08 8e 00 8a 00 0e 00 00 08 ff ff ff ff 0f e8"
    elif packet[1]==0x22:
        # channel setup?
        # channel values in bytes 2 and 3
        print "channel setup: ",packet
        channelNum=(packet[2]<<8)+packet[3]
        channelResponses={
        0x0000: "aa aa 82 23 00 00 80 00 09 00 d3 00 73 12 51 37 a0 03 a5 00 0e 00 06 00 09 00 5d 00 ab 00 c9 70 7d 3f 51 37 d7 c6 5d 00 ad 00 30 c9 7d 3f 81 17 56 c7 5d 00 af 00 6c 66 7d 3f d1 17 86 c7 5d 00 b1 00 60 e4 7d 3f 9d c6 03 c7 66 00 b3 00 02 60 80 3f 0a a8 1c c4 66 00 b5 00 17 5f 80 3f 95 d5 84 c4 66 00 b7 00 22 67 80 3f b2 15 b3 c4 66 00 b9 00 e2 64 80 3f 5c b3 d2 c4 6f 00 bb 00 00 00 80 3f 00 00 00 00 78 00 bf 00 00 00 80 3f 00 00 00 00 81 00 c3 00 00 00 80 3f 00 00 00 00 8a 00 c7 00 00 00 80 3f 00 00 00 00 93 00 cc 00 00 00 80 3f 00 00 00 00 9c 00 d0 00 00 00 80 3f 00 00 00 00 09 00 02 00 00 00 18 01 6c 21 48 3c 00 00 00 00 01 fa 09 00 03 00 00 00 18 01 01 00 7a 39 00 00 00 00 01 fd 09 00 0b 00 14 00 08 00 00 00 80 3f 00 00 00 00 02 00 09 00 0b 00 15 00 08 00 00 00 80 3f 00 00 00 00 7c 96" ,
        0x8000: "aa aa 55 23 80 00 53 00 00 00 09 00 0b 00 16 00 08 00 00 00 80 3f 00 00 00 00 03 00 09 00 0b 00 17 00 08 00 00 00 80 3f 00 00 00 00 00 00 09 00 04 00 00 00 08 00 00 00 80 3f 00 00 00 00 00 00 09 00 0a 00 00 00 08 00 00 00 80 3f 00 00 00 00 00 00 06 00 4e 65 58 75 73 2d 31 30 00 00 02 00 41 00 02 00 42 00 02 00 43 00 02 00 44 00 02 00 45 00 02 00 46 00 02 00 47 00 02 00 48 00 04 00 53 61 4f 32 00 00 04 00 50 6c 65 74 68 00 04 00 48 52 61 74 65 00 05 00 53 74 61 74 75 73 00 00 04 00 44 69 67 69 00 00 03 00 53 61 77 00 8f 2b" ,
        0x0010: "aa aa 03 23 00 10 01 00 00 00 52 22", 
        0x0110: "aa aa 03 23 01 10 01 00 00 00 51 22" 
        }
        return channelResponses[channelNum]
    elif packet[1]==0x27:
        # keep alive - do nothing
        return ""
    elif packet[1]==0x30:
        # mystery packet - do nothing
        print "Mystery: ",packet
        return ""
    elif packet[1]==0x02:
        print "Packet 2",packet
        # some other packet?
        return "AA AA 02 00 03 02 00 00 51 53"
        
    else:
        print "Unknown packet type: %x"%packet[1]
        
if curMode!=MODE_REPLAY and curMode!=MODE_FAKE:
    port = serial.Serial("COM5",115200,timeout=1)    
#    port = serial.Serial("COM5",115200,timeout=1)    
   
    #    start reading
    writePort("AA AA 00 05 56 50")
      
      
    #port setup
    writePort("AA AA 02 22 00 00 80 00 D4 32")
    writePort("AA AA 02 22 80 00 53 00 81 32")
    writePort("AA AA 02 22 00 10 80 00 D4 22")
    writePort("AA AA 02 22 01 10 80 00 D3 22 	")
    writePort("AA AA 02 22 00 00 80 00 D4 32 ")
    writePort("AA AA 02 22 80 00 53 00 81 32 ")
    writePort("AA AA 02 22 00 10 80 00 D4 22 ")
    writePort("AA AA 02 22 01 10 80 00 D3 22 	")

    #go
    writePort("AA AA 03 02 0E 00 00 00 00 00 45 53")

    #stay alive
    writePort("AA AA 00 27 56 2E",False)
    packetBuffer=[]      
    singlePacket=[]
    foundPacket=0


    starttime=time.clock()
    numpackets=0

    stayalivelast=time.clock()        
    while time.clock()-starttime<10:
        #    stay alive
        if (time.clock()-stayalivelast)>0.5:
            writePort("AA AA 00 27 56 2E",False)
            stayalivelast=time.clock()        
        
    #    print "Stay alive"
        data=port.read(64)
        packetBuffer=map(ord,data)
        for c in packetBuffer:
            if c==0xaa:
                foundPacket+=1
            else:
                if foundPacket>=2:
                    if curMode==MODE_DUMP:
                        for val in singlePacket:
                            print "%02x"%val,
                        print ""
                    else:
                        # interpret live data
                        showPacketValues(singlePacket)
                    numpackets+=1
                    singlePacket=[]
                foundPacket=0
            singlePacket.append(c)

    if curMode!=MODE_DUMP:
        print "Messages per second", numpackets/(time.clock()-starttime)
            
    writePort("AA AA 03 02 0E 00 00 00 01 00 44 53")
else:
    # replay
    port=None
    if curMode==MODE_FAKE:
        port = serial.Serial("COM4",115200,timeout=10)
        print "Opened port COM4 - start biotrace now"
        started=False
        lastByte=0
        while not started:
            byte=port.read(1)
            if len(byte)>0:
                if ord(byte)==0xaa and lastByte==0xaa:
                    bytes=[ord(port.read(1))]
                    packetLen=bytes[0]*2
                    packetData=port.read(packetLen+3)
                    bytes.extend(map(ord,packetData))
#                    print "packet",packetLen,":",
#                    for val in bytes: 
#                        print "%02x"%val,
#                    print ""
                    writePort(fakeResponse(bytes))
                    if packetLen==0 and bytes[1]==0x05:
                        print "Got nexus start"
                        started=True
                    lastByte=0
                else:
                    lastByte=ord(byte)
    finished=False
    skipLines=0
    curTime=time.clock()
    while not finished:
        file = open(replayFile,"r")
        for line in file:
            packet=makeValuesFromHex(line)
            # second time round we skip the initialisation stuff
            if skipLines>0:
                skipLines-=1
                continue
#            showPacketValues(packet)
            if curMode==MODE_FAKE and port!=None:
                if port.inWaiting()>=6:
                    inBuf=port.read(port.inWaiting())
                    bytes=map(ord,inBuf)
#                    for val in bytes: 
#                        print "%02x"%val,
                    writePort(fakeResponse(bytes[2:]),display=False)
#                if len(packet)>64:
#                    for c in range(32,32+15):
#                        packet[c]=0xff
                writePortBytes(packet,display=False)
                time.sleep(0.01)
        if curMode!=MODE_FAKE:
            finished=True
        else:
            skipLines=10



