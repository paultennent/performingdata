from performingdata.ChildDataSender import ChildDataSender
from collections import deque
import sys, random,time
from headset import Headset
from performingdata.SenderParent import *

from threading import Lock

# Collector for EMOTIV headset - needs Emotiv Control Panel running (this runs on port 3008)
# command line options <emotiv IP> <emotiv port> <out port> <headset number>
# eg. usually 127.0.0.1 3008 <portnum> 0

# Channels:
# Cognitive
# 0:Type
# 1:Intensity
# Affectiv:
# 2:Engaged/Bored
# 3:Excitement
# 4:Excitement Long Term
# 5:Meditation
# 6:Frustration
# Expressiv:
# 7:WINK_LEFT
# 8:WINK_RIGHT
# 9:BLINK
# 10:LEFT_LID
# 11:RIGHT_LID
# 12:HORIEYE
# 13:VERTEYE
# 14:SMILE
# 15:CLENCH
# JM:the below are not yet implemented but very easy if needed
# 16:LAUGH
# 17:SMIRK_LEFT
# 18:SMIRK_RIGHT
# 19:FURROW
# 20:EYEBROW

# 100-125: raw EEG etc. channels  (100 + value below)
#   ED_COUNTER = 0
#   ED_INTERPOLATED=1
#   ED_RAW_CQ=2
#   ED_AF3=3
#   ED_F7=4
#   ED_F3=5
#   ED_FC5=6
#   ED_T7=7 
#   ED_P7=8
#   ED_O1=9
#   ED_O2=10
#   ED_P8=11
#   ED_T8=12
#   ED_FC6=13
#   ED_F4=14
#   ED_F8=15
#   ED_AF4=16
#   ED_GYROX=17    
#   ED_GYROY=18
#   ED_TIMESTAMP=19
#   ED_ES_TIMESTAMP=20
#   ED_FUNC_ID=21
#   ED_FUNC_VALUE=22
#   ED_MARKER=23    
#   ED_SYNC_SIGNAL=24


# Values For Cognitive Suite:
# Neutral = 0
# Push = 1
# Pull = 2
# Lift = 3
# Drop = 4
# Left = 5
# Right = 6
# Rotate Left = 7
# Rotate Right = 8
# Rotate Clockwise = 9
# Roatate Anticlockwise = 10
# Rotate Forwards = 11
# Rotate Backwards = 12
# Disappear = 13

class EmotivDirectCollector():

    # userNum is the index of the headset to use
    def run(self,emotivHost,emotivPort,portBase,userNum):        
        self.senderLock=Lock()
        self.portBase = portBase
        self.channelCount=15
        self.outQueues = {}
        self.dataSenders={}
        self.newSenders={}
        self.headset=Headset(emotivHost,emotivPort,userNum)
        self.senderParent=SenderParent(portBase,self)
        self.senderParent.start()

        lastTime=time.clock()
        while(True):
            # arbitrary polling interval - 128hz
            curTime=time.clock()
            sleepTime=0.0078125-( curTime-lastTime)
            if sleepTime>0:
                time.sleep(0.0078125-( curTime-lastTime))
            
            lastTime=curTime

            self.headset.handleEvents()
# debug code to display all channel values            
#            values=[]
#            for c in range(0,15):
#                values.append(self.getHeadsetChannel(c))
#            print values
            # read the raw data from the headset
            self.headset.readRawData()
            # get the data from the channels we are listening to from the headset
            # and put the data into the out queues
            self.senderLock.acquire()
            for (channel,ds) in self.dataSenders.iteritems():
                if ds.isConnected():
                    if channel<100:
                        # emotion detection etc.
                        value=self.getHeadsetChannel(channel)
                        self.outQueues[channel].append(value)
                    elif channel<200:
                        # raw EEG/accelerometer etc. channels 100-125
                        if self.headset.hasState:
                            values=self.headset.getRawChannel(channel-100)
                            for c in values:
                                self.outQueues[channel].append("%f"%c)
                    elif channel<300:
                        # signal quality / connection quality
                        raise "need to implement signal quality channels"
            # trigger data senders for each out queue - trigger all at once so data comes nicely in sync
            for ds in self.dataSenders.itervalues():
                if ds.isConnected():
                    ds.dataReady()
            self.senderLock.release()

    # called by datasenders to get the data to be sent
    def getData(self,port):
        if self.outQueues.has_key(port) and len(self.outQueues[port])>0:
            return self.outQueues[port].popleft()
        else:
            return None
            
    def getMultiStream(self,queryString):
#        print "connect multistream %s"%queryString
        retVal=None
        self.senderLock.acquire()
        try:
            channelNum=int(queryString)
            if channelNum<self.channelCount and channelNum>=0:
                if not self.dataSenders.has_key( channelNum):
                    self.dataSenders[channelNum]=ChildDataSender(channelNum,self)
                    self.outQueues[channelNum]=deque([],1000)
                    self.dataSenders[channelNum].start()
                retVal= self.dataSenders[channelNum]
            elif channelNum>=100 and channelNum<125:
                # raw EEG / accelerometer data
                if not self.dataSenders.has_key( channelNum):
                    self.dataSenders[channelNum]=ChildDataSender(channelNum,self)
                    self.outQueues[channelNum]=deque([],1000)
                    self.dataSenders[channelNum].start()
                retVal= self.dataSenders[channelNum]
            elif channelNum>=200 and channelNum<216:
                # sensor signal quality
                raise "Implement sensor signal quality channels"
                retVal=None
        except ValueError,v:
            retVal=None # drop out
        if retVal==None:
            print "bad query string for Emotiv collector:",queryString
        self.senderLock.release()
        return retVal
     
    def getHeadsetChannel(self,channel):
        if not self.headset.hasState:
            return "0"
# Cognitive
# 0:Cog Type
        if channel==0:
            return "%d"%self.headset.getCognitivAction()
# 1:Cog Intensity
        elif channel==1:
            return "%f"%self.headset.getCognitivActionIntensity()
# Affectiv:
# 2:Engaged/Bored
# 3:Excitement
# 4:Excitement Long Term
# 5:Meditation
# 6:Frustration
        elif channel==2:
            return "%f"%self.headset.getAffectivEngagedBored()            
        elif channel==3:
            return "%f"%self.headset.getAffectivExcitement()            
        elif channel==4:
            return "%f"%self.headset.getAffectivExcitementLongTerm()            
        elif channel==5:
            return "%f"%self.headset.getAffectivMeditation()            
        elif channel==6:
            return "%f"%self.headset.getAffectivFrustration()            
# Expressiv:
# 7:WINK_LEFT
# 8:WINK_RIGHT
# 9:BLINK
# 10:LEFT_LID
# 11:RIGHT_LID
# 12:HORIEYE
# 13:VERTEYE
# 14:SMILE
# 15:CLENCH
# 16:LAUGH
# 17:SMIRK_LEFT
# 18:SMIRK_RIGHT
# 19:FURROW
# 20:EYEBROW
        elif channel==7:
            return "%d"%self.headset.getExpressivWinkLeft()            
        elif channel==8:
            return "%d"%self.headset.getExpressivWinkRight()            
        elif channel==9:
            return "%d"%self.headset.getExpressivBlink()            
        elif channel==10:
            return "%f"%self.headset.getExpressivEyelids()[0]            
        elif channel==11:
            return "%f"%self.headset.getExpressivEyelids()[1] 
        elif channel==12:
            return "%f"%self.headset.getExpressivEyeDir()[0] 
        elif channel==13:
            return "%f"%self.headset.getExpressivEyeDir()[1] 
        elif channel==14:
            return "%f"%self.headset.getExpressivSmile()
        elif channel==15:
            return "%f"%self.headset.getExpressivClench()
        elif channel==16:
            raise "Error not implemented yet"
        elif channel==17:
            raise "Error not implemented yet"
        elif channel==18:
            raise "Error not implemented yet"
        elif channel==19:
            raise "Error not implemented yet"
        elif channel==20:
            raise "Error not implemented yet"
     
    
def main(EmotivHost,EmotivPort,portBase,userNum):
    c = EmotivDirectCollector()
    c.run(EmotivHost,EmotivPort,portBase,userNum)
    
if __name__ == '__main__': 
    if len(sys.argv)<5:
        print "EmotivDirectCollector emotivHost emotivPort port headsetNum\n"
        print "    If EmotivPort is -1, then it will connect directly to a headset"
        print "    otherwise, it will connect via emotiv control panel (usually on port 3008)"
        print "    Note: Raw EEG data only works if connecting directly\n"
    else:
        main(sys.argv[1],int(sys.argv[2]),int(sys.argv[3]),int(sys.argv[4]))
