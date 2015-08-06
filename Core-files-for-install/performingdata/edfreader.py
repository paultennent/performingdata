from ctypes import *

import os,time,re,calendar

import struct

from DataStoreConnection import *

class  EDFReader:
    # file types this supports
    EDF_FILE=1 # standard EDF File
    EDA_FILE=2 # EDA file as saved by affectiva Q sensors

    class EDAGlobalHeader():
        def __init__(self,file):
            while True:
                line=file.readline()
                if line[0]=='-':
                    #end of header
                    break
                lineSplitPos=line.find(":")
                if lineSplitPos>=0:
                    lineType=line[0:lineSplitPos]
                    lineValue=line[lineSplitPos+1:]
                    if lineType=='UUID':
                        self.patientID=lineValue
                    elif lineType=='Sampling Rate':
                        self.samplingRate=float(lineValue)
                    elif lineType=='Start Time':
                        offsetPos=lineValue.find("Offset:")
                        if offsetPos>0:
                            self.startTime=calendar.timegm(time.strptime(lineValue[0:offsetPos],' %Y-%m-%d %H:%M:%S '))
                            self.hoursOffset=int(lineValue[offsetPos+7:])
                            self.startTime-=self.hoursOffset*60*60
                        else:
                            self.startTime=calendar.timegm(time.strptime(lineValue,' %Y-%m-%d %H:%M:%S '))
                            self.hoursOffset=0
            self.numSignals=6            
            fileLen=os.fstat(file.fileno()).st_size
            curPos=file.tell()
            self.recordCount=(fileLen-curPos)/(14)
            
    
        # note: offsetFromDST is ignored for EDA files
        def getStartTime(self,offsetFromDST=None):
            return self.startTime

        def getRecordDuration(self):
            return 1.0/self.samplingRate            
            
        def getAttributeDict(self):
            retVal={}
            fields=["numSignals","recordCount","patientID","samplingRate","startTime","hoursOffset"]
            for c in fields:
                retVal[c]=str(getattr(self,c)).strip()
            return retVal

    class EDASignalHeader:
        def __init__(self,label,physicalDimension,resolution):
            self.label=label
            self.resolution=resolution
            self.physicalDimension=physicalDimension            
                
        def getAttributeDict(self):
            retVal={}
            copyAttrs=["label","resolution","physicalDimension"]
            for c in copyAttrs:
                retVal[c]=str(getattr(self,c)).strip()
            return retVal

            
    class EDFGlobalHeader(Structure):   
        _pack_=1
        _fields_ = [ ('version',8*c_char),('patientID',80*c_char),('recordID',80*c_char),('startDate',8*c_char),('startTime',8*c_char),('headerSize',8*c_char),('reserved',44*c_char),('recordCount',8*c_char),('recordDuration',8*c_char),('numSignals',4*c_char) ]
        
        def getStartTime(self,offsetFromDST=None):
            # get start date from the startDate field
            dateMatch=re.match("(\d\d).(\d\d).(\d\d)",self.startDate)
            (y,mon,d)=("1990","1","1")
            if dateMatch!=None:
                (d,mon,y)=dateMatch.group(1,2,3)
            # but if it is EDF plus, then also get the year from the recordID, as this has a 4 digit year
            longYearMatch=re.match("Startdate \d\d-\w\w\w-(\d\d\d\d)",self.recordID)
            if longYearMatch!=None:
                y=int(longYearMatch.group(1))
            else:
                # guess year from whether it is less than 85 or not as per spec
                if int(y)<85:
                    y = "%d"%int(y)+2000
                elif int(y)>=85 and int(y)<100:
                    y = "%d"%int(y)+1990
            (h,min,s) =("00","00","00")
            timeMatch=re.match("(\d\d).(\d\d).(\d\d)",self.startTime)
            if timeMatch!=None:
                (h,min,s) =timeMatch.group(1,2,3)
            timeInSeconds=calendar.timegm(time.strptime("%s:%s:%s %s:%s:%s"%(y,mon,d,h,min,s),"%Y:%m:%d %H:%M:%S"))
            if offsetFromDST!=None:
                timeInSeconds+=float(offsetFromDST)*3600.0
            return timeInSeconds

        def getRecordDuration(self):
            return float(self.recordDuration)
            
        def getAttributeDict(self):
            retVal={}
            for c,typeofc in self._fields_:
                retVal[c]=getattr(self,c).strip()
            return retVal

    class EDFSignalHeader:
        def __init__(self,allSignalHeaderStruct,signalNum):
            self._copyAttrs_=["label","transducerType","physicalDimension","physicalMinimum","physicalMaximum","digitalMinimum","digitalMaximum","prefiltering","samplesPerRecord"]
            for c in self._copyAttrs_:
                setattr(self,c,getattr(allSignalHeaderStruct,c+"%d"%signalNum))
                
        def getAttributeDict(self):
            retVal={}
            for c in self._copyAttrs_:
                retVal[c]=getattr(self,c).strip()
            return retVal
                
    def __init__(self,filename,fileType=EDF_FILE):
        self.fileType=fileType
        self.filename=filename
        self.file = open(filename,"rb")
        if self.fileType==EDFReader.EDA_FILE:
            self.globalHeader=EDFReader.EDAGlobalHeader(self.file)
            self.numSignals=int(self.globalHeader.numSignals)
            self.signalHeaders=[]
            self.signalHeaders.append(EDFReader.EDASignalHeader("Z-axis","g",0.01))
            self.signalHeaders.append(EDFReader.EDASignalHeader("Y-axis","g",0.01))
            self.signalHeaders.append(EDFReader.EDASignalHeader("X-axis","g",0.01))
            self.signalHeaders.append(EDFReader.EDASignalHeader("Battery","v",0.01))
            self.signalHeaders.append(EDFReader.EDASignalHeader("Temperature","c",0.1))
            self.signalHeaders.append(EDFReader.EDASignalHeader("EDA","uS",0.001))
        else:
            self.globalHeader=EDFReader.EDFGlobalHeader()
            self.file.readinto(self.globalHeader)
           # print "\nEDF Header\n---------"
           # self.dumpStruct(self.globalHeader)
           # print "Start time:",self.globalHeader.getStartTime()
            self.numSignals=0
            self.signalHeaders=[]
            try:
                self.numSignals=int(self.globalHeader.numSignals)
            except ValueError:
                if headerSize>0:
                    self.numSignals=headerSize/256
                else:
                    print "Couldn't get number of signals"
                    return
            allSignalHeaders=self.makeEDFSignalHeaderStruct()
            self.file.readinto(allSignalHeaders)
            self.samplesForAllSignals=0
            for c in range(self.numSignals):
                self.signalHeaders.append(EDFReader.EDFSignalHeader(allSignalHeaders,c))
                self.samplesForAllSignals+=int(self.signalHeaders[-1].samplesPerRecord)
            #for (c,s) in enumerate(self.signalHeaders):
            #    print "\nsignal %d\n---------"%c
            #    self.dumpStruct(s)
         
    # build a signal header struct for the right number of signals
    def makeEDFSignalHeaderStruct(self):
        numSignals=self.numSignals
        new_class=type('EDFSignalHeaders%d'%numSignals,(Structure,),{})
        new_class._pack_=1
        # make fields
        fieldList=[('label',(16*c_char)),('transducerType',(80*c_char)),('physicalDimension',(8*c_char)),('physicalMinimum',(8*c_char)),('physicalMaximum',(8*c_char)),('digitalMinimum',(8*c_char)),('digitalMaximum',(8*c_char)),('prefiltering',(80*c_char)),('samplesPerRecord',(8*c_char)),('reserved',(32*c_char))]
        fullFieldList=[]
        for (fieldName,fieldType) in fieldList:
            for d in range(self.numSignals):                
                fullFieldList.append((fieldName+"%d"%d,fieldType))
        new_class._fields_=fullFieldList
        return new_class()
    
    def dumpStruct(self,obj):
        for attr in dir(obj):
            if attr[0]!='_':
                val=getattr(obj,attr)
                if type(val)==str:
                    print attr,":",val.strip()
                    
    def getEDFHeaderInfo(self):
        return self.globalHeader.getAttributeDict()

    def getEDFStreamInfo(self,streamNum):
        if streamNum<len(self.signalHeaders):        
            return self.signalHeaders[streamNum].getAttributeDict()
        else:
            return None
            
    def decodeEDAVal(self,packet,number):
        valSign=1+(-2*(ord(packet[number*2])>>7))
        valRaw=ord(packet[number*2+1])+(ord(packet[number*2])&0x0f)*256
        val=valSign*valRaw
        return val

    def checkForSessionName(self,sessionName,dbConnection):
        sesList = dbConnection.listSessions()
        for sesId,sesName in sesList:
            if(sessionName == sesName):
                return int(sesId)
        return None 
            
            
    def readEDFSignalIntoDBConnection(self,dbConnection,streamsSelected,sessionName,timeOffset=0):
        timeStart=self.globalHeader.getStartTime(-timeOffset)
        timePerRecord=self.globalHeader.getRecordDuration()
        streamNames=[]
        for signalNum in streamsSelected:
            if signalNum>=0 and signalNum<len(self.signalHeaders):
                info=self.signalHeaders[signalNum]
                streamNames.append(info.label.strip())
            else:
                print "Bad signal number: %d, stopping import"%signalNum
                return False
        sesID = self.checkForSessionName(sessionName,dbConnection)
        if sesID != None:
            importSession,importStreamList=dbConnection.startSessionImporting(timeStart,streamNames,existingSessionID=sesID)
        else:
            importSession,importStreamList=dbConnection.startSessionImporting(timeStart,streamNames,sessionName=sessionName)
        if importSession==None:
            return False
        importStreamDetails={}
        if self.fileType==EDFReader.EDA_FILE:
            # Affectiva Qsensor EDA data format
            for signalNum,streamID in zip(streamsSelected,importStreamList):
                signal=self.signalHeaders[signalNum]
                multiplier=signal.resolution
                importStreamDetails[signalNum]=(signal,signalNum,int(streamID),multiplier)
            for (signal,signalNum,streamID,multiplier) in importStreamDetails.values():
                # copy EDF stream information into database
                dbConnection.setStreamAttributes(streamID,signal.getAttributeDict())
            numRecords=int(self.globalHeader.recordCount)
            curRecords=0
            totalSamples=0
            # convert the data into edf format and then whack it across to the db all in one go
            streamDataBuffers={}
            for (signal,signalNum,streamID,multiplier) in importStreamDetails.values():
                streamDataBuffers[signalNum]=bytearray()
            while curRecords<numRecords:
                packet=self.file.read(14)
                if len(packet)<14:
                    #unexpected end of file
                    break
                for (signal,signalNum,streamID,multiplier) in importStreamDetails.values():
                    val=self.decodeEDAVal(packet,signalNum)                    
                    streamDataBuffers[signalNum].extend(struct.pack('<h',val))
                    totalSamples+=1
                curRecords+=1
            for (signal,signalNum,streamID,multiplier) in importStreamDetails.values():
                if dbConnection.write16BitImportData(streamID,streamDataBuffers[signalNum],timeStart,timePerRecord,preOffset=0.0,scaling=multiplier,postOffset=0.0)==False:
                    print "Error writing import data - stopping"
                    dbConnection.stopSessionImporting(importSession)
                    return False
                
        else:
            # standard EDF format
            for signalNum,streamID in zip(streamsSelected,importStreamList):
                signal=self.signalHeaders[signalNum]
                preOffset= -float(signal.digitalMinimum)
                scaling = (float(signal.physicalMaximum)-float(signal.physicalMinimum)) / (float(signal.digitalMaximum)-float(signal.digitalMinimum))
                postOffset= float(signal.physicalMinimum)
                print "preOffset,scaling,postOffset:",preOffset,",",scaling,",",postOffset
                importStreamDetails[signalNum]=(signal,int(streamID),preOffset,scaling,postOffset)
            for (signal,streamID,preOffset,scaling,postOffset) in importStreamDetails.values():
                # copy EDF stream information into database
                dbConnection.setStreamAttributes(streamID,signal.getAttributeDict())
            numRecords=int(self.globalHeader.recordCount)
            if numRecords==-1:
                fileLen=os.fstat(self.file.fileno()).st_size
                curPos=self.file.tell()
                numRecords=(fileLen-curPos)/(2*self.samplesForAllSignals)
                print "Estimate number of records:",numRecords
            recordDuration=float(self.globalHeader.recordDuration)
            curRecords=0
            totalSamples=0
            while curRecords<numRecords:
                for (signalNum,signal) in enumerate(self.signalHeaders):
                    # read one frame from this stream
                    numSamples=int(signal.samplesPerRecord)
                    dataBuf=(c_int16*numSamples)()
                    self.file.readinto(dataBuf)
                    timePerSample=float(timePerRecord)/float(numSamples)
                    # now write it out into the database connection
                    # the DB has direct support for 16 bit integer data and scaling of data afterwards
                    # so we don't need to do anything cunning until it gets into the db itself
                    if importStreamDetails.has_key(signalNum):
                        (signal,streamID,preOffset,scaling,postOffset)=importStreamDetails[signalNum]
                        if dbConnection.write16BitImportData(streamID,dataBuf,timeStart,timePerSample,preOffset=preOffset,scaling=scaling,postOffset=postOffset)==False:
                            print "Error writing import data - stopping"
                            dbConnection.stopSessionImporting(importSession)
                            return False
                        totalSamples+=numSamples
                curRecords+=1
                timeStart+=timePerRecord
        print "Written: %d records = %d data points"%(curRecords,totalSamples)
        # stop the session importing (doesn't really do very much, except let 
        # the db connection know it can drop any import connections it has held open
        dbConnection.stopSessionImporting(importSession)
        return True

if __name__ == '__main__':     
    db=DataStoreConnection("127.0.0.1",49990)
    while db.checkConnection()==False:
        print "Can't connect to database"
        time.sleep(1.0)
    r=EDFReader("test.eda",EDFReader.EDA_FILE)
    print r.getEDFHeaderInfo()
    print r.getEDFStreamInfo(0)
    print r.getEDFStreamInfo(1)
    print r.getEDFStreamInfo(2)
    print r.getEDFStreamInfo(3)
    if not r.readEDFSignalIntoDBConnection(db,[5],"Woo"):
        print "Failed to read edf into DB"
#    if not r.readEDFSignalIntoDBConnection(db,[0,1,2,3,4,5],"Woo"):
#        print "Failed to read edf into DB"
    