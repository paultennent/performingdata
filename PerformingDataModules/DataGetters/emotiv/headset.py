from ctypes import *
import os

# python API for emotiv headset

class Headset():

    # error codes
    EDK_OK                                =0x0000
    #//! An internal error occurred
    EDK_UNKNOWN_ERROR                    =0x0001
    #//! Invalid Developer ID
    EDK_INVALID_DEV_ID_ERROR            =0x0002
    #//! The contents of the buffer supplied to EE_SetUserProfile aren't a valid, serialized EmoEngine profile.
    EDK_INVALID_PROFILE_ARCHIVE            =0x0101
    #//! Returned from EE_EmoEngineEventGetUserId if the event supplied contains a base profile (which isn't associated with specific user).
    EDK_NO_USER_FOR_BASEPROFILE            =0x0102

    #//! The EmoEngine is unable to acquire EEG data for processing.
    EDK_CANNOT_ACQUIRE_DATA                =0x0200

    #//! The buffer supplied to the function isn't large enough
    EDK_BUFFER_TOO_SMALL                =0x0300
    #//! A parameter supplied to the function is out of range
    EDK_OUT_OF_RANGE                    =0x0301
    #//! One of the parameters supplied to the function is invalid
    EDK_INVALID_PARAMETER                =0x0302
    #//! The parameter value is currently locked by a running detection and cannot be modified at this time.
    EDK_PARAMETER_LOCKED                =0x0303
    #//! The current training action is not in the list of expected training actions
    EDK_COG_INVALID_TRAINING_ACTION        =0x0304
    #//! The current training control is not in the list of expected training controls
    EDK_COG_INVALID_TRAINING_CONTROL    =0x0305
    #//! One of the field in the action bits vector is invalid
    EDK_COG_INVALID_ACTIVE_ACTION        =0x0306
    #//! The current action bits vector contains more action types than it is allowed
    EDK_COG_EXCESS_MAX_ACTIONS            =0x0307
    #//! A trained signature is not currently available for use - addition actions (including neutral) may be required
    EDK_EXP_NO_SIG_AVAILABLE            =0x0308
    #//! A filesystem error occurred that prevented the function from succeeding
    EDK_FILESYSTEM_ERROR                =0x0309

    #//! The user ID supplied to the function is invalid
    EDK_INVALID_USER_ID                    =0x0400

    #//! The EDK needs to be initialized via EE_EngineConnect or EE_EngineRemoteConnect
    EDK_EMOENGINE_UNINITIALIZED            =0x0500
    #//! The connection with a remote instance of the EmoEngine (made via EE_EngineRemoteConnect) has been lost
    EDK_EMOENGINE_DISCONNECTED            =0x0501
    #//! The API was unable to establish a connection with a remote instance of the EmoEngine.
    EDK_EMOENGINE_PROXY_ERROR            =0x0502

    #//! There are no new EmoEngine events at this time
    EDK_NO_EVENT                        =0x0600

    #//! The gyro is not calibrated. Ask the user to stay still for at least 0.5s
    EDK_GYRO_NOT_CALIBRATED                =0x0700 

    #//! Operation failure due to optimization
    EDK_OPTIMIZATION_IS_ON                =0x0800

    #//! Reserved return value
    EDK_RESERVED1                       =0x0900
    
    # core EDK functions (training, etc.)
    # the emotional state access functions are below

    #! Expressiv Suite threshold type enumerator
    #    typedef enum EE_ExpressivThreshold_enum {
    #        EXP_SENSITIVITY
    #    } EE_ExpressivThreshold_t;
    EE_ExpressivThreshold_t=c_uint
    EXP_SENSITIVITY=0
    
    #! Expressiv Suite training control enumerator
    #    typedef enum EE_ExpressivTrainingControl_enum {
    #        EXP_NONE = 0, EXP_START, EXP_ACCEPT, EXP_REJECT, EXP_ERASE, EXP_RESET
    #    } EE_ExpressivTrainingControl_t;
    EE_ExpressivTrainingControl_t=c_uint
    EXP_NONE = 0
    EXP_START=1
    EXP_ACCEPT=2
    EXP_REJECT=3
    EXP_ERASE=4
    EXP_RESET=5
    
    #! Expressiv Suite signature type enumerator
    #    typedef enum EE_ExpressivSignature_enum {
    #        EXP_SIG_UNIVERSAL = 0, EXP_SIG_TRAINED
    #    } EE_ExpressivSignature_t;
    EE_ExpressivSignature_t=c_uint
    EXP_SIG_UNIVERSAL = 0
    EXP_SIG_TRAINED=1
    
     #! Cognitiv Suite training control enumerator
    #    typedef enum EE_CognitivTrainingControl_enum {
    #        COG_NONE = 0, COG_START, COG_ACCEPT, COG_REJECT, COG_ERASE, COG_RESET
    #    } EE_CognitivTrainingControl_t;
    EE_CognitivTrainingControl_t=c_uint
    COG_NONE = 0
    COG_START=1
    COG_ACCEPT=2
    COG_REJECT=3
    COG_ERASE=4
    COG_RESET=5

    #! Handle to internal EmoState structure allocated by EE_EmoStateCreate()
    #typedef void*         EmoStateHandle;
    EmoStateHandle=c_void_p

    #! Handle to internal event structure allocated by EE_EmoEngineEventCreate()
    #typedef void*         EmoEngineEventHandle;
    EmoEngineEventHandle=c_void_p
    
    #! Handle to internal event structure allocated by EE_OptimizationParamCreate()
    #typedef void*         OptimizationParamHandle;
    OptimizationParamHandle=c_void_p

    #typedef void*          DataHandle;
    DataHandle=c_void_p

    #! EmoEngine event types
    #typedef enum EE_Event_enum {
    #        EE_UnknownEvent        = 0x0000,
    #        EE_EmulatorError    = 0x0001,
    #        EE_ReservedEvent    = 0x0002,
    #        EE_UserAdded        = 0x0010,
    #        EE_UserRemoved        = 0x0020,
    #        EE_EmoStateUpdated    = 0x0040,
    #        EE_ProfileEvent        = 0x0080,
    #        EE_CognitivEvent    = 0x0100,
    #        EE_ExpressivEvent    = 0x0200,
    #        EE_InternalStateChanged = 0x0400,
    #        EE_AllEvent            = EE_UserAdded | EE_UserRemoved | EE_EmoStateUpdated | EE_ProfileEvent |
    #                              EE_CognitivEvent | EE_ExpressivEvent | EE_InternalStateChanged
    #    } EE_Event_t;
    EE_Event_t=c_uint
    EE_UnknownEvent        = 0x0000
    EE_EmulatorError    = 0x0001
    EE_ReservedEvent    = 0x0002
    EE_UserAdded        = 0x0010
    EE_UserRemoved        = 0x0020
    EE_EmoStateUpdated    = 0x0040
    EE_ProfileEvent        = 0x0080
    EE_CognitivEvent    = 0x0100
    EE_ExpressivEvent    = 0x0200
    EE_InternalStateChanged = 0x0400
    EE_AllEvent            = EE_UserAdded | EE_UserRemoved | EE_EmoStateUpdated | EE_ProfileEvent |EE_CognitivEvent | EE_ExpressivEvent | EE_InternalStateChanged

    
    #! Expressiv-specific event types
    #    typedef enum EE_ExpressivEvent_enum {
    #        EE_ExpressivNoEvent = 0, EE_ExpressivTrainingStarted, EE_ExpressivTrainingSucceeded,
    #        EE_ExpressivTrainingFailed, EE_ExpressivTrainingCompleted, EE_ExpressivTrainingDataErased,
    #        EE_ExpressivTrainingRejected, EE_ExpressivTrainingReset
    #    } EE_ExpressivEvent_t;
    EE_ExpressivEvent_t=c_uint
    EE_ExpressivNoEvent = 0
    EE_ExpressivTrainingStarted=1
    EE_ExpressivTrainingSucceeded=2
    EE_ExpressivTrainingFailed=3
    EE_ExpressivTrainingCompleted=4
    EE_ExpressivTrainingDataErased=5
    EE_ExpressivTrainingRejected=6
    EE_ExpressivTrainingReset=7
    
    #! Cognitiv-specific event types
    #    typedef enum EE_CognitivEvent_enum {#
    #        EE_CognitivNoEvent = 0, EE_CognitivTrainingStarted, EE_CognitivTrainingSucceeded,
    #        EE_CognitivTrainingFailed, EE_CognitivTrainingCompleted, EE_CognitivTrainingDataErased,
    #        EE_CognitivTrainingRejected, EE_CognitivTrainingReset,
    #        EE_CognitivAutoSamplingNeutralCompleted, EE_CognitivSignatureUpdated
    #    } EE_CognitivEvent_t;
    EE_CognitivEvent_t=c_uint
    EE_CognitivNoEvent = 0
    EE_CognitivTrainingStarted=1
    EE_CognitivTrainingSucceeded=2    
    EE_CognitivTrainingFailed=3
    EE_CognitivTrainingCompleted=4
    EE_CognitivTrainingDataErased=5
    EE_CognitivTrainingRejected=6
    EE_CognitivTrainingReset=7
    EE_CognitivAutoSamplingNeutralCompleted=8
    EE_CognitivSignatureUpdated=9
    


    #    typedef enum EE_DataChannels_enum {
    #        ED_COUNTER = 0, ED_INTERPOLATED, ED_RAW_CQ,
    #        ED_AF3, ED_F7, ED_F3, ED_FC5, ED_T7, 
    #        ED_P7, ED_O1, ED_O2, ED_P8, ED_T8, 
    #        ED_FC6, ED_F4, ED_F8, ED_AF4, ED_GYROX, 
    #        ED_GYROY, ED_TIMESTAMP, ED_ES_TIMESTAMP, ED_FUNC_ID, ED_FUNC_VALUE, ED_MARKER, 
    #        ED_SYNC_SIGNAL
    #    } EE_DataChannel_t;
    EE_DataChannel_t=c_uint
    ED_COUNTER = 0
    ED_INTERPOLATED=1
    ED_RAW_CQ=2
    ED_AF3=3
    ED_F7=4
    ED_F3=5
    ED_FC5=6
    ED_T7=7 
    ED_P7=8
    ED_O1=9
    ED_O2=10
    ED_P8=11
    ED_T8=12
    ED_FC6=13
    ED_F4=14
    ED_F8=15
    ED_AF4=16
    ED_GYROX=17    
    ED_GYROY=18
    ED_TIMESTAMP=19
    ED_ES_TIMESTAMP=20
    ED_FUNC_ID=21
    ED_FUNC_VALUE=22
    ED_MARKER=23    
    ED_SYNC_SIGNAL=24
    
    #/**
    # * Emotiv Detection Suite enumerator
    # */
    #typedef enum EE_EmotivSuite_enum {
    EE_EXPRESSIV = 0
    EE_AFFECTIV = 1
    EE_COGNITIV =2
    EE_EmotivSuite_t=c_uint

    #/**
    # * Expressiv facial expression type enumerator
    # */
    #typedef enum EE_ExpressivAlgo_enum {

    EXP_NEUTRAL            = 0x0001,
    EXP_BLINK            = 0x0002,
    EXP_WINK_LEFT        = 0x0004,
    EXP_WINK_RIGHT        = 0x0008,
    EXP_HORIEYE            = 0x0010,
    EXP_EYEBROW            = 0x0020,
    EXP_FURROW            = 0x0040,
    EXP_SMILE            = 0x0080,
    EXP_CLENCH            = 0x0100,
    EXP_LAUGH            = 0x0200,
    EXP_SMIRK_LEFT        = 0x0400,
    EXP_SMIRK_RIGHT        = 0x0800

    EE_ExpressivAlgo_t=c_uint
    
#    /**
#     * Affectiv emotional type enumerator
#     */
    #typedef enum EE_AffectivAlgo_enum {
    AFF_EXCITEMENT            = 0x0001
    AFF_MEDITATION            = 0x0002
    AFF_FRUSTRATION            = 0x0004
    AFF_ENGAGEMENT_BOREDOM    = 0x0008

    EE_AffectivAlgo_t=c_uint

#    /**
#     * Cognitiv action type enumerator
#     */
    #typedef enum EE_CognitivAction_enum {
    COG_NEUTRAL                        = 0x0001
    COG_PUSH                        = 0x0002
    COG_PULL                        = 0x0004
    COG_LIFT                        = 0x0008
    COG_DROP                        = 0x0010
    COG_LEFT                        = 0x0020
    COG_RIGHT                        = 0x0040
    COG_ROTATE_LEFT                    = 0x0080
    COG_ROTATE_RIGHT                = 0x0100
    COG_ROTATE_CLOCKWISE            = 0x0200
    COG_ROTATE_COUNTER_CLOCKWISE    = 0x0400
    COG_ROTATE_FORWARDS                = 0x0800
    COG_ROTATE_REVERSE                = 0x1000
    COG_DISAPPEAR                    = 0x2000
    EE_CognitivAction_t=c_uint
    
#    /**
#     * Wireless Signal Strength enumerator
#     */
    #typedef enum EE_SignalStrength_enum {
    NO_SIGNAL = 0
    BAD_SIGNAL=1
    GOOD_SIGNAL=2    
    EE_SignalStrength_t=c_uint

#    //! Logical input channel identifiers
#    /*! Note: the number of channels may not necessarily match the number of
#        electrodes on your headset.  Signal quality and input data for some
#        sensors will be identical: CMS = DRL, FP1 = AF3, F2 = AF4.
#    */
    #typedef enum EE_InputChannels_enum {        
    EE_CHAN_CMS= 0
    EE_CHAN_DRL=1
    EE_CHAN_FP1=2
    EE_CHAN_AF3=3
    EE_CHAN_F7=4
    EE_CHAN_F3=5
    EE_CHAN_FC5=6
    EE_CHAN_T7=7
    EE_CHAN_P7=8
    EE_CHAN_O1=9
    EE_CHAN_O2=10
    EE_CHAN_P8=11
    EE_CHAN_T8=12
    EE_CHAN_FC6=13
    EE_CHAN_F4=14
    EE_CHAN_F8=15
    EE_CHAN_AF4=16
    EE_CHAN_FP2=17
    EE_InputChannels_t=c_uint

#    //! EEG Electrode Contact Quality enumeration
#    /*! Used to characterize the EEG signal reception or electrode contact
#        for a sensor on the headset.  Note that this differs from the wireless
#        signal strength, which refers to the radio communication between the 
#        headset transmitter and USB dongle receiver.
#     */
    #typedef enum EE_EEG_ContactQuality_enum {
    EEG_CQ_NO_SIGNAL=0
    EEG_CQ_VERY_BAD=1
    EEG_CQ_POOR=2
    EEG_CQ_FAIR=3
    EEG_CQ_GOOD=4
    EE_EEG_ContactQuality_t=c_uint

    #! Input sensor description
    #    typedef struct InputSensorDescriptor_struct {
    #        EE_InputChannels_t channelId;  # logical channel id
    #        int                fExists;    # does this sensor exist on this headset model
    #        const char*        pszLabel;   # text label identifying this sensor
    #        double             xLoc;       # x coordinate from center of head towards nose
    #        double             yLoc;       # y coordinate from center of head towards ears
    #        double             zLoc;       # z coordinate from center of head toward top of skull
    #    } InputSensorDescriptor_t;
    class InputSensorDescriptor_t(Structure):
        _pack_=8
        _fields_=[("channelId",c_uint),("fExists",c_int),("pszLabel",c_char_p),("xLoc",c_double),("yLoc",c_double),("zLoc",c_double)]
        
        
    def __init__(self,host,port,userNum):
        self.host=host
        self.port=port
        self.numRawSamples=0
        self.userNum=userNum
        thisFilePath =  os.path.dirname(__file__)
        savedPath=os.getcwd()
        os.chdir(thisFilePath)
#        self.dll=cdll.LoadLibrary(thisFilePath+os.sep+'edk_utils.dll')
        self.dll=cdll.LoadLibrary('edk.dll')
        os.chdir(savedPath)

        self.dll.ES_AffectivGetExcitementLongTermScore.restype=c_float
        self.dll.ES_CognitivGetCurrentActionPower.restype=c_float
        self.dll.ES_AffectivGetEngagementBoredomScore.restype=c_float
        self.dll.ES_AffectivGetExcitementShortTermScore.restype=c_float
        self.dll.ES_AffectivGetExcitementLongTermScore.restype=c_float
        self.dll.ES_AffectivGetMeditationScore.restype=c_float
        self.dll.ES_AffectivGetFrustrationScore.restype=c_float
        self.dll.ES_ExpressivGetSmileExtent.restype=c_float
        self.dll.ES_ExpressivGetClenchExtent.restype=c_float
        self.connected=False
        self.state=None
        self.event=None
        self.dataHandle=None
        self.hasState=False
        
    def connect(self):
        if self.connected==True:
            return
        if self.port==-1:
            err = self.dll.EE_EngineConnect("Emotiv Systems-5")
        else:
            err = self.dll.EE_EngineRemoteConnect(self.host,self.port,"Emotiv Systems-5")
        print "Connect emotiv:",err
        if err!=Headset.EDK_OK:
            return
        self.connected=True
        self.event=self.dll.EE_EmoEngineEventCreate()
        self.state=self.dll.EE_EmoStateCreate()
        self.dataHandle=self.dll.EE_DataCreate()
        # set data acquisition buffer length (1 second is easily enough, as we poll 128 times a second)
        self.dll.EE_DataSetBufferSizeInSec(c_float(1.0));
        
    def disconnect(self):
        if self.connected:
            if self.event!=None:
                self.dll.EE_EmoEngineEventFree(self.event)
            if self.state!=None:
                self.dll.EE_EmoStateFree(self.state)
            if self.dataHandle!=None:
                self.dll.EE_DataFree(self.dataHandle)
            if self.dll!=None:
                self.dll.EE_EngineDisconnect()
            self.connected=False

        
    def __del__(self):
        self.disconnect()
        self.dll=None
        
    def handleEvents(self):
        if not self.connected:
            self.connect()            
        while self.connected and self.dll.EE_EngineGetNextEvent(self.event)==Headset.EDK_OK:
            eventType=self.dll.EE_EmoEngineEventGetType(self.event)
#            print "Event type:%x"%eventType
            #check user ID
            userID=c_uint()
            self.dll.EE_EmoEngineEventGetUserId(self.event, byref(userID))
#            print "Event User ID %d"%userID.value
            if (userID.value==self.userNum and eventType & Headset.EE_EmoStateUpdated)!=0:
                # emotional state changed - copy it to our buffer
                self.dll.EE_EmoEngineEventGetEmoState(self.event,self.state)
                if not self.hasState:
                    # first time we got emotional state - now is a good time to start things up on the headset
                    self.hasState=True
                    # display a player number on the dongle
                    self.dll.EE_SetHardwarePlayerDisplay(self.userNum,self.userNum+1)
                    # start raw EEG data streaming
                    self.dll.EE_DataAcquisitionEnable(self.userNum,c_bool(True))
        # our buffer now has the latest emotional state in, so calls to other methods should work

    # returns the currently active action 
    def getCognitivAction(self):
        return self.dll.ES_CognitivGetCurrentAction(self.state)        
    # returns the currently active action intensity
    def getCognitivActionIntensity(self):
        return self.dll.ES_CognitivGetCurrentActionPower(self.state)
        
    def getAffectivEngagedBored(self):
        return self.dll.ES_AffectivGetEngagementBoredomScore(self.state)
    def getAffectivExcitement(self):
        return self.dll.ES_AffectivGetExcitementShortTermScore(self.state)
    def getAffectivExcitementLongTerm(self):
        return self.dll.ES_AffectivGetExcitementLongTermScore(self.state)
    def getAffectivMeditation(self):
        return self.dll.ES_AffectivGetMeditationScore(self.state)
    def getAffectivFrustration(self):
        return self.dll.ES_AffectivGetFrustrationScore(self.state)


    def getExpressivWinkLeft(self):
        return self.dll.ES_ExpressivIsLeftWink(self.state)        
    def getExpressivWinkRight(self):
        return self.dll.ES_ExpressivIsRightWink(self.state)        
    def getExpressivBlink(self):
        return self.dll.ES_ExpressivIsBlink(self.state)
    def getExpressivEyelids(self):
        leftLid=c_float()
        rightLid=c_float()
        self.dll.ES_ExpressivGetEyelidState(self.state,byref(leftLid),byref(rightLid))
        return (leftLid.value,rightLid.value)
    def getExpressivEyeDir(self):
        leftEye=c_float()
        rightEye=c_float()
        self.dll.ES_ExpressivGetEyeLocation(self.state,byref(leftEye),byref(rightEye))
        return (leftEye.value,rightEye.value)
    def getExpressivSmile(self):    
        return self.dll.ES_ExpressivGetSmileExtent(self.state)
    def getExpressivClench(self):    
        return self.dll.ES_ExpressivGetClenchExtent(self.state)
        
    def readRawData(self):
        self.dll.EE_DataUpdateHandle(self.userNum, self.dataHandle);
        samplesRead=c_uint()
        self.dll.EE_DataGetNumberOfSample(self.dataHandle,byref(samplesRead))
        self.numRawSamples=samplesRead.value
        
    # get the samples that are currently waiting for this channel
    # NB: unless you call this at least once a second, you will lose data
    # as that is the size of the input buffer
    def getRawChannel(self,channelNum):
        if self.numRawSamples>0:
            dataBuf=(c_double*self.numRawSamples)()            
            self.dll.EE_DataGet(self.dataHandle, channelNum, dataBuf, self.numRawSamples);
            return dataBuf
        else:
            return []