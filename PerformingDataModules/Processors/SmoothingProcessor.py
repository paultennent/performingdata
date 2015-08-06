from performingdata.Processor import Processor

#processor to smooth an incoming stream of data.
#the amount of smoothing can be controlled based on a float value for the parameter 'smoothfactor'

class SmoothingProcessor(Processor):
    
    def __init__(self,):
        Processor.__init__(self,"SmoothingProcessor", [("source","data source to smooth")], ["Smoothed data"],[("smoothing","Initial smoothing factor (0.0 - 1.0)")],self.defineGUI)
        self.lastValue=None
        self.MAX_RANGE = 10000.0
        self.despikeValues=[]
        self.run()

    # removes spikes from nexus gsr data (which is lovely and smooth but has horrible spikes)
    # get first:
    # a b c: out b
    # b c d: out c
    # c d s : out d
    # d s e : out d - delete s
    # d e f : out e
    def deSpikeFilter(self,value):
        self.despikeValues.append(value)
        if len(self.despikeValues)!=4:
            return value
        self.despikeValues.pop(0)
        if self.despikeValues[2]>=self.despikeValues[1]>=self.despikeValues[0]:
            None
        elif self.despikeValues[2]<=self.despikeValues[1]<=self.despikeValues[0]:
            None
        else:
            self.despikeValues[1]=self.despikeValues[0]
        return self.despikeValues[1]

    # process initial arguments
    def processArguments(self,initialTime):
        # make sure that the smoothing value is a float
        self.argumentValues[0]=float(self.argumentValues[0])
            
    # main data processing fn - process one sample
    def process(self,timeStamp,values,queueNo):
        datain = values[0]
            
        curValue = float(datain)
        curValue=self.deSpikeFilter(curValue)
	      
        if self.lastValue!=None:
            # smooth the value
            curValue=self.lastValue*(1-self.argumentValues[0]) + curValue*self.argumentValues[0]
            self.addProcessedValues(curValue)
        self.lastValue=curValue
                    
    def defineGUI(self,frame):
        import wx
        frame.SetBackgroundColour("White")
        sb = wx.StaticBox(frame, label='Smoothing Factor')
        sbs = wx.StaticBoxSizer(sb, orient=wx.VERTICAL) 
        self.slider = wx.Slider(frame,wx.ID_ANY,minValue=0,maxValue=self.MAX_RANGE,style=wx.SL_HORIZONTAL,size=(300,50))
        self.slider.SetValue(self.argumentValues[0]*self.MAX_RANGE)
        self.slider.Bind(wx.EVT_SCROLL,self.OnValueChange)
        sbs.Add(self.slider,flag=wx.EXPAND)
        self.label = wx.TextCtrl(frame,wx.ID_ANY,str(self.argumentValues[0]),style=wx.TE_PROCESS_ENTER)
        self.label.Bind(wx.EVT_TEXT_ENTER,self.OnTextEntered)
        sbs.Add(self.label,flag=wx.EXPAND)
        frame.SetSizerAndFit(sbs)
        
    def OnValueChange(self, event):
        val=event.GetPosition()
        self.argumentValues[0] = float(val)/self.MAX_RANGE
        self.label.SetValue(str(self.argumentValues[0]))
        
    def OnTextEntered(self, event):
        self.argumentValues[0] = float(self.label.GetValue())
        self.slider.SetValue(self.argumentValues[0]*self.MAX_RANGE)                                                    
                    
if __name__ == '__main__': SmoothingProcessor()
