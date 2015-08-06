from performingdata.Processor import Processor
import sys
from collections import deque
import time

class ScalingProcessor(Processor):

    def __init__(self):
        Processor.__init__(self,"ScalingProcessor", [("Input","Input Stream")], [("Scaled","Scaled Input")],[("Old Min","Old Min","0.0"),("Old Max","Old Max","1.0"),("New Min","New Min","0.0"),("New Max","New Max","100.0")])    
        self.run()  

    # process initial arguments
    def processArguments(self,initialTime):
        self.oMin=float(self.argumentValues[0])
        self.oMax=float(self.argumentValues[1])
        self.nMin=float(self.argumentValues[2])
        self.nMax=float(self.argumentValues[3])

            
    # main data processing function
    def process(self,timeStamp,values,queueNo):
            val=float(values[0])
            newValue = (((val - self.oMin) * (self.nMax - self.nMin)) / (self.oMax - self.oMin)) + self.nMin
            self.addProcessedValues(newValue)
            
                    
if __name__ == '__main__': ScalingProcessor()
