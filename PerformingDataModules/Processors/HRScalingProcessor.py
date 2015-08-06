

from performingdata.Processor import Processor
import sys
from collections import deque
import time

# processes GSR, Heart Rate to get a 'threat level' from 0 to 3
class HRScalingProcessor(Processor):

    def __init__(self):
        Processor.__init__(self,"HRScalingProcessor", [("Input","Input Stream")], ["Scaled Input"],[("Age","Age","25"),("Fitness","Fitness","4"),("Gender","Gender","Male")])    
        self.run()  

    # process initial arguments
    def processArguments(self,initialTime):
        # make sure that the smoothing value is a float
        self.age=float(self.argumentValues[0])
        self.fitness=int(self.argumentValues[1])
        self.gender=self.argumentValues[2]
        self.maxFitScale = 7
        self.minFitScale = 1

        self.maxUnfit = (217 - (0.85 * self.age))
        if(self.gender=="Male"):
            self.maxFit = (202.0 - (0.55 * self.age))
        else:
            self.maxFit = (216 - (1.09 * self.age))

        self.max = self.maxFit+(self.maxUnfit-self.maxFit)*(self.maxFitScale-self.fitness)/(self.maxFitScale-self.minFitScale)

        self.min = self.getMinHR(self.age,self.fitness,self.gender)

    def getMinHR(self,age,fitness,gender):
        if(gender=="Male"):
            if (age <= 25):
                vals = [88,82,76,72,68,63,57]             
            elif (age > 25 and age <=35):
                vals = [86,80,75,71,67,62,57] 
            elif (age > 35 and age <=45):
                vals = [88,82,76,72,67,62,57]
            elif (age > 45 and age <=55):
                vals = [87,81,76,72,66,63,57]
            elif (age > 55 and age <=65):
                vals = [87,81,76,71,67,62,57]
            elif (age > 65):
                vals = [87,81,75,71,67,62,54]          
        else:
            if (age <= 25):
                vals = [85,78,72,68,64,59,52]             
            elif (age > 25 and age <=35):
                vals = [85,78,73,68,64,58,52] 
            elif (age > 35 and age <=45):
                vals = [86,79,73,69,65,60,53]
            elif (age > 45 and age <=55):
                vals = [87,80,74,70,66,61,54]
            elif (age > 55 and age <=65):
                vals = [85,79,74,70,65,59,54]
            elif (age > 65):
                vals = [83,77,72,68,64,59,53]

        return float(vals[fitness])  
            
    # main data processing function
    def process(self,timeStamp,values,queueNo):
            val=float(values[0])
            newValue = (((val - self.min) * (100.0 - 0.0)) / (self.max - self.min)) + 0
            self.addProcessedValues(newValue)
            
                    
if __name__ == '__main__': HRScalingProcessor()
