import sys
from Processor import Processor
import ctypes
import dll_loader

FILTER_LOOKUP = { "test": 0,
              "null": 1,
              "average": 2,
              "emg": 3,
              "ecg": 4,
              "gsr": 5,
              "bpm": 6,
              "pulse": 7,
              "noise": 8}

class ProcessorWorkerFilter(object):
    
    filterLib = dll_loader.getLib("filters")
    makeFilter = filterLib.makeFilter
    process = filterLib.process
    
    def __init__(self, type):
        self.id = self.makeFilter(type)

    def handleData(self, datain):
        input = ctypes.c_double(datain)
        output = ctypes.c_double()
        self.process(self.id, input, ctypes.byref(output))
        return output.value 

def main(inhost,inport, outport, type):
    me = ProcessorWorkerFilter(type)
    c = Processor()
    c.run(inhost,inport,outport,me)
    
if __name__ == '__main__': main(sys.argv[1],int(sys.argv[2]),int(sys.argv[3],int(sys.argv[4])))