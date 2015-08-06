import sys
from Processor import Processor

class ProcessorWorker():

    def handleData(self,datain):
        return float(datain) * -1  

def main(inhost,inport, outport):
    me = ProcessorWorker()
    c = Processor()
    c.run(inhost,inport,outport,me)
    
if __name__ == '__main__': main(sys.argv[1],int(sys.argv[2]),int(sys.argv[3]))