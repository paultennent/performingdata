from performingdata.Consumer import Consumer
import sys,time

class DataConsumerTest():

    def __init__(self, host,port):
        self.consumer = Consumer(host,port, self)

    def run(self):
        while(True):
            self.consumer.waitData()
            data = self.consumer.getData()
            if data != None:
                print str(data)
            
            
def main(host,port):
    chart = DataConsumerTest(host,port)
    chart.run()
    
if __name__ == '__main__': main(sys.argv[1],int(sys.argv[2]))
    