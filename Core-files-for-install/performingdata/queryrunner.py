from Consumer import Consumer
import sys,time

class QueryRunner():

    def __init__(self, host,port,srcFile):
        file=open(srcFile)
        data=file.read()        
        self.consumer = Consumer(data+"@"+host,port, self,runOnlyOnce=True)

    def run(self):
        while(True):
            try:
                self.consumer.waitData()
                data = self.consumer.getData()
                if data != None:
                    print str(data)
            except Consumer.ConsumerException,e:
                print e.msg
                return
                
            
            
def main(host,port,query):
    chart = QueryRunner(host,port,query)
    chart.run()
    
if __name__ == '__main__': main(sys.argv[1],int(sys.argv[2]),sys.argv[3])
    