from performingdata.Processor import Processor
from json import JSONEncoder


class JsonMultiChannelBufferProcessor(Processor):

    def __init__(self):
        Processor.__init__(self, "JsonMultiChannelBufferProcessor", [("*source", "input data")], [("buffered", "Buffered JSON blob")], [("type","choose millis or values", "values"),("number", "number of values or millis", "128"),("channelnames","comma seperated names for all incoming channels",""),("channeltypes","comma seperated trpes for all incoming channels",""),("sigfigs","accuracy of the data - use -1 for default","-1")])
        self.run()

    def processArguments(self, firsttimeStamp):
        self.type = str(self.argumentValues[0])
        self.number = float(self.argumentValues[1])
        self.firsttimeStamp = firsttimeStamp
        self.channelnames = self.argumentValues[2].split(",")
        self.channeltypes = self.argumentValues[3].split(",")
        self.sigfigs = int(self.argumentValues[4])
        self.mybuffer = {}
        self.mybuffer["channelnames"] = self.channelnames
        self.mybuffer["channeltypes"] = self.channeltypes
        self.mybuffer["data"] = []
        self.dominantQ = None


    # main data processing function
    def process(self, timeStamp, values, queueNo):
        # data point
        if self.dominantQ == None:
            self.dominantQ = queueNo
        elif self.dominantQ == queueNo:

            # build the data buffer
            data = []
            for i in range(0,len(self.channelnames)):
                toAppend = float(values[i])
                if(self.sigfigs != -1):
                    toAppend = round(toAppend,self.sigfigs)
                data.append(toAppend) 
            self.mybuffer["data"].append([timeStamp, data])

            if self.type == "values":
                if len(self.mybuffer["data"]) == self.number:
                    self.addProcessedValues(self.buildOutput())
                    self.mybuffer["data"] = []
            else:
                if (timeStamp-self.firsttimeStamp) > (self.number/1000.0):
                    self.addProcessedValues(self.buildOutput())
                    self.mybuffer["data"] = []
                    self.firsttimeStamp = timeStamp

    def buildOutput(self):
        jsonstring = JSONEncoder().encode(self.mybuffer)
        #print jsonstring
        return jsonstring


if __name__ == '__main__':
    JsonMultiChannelBufferProcessor()
