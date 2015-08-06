from performingdata.Processor import Processor


class AverageProcessor(Processor):

    def __init__(self):
        Processor.__init__(self, "AverageProcessor", [("source", "Current numerical data")], [("Average", "Average Value")], [])
        self.count = 0
        self.total = 0
        self.run()

    # main data processing function
    def process(self, timeStamp, values, queueNo):
        # data point
        curValue = float(values[0])
        self.count += 1
        self.total += curValue
        ave = self.total/self.count
        self.addProcessedValues(ave)


if __name__ == '__main__':
    AverageProcessor()
