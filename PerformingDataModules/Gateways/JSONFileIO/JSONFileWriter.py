from performingdata.Consumer import Consumer
import json
import sys
import datetime
import calendar


class FileWriter():

    def __init__(self, host, port, filename):
        self.consumer = Consumer(host, port, self)
        self.filename = filename

    def run(self):
        while(True):
            self.consumer.waitData()
            data = self.consumer.getData()
            if data is not None:
                with open(self.filename, 'a') as the_file:
                    the_file.write(str(data)+'\n')

def main(host, port, filename):
    chart = FileWriter(host, port, filename)
    chart.run()

if __name__ == '__main__':
    main(sys.argv[1], int(sys.argv[2]), str(sys.argv[3]))
