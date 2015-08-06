from performingdata.Pubnub import Pubnub
import sys
import time
import json

from threading import Lock


class PubnubRecorder():

    def run(self, channel, filename):
        self.channel = channel
        self.filename = filename
        self.debug = False

        pubnub = Pubnub("pub-c-e655613e-f776-4301-9f29-f71edbcd6559",
                        "sub-c-2eafcf66-c636-11e3-8dcd-02ee2ddab7fe",
                        "sec-c-ZjUwZDgzMTItYzE2Mi00ZGYyLTg2NGMtNmE5N2Q3MGI0MTli",
                        False)


        while True:
            pubnub.subscribe({
                    'channel': self.channel,
                    'callback': self.setData
                    })

    def setData(self, message):
        data = json.dumps(message)
        with open(self.filename, 'a') as the_file:
            the_file.write(str(data)+'\n')
        if self.debug:
            print "Wrote value"
         

def main(channel,filename):
    c = PubnubRecorder()
    c.run(channel,filename)

if __name__ == '__main__':
    main(sys.argv[1],sys.argv[2])
