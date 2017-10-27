# coding: utf8

from socket import *  
from time import ctime
import time
import itertools

class LineFile(object):
    def __init__(self, fn):
        self.f = open(fn, "r")

    def __del__(self):
        self.f.close()

    def __iter__(self):
        return self

    def next(self):
        while True:
            l = self.f.readline()
            if not l:
                raise StopIteration()
            else:
                l = l.strip(" ").strip("\n")
                if l == "":
                    continue
                else:
                    return l

def stream_server(source, total, BATCH_SIZE = 2, HOST = '127.0.0.1', PORT = 9999):
    ADDR = (HOST, PORT)
    BUFSIZE = 1024
    tcpSvrSock = socket(AF_INET, SOCK_STREAM)
    tcpSvrSock.bind(ADDR)
    tcpSvrSock.listen(5) # backlog
    while True:
        print 'wating for connection...'
        tcpCliSock, addr = tcpSvrSock.accept()
        print 'connection accepted, start data streaming...'

        percentile, bi = total / 100, 0

        for (i, item) in itertools.izip(itertools.count(0), source):
            bi += 1
            if i % percentile == 0:
                print "completing percent %d " % int(i / percentile)

            print "i", i
            [t, index, PM25, PM10, SO2, CO, NO2, O3] = item.split(",")
            data = "{},{},{},{},{},{},{},{}".format(t, index, PM25, PM10, SO2, CO, NO2, O3)
            tcpCliSock.send(data + "\n")

            if bi >= BATCH_SIZE:
                time.sleep(1)

        print "data transmition succeed, now wait peer disconnecting"
        data = tcpCliSock.recv(BUFSIZE)  
        if not data:
            print "connection closed..."
            break  

def read_file(maximun_lines = 100):
    f = LineFile("data2.txt")
    res = []
    for i in xrange(maximun_lines):
        try:
            l = f.next()
            t, index, PM25, PM10, SO2, CO, NO2, O3 = l.split(",")
            [index, PM25, PM10, SO2, CO, NO2, O3] = map(int, [index, PM25, PM10, SO2, CO, NO2, O3])
            tsobj = time.strptime(t, "%Y-%m-%d %H:%M:%S")
            ts = time.mktime(tsobj)
            res.append("{},{},{},{},{},{},{},{}".format(ts,index, PM25, PM10, SO2, CO, NO2, O3).decode("utf8"))
        except StopIteration:
            break
    return res

if __name__ == '__main__':
    source = read_file()
    stream_server(source, 100)