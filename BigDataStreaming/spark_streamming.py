# coding: utf8

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkConf
import sys
from time import ctime
import time

sc = SparkContext("local[4]", "AirAssess")
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("127.0.0.1", 9999)

# time, index, PM25, PM10, SO2, CO, NO2, O3
limitation = [150, 350, 800, 60, 1200, 400]
name = ["PM25", "PM10", "SO2", "CO", "NO2", "O3"]

def reorg(line):
    s = map(int, map(float, line.strip(" ").split(",")))
    ts = s[0]
    i = s[1]
    ans = []
    for j in xrange(2, 8):
        ans.append((ts, i, j - 2, s[j]))
    return ans

indexs = lines.flatMap(lambda line: reorg(line))
checking = indexs.filter(lambda index: index[3] >= limitation[index[2]])
fmt = checking.map(lambda index: '{}|{}|{}|{}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(index[0])), index[1], name[index[2]], index[3]))

fmt.pprint()

ssc.start() 
ssc.awaitTermination()



# def fil(index):
#     for i in xrange(len(index)):
#         if index[i][3] >= limitation[index[i][2]]:
#             tstr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(index[i][0]))
#             print '{} | {} | {} | {}'.format( tstr, index[i][1], name[index[i][2]], index[i][3])

# indexs = lines.map(lambda line: reorg(line))
# checking = indexs.filter(lambda index: fil(index))