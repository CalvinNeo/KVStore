#coding: utf8

import random


if __name__ == '__main__':
    random.seed()

    index = 0
    f = open("data2.txt", "w")
    for i in xrange(100):
        year = random.randint(2000, 2017)
        month = random.randint(1, 12)
        day = random.randint(1, 30)
        H = random.randint(0, 23)
        M = random.randint(0, 59)
        S = random.randint(0, 59)


        PM25 = random.randint(0, 300)
        PM10 = random.randint(0, 350)
        SO2 = random.randint(0, 1600)
        CO = random.randint(0, 120)
        NO2 = random.randint(0, 1200)
        O3 = random.randint(0, 400)

        t = "%04d-%02d-%02d %02d:%02d:%02d" % (year, month, day, H, M, S)
        data = "{},{},{},{},{},{},{},{}\n".format(t, index + 1, PM25, PM10, SO2, CO, NO2, O3)

        f.write(data)

        index += 1
        index = index % 10

    f.close()
