import sys


# if __name__ == '__main__':
#     cases = ""
#     f = open("out.txt", "w+")
#     for i in xrange(100):
#         values = ""
#         for j in xrange(2):
#             value = '"feature{}" : "value{}"'.format(j, j)
#             if j != 0:
#                 values += " , "
#             values += value

#         case = '"key" : "key{}", "value" : {} \n'.format(i,  "{" + values + "}")
#         if i != 0:
#             cases += ",\n"
#         cases += "{" + case + "}"
#     f.write(cases)

if __name__ == '__main__':
    cases = ""
    f = open("out.txt", "w+")
    for i in xrange(100):
        values = ""
        for j in xrange(2):
            value = '"feature{}":"value{}"'.format(j, j)
            if j != 0:
                values += ","
            values += value

        case = '"key{}":{}'.format(i,  "{" + values + "}")
        if i != 0:
            cases += ","
        cases += case
    cases = "{" + cases + "}"
    f.write(cases)
