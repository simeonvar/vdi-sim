
# This script will sample from the raw data to 
from random import randrange

inf = "./data/raw_data.weekday"
MAX_ROW = 1538


day = "weekday"
# day = "weekend"
nVMs = 20
nSamples = 10

# generate a sorted random row numbers
def gen_rand_rows(n, rows):

    for i in range(0,n):
        r = randrange(MAX_ROW)
#        print r
        rows.append(randrange(MAX_ROW))
    rows.sort()

# main body
for i in range(0, nSamples):
    outf = "./data/%s-%d-vm.sample.%d"%(day,nVMs,i+1)
    rows = []
    gen_rand_rows(nVMs, rows)
    
    print "Sampling run: %d" % i
    fin = open(inf, "r")
    fout = open(outf, "w+")
    
    linecnt = 0
    ri = 0
    lines = []
    for line in fin:
        if linecnt == rows[ri]:
            print "gettig line %d" % rows[ri]
            splits = line.rstrip().split(",")
            lines.append(splits)
            ri += 1
            if ri == nVMs:
                break
        linecnt += 1

    for s in range(0, 86400):
        linecnt = 0
        for l in lines:
            fout.write(l[s+1]) # the first one is the weekday
            if linecnt != (nVMs-1):
                fout.write(",")
            linecnt += 1
        fout.write("\n")
    
    fin.close()
    fout.close()

print "Done"







