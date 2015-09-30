# adjust local partial resume latency. These local partials have to wait for all the other remote partials to vacate so that they can be swapped back. 

import sys
PARTAIL_MIGRATION_LATENCY = 4

logs_file = sys.argv[1]
outf = logs_file+"-adjusted-perfect-paced-latencies.csv"
of = open(outf, "w+")
for l in open(logs_file, "r"):
    splits = l.rstrip().lstrip().split(",")
    exceed = splits[7]
    vm_index = int(splits[1])
    src = int(splits[2])
    cur = int(splits[3])
    timestamp = int(splits[0])
    latency = int(splits[8])
    if exceed == "N" and src == cur:
        latency = 0
    elif exceed == "N" and src != cur:
        latency = 40
    else:
        latency = 4
    splits[8] = str(latency)
    of.write(",".join(splits)+"\n")
    
of.close()
print "Done"
