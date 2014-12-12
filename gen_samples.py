#Generate samples that can use to verify the correctness of result

import os, sys

nVMs = 80
lnum = 86400                    # total line numbers, == second# of a day

# generate a sample that are idle for the 3 hours, then all active for the rest of the day. 
# to see if the simulator will resume.
def gen_sam1():
    print "Generating sample1"
    o1 = "./data/gen_sam2"
    f = open(o1, "w+")
    t1 = 3 * 60 * 60
    for i in range(0, lnum):        
        for j in range(0, nVMs):            
            if i < t1:
                f.write("0")
            else:
                f.write("1")
            if j < nVMs - 1:
                f.write(",")
        f.write("\n")
    f.close()
    print "Done. See result in %s" % o1


gen_sam1()
    
