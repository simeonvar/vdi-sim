
#!/usr/bin/env python

import ConfigParser
import math
import linecache
import sys
from random import randint
# a simple simulator program 

nVMs = 200
nVDIs = 10

interval = 60 * 20


def randSample(n, min, max):
    ret = []
    for i in range(0, n):
        ret.append(randint(min, max))

    return ret

def run(inf):

    server1_idle_intervals = 0
    server1_state = 0

    # randomly sampled 20 VMs. And observe their traces
    vmlist1 = randSample(20, 0, 199)
    # vmlist2 = randSample(20, 0, 199)
    print "vmlist is ", vmlist1

    cur_sec = 0

    inf = open(inf, "r")

    total_idle_intervals = 0

    vdi_total_idle = {}
    # init the array
    vdi_idle_one_interval = {}
    
    vdi_total_idle_seoncds = {}
    vdi_state_at_this_interval = {}

    # init the array
    for i in range(0,nVDIs):
        vdi_state_at_this_interval[i] = 0

    for i in range(0,nVDIs):
        vdi_total_idle[i] = 0
        vdi_idle_one_interval[i] = 0
        vdi_total_idle_seoncds[i] = 0

    for line in inf:
        line = line.rstrip()

        if len(line) <= 1:
            continue
        activities = line.split(",")

        active_flag = False
        # evaluate the current situation

        vdi_state_one_second = {}

        for i in range(0,nVDIs):
            vdi_state_one_second[i] = 0

        for i in vmlist1:
            a = int(activities[i].lstrip())
            server1_state += a

        for i in range(0, nVMs):
            a = int(activities[i].lstrip())
            vdi_state_at_this_interval[i/(nVMs/nVDIs)] += a
            vdi_state_one_second[i/(nVMs/nVDIs)] += a

        for i in range(0,nVDIs):
            if vdi_state_one_second[i] == 0:
                vdi_total_idle_seoncds[i] += 1

        for i in range(0,nVDIs):
            if vdi_state_at_this_interval[i] == 0:
                vdi_idle_one_interval[i] += 1
            
        cur_sec += 1
        
        if cur_sec % interval == 0:
            for i in range(0,nVDIs):
                # it requires the VM to be all idle for that
                if vdi_idle_one_interval[i] == 0:
                    vdi_total_idle[i] += 1
                # re-init the interval data
                vdi_idle_one_interval[i] = 0

            if server1_state == 0:
                server1_idle_intervals += 1
            server1_state = 0

    print "Total_seconds * nVDIs: %d" % (cur_sec * nVDIs)


    for i in range(0,nVDIs):
        print "VDI %d: Total idle interval : %d" %(i, vdi_total_idle[i])
        print "VDI %d: Total idle percentage : %f" %(i, float(vdi_total_idle[i])/(cur_sec/interval))
        total_idle_intervals += vdi_total_idle[i]
    print "percentage: %f" % (float(total_idle_intervals)/(cur_sec*nVDIs/interval))

    total_idle_seonds = 0
    for i in range(0,nVDIs):
        print "VDI %d: Total idle seconds : %d" %(i, vdi_total_idle_seoncds[i])
        print "VDI %d: Total idle percentage : %f" %(i, float(vdi_total_idle_seoncds[i])/(cur_sec))
        total_idle_seonds += vdi_total_idle_seoncds[i]
    print "percentage2: %f" % (float(total_idle_seonds)/(cur_sec*nVDIs))

    print "server1 idle intervals: %d" % server1_idle_intervals
    print "server1 %d: Total idle percentage : %f" %(i, float(server1_idle_intervals)/(cur_sec/interval))


    inf.close()

#f = "data/vm-200.csv"

#run(f)
