#!/usr/bin/env python

import ConfigParser
# a simple simulator program 

SETTING = "./setting.conf"


# VDI states
FULL = 1
MIGRATING = 2
MIGRATED = 3

#configs = {}

def read_config():
    Config = ConfigParser.ConfigParser()
    Config.read(SETTING)
    dict1={}
    for section in Config.sections():
        dict1 = dict(dict1.items() + Config.items(section))
    return dict1

def run(configs):
    inf = open(configs['input'], 'r')
    nVMs = int(config['nVMs'])
    nVDIs = int(config['nVDIs'])
    interval = int(config['interval'])
    # each line is one second of snapshot of NUM of desktops, 1 is active and 0 is idle
    sec_past = 0
    # current VM states in a time interval
    vm_states = []
    for i in range(0, nVMs):
        vm_states.append(0)

    cur_sec = 0                 # current second of the day, 0 ~ 24*60*60 = 86400
    cur_vdi_states =[]
    # vdi states, a list of list, e.g.,  [[FULL, MIGRATING, FULL], [FULL, MIGRATING, FULL], ...]
    vdi_states = []
    for line in inf:
        line = line.rstrip()
        activities = line.split(",")
        
        if cur_sec == 0:
            for i in range(0,nVDIs):
                cur_vdi_states.append(FULL)
        else:
            cur_vdi_states = vdi_states[cur_sec -1]
        # evaluate the current situation
        for i in range(0, nVMs):
            if activities[i] == '1':
                vm_states[i] = 1
        if sec_past >= interval:
            # Reaching the end of the interval, time to make decision
            next_vdi_states = make_decision(configs, vm_states, cur_sec, interval, vdi_states)
            # re-init the interval value to  0
            sec_past = 0            
            vdi_states.append(next_vdi_states)
        else:
            sec_past += 1            
            vdi_states.append(cur_vdi_states)
        cur_sec += 1

    inf.close()

def make_decision(configs, vm_states, cur_sec, interval, vdi_states):
    
    migrated = decisions[-1]    # the last decision indicates the current status
    next_decision = False

    if migrated:
        # decides whether to migrate back
        migrated
    else:
        #decides whether to migrate 
        migrated

    return next_decision
