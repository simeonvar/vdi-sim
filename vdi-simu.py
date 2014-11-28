#!/usr/bin/env python

import ConfigParser
import math
# a simple simulator program 

SETTING = "./setting.conf"

# VM states
ORIGINAL = 1
MIGRATING = 2
MIGRATED = 3
RESUMING = 4
configs = {}

# VDI state
FULL = 5
MIGRATING = 6
S3 = 7
REINTEGRATING = 8

# global variables used in decide_to_migrate
migration_interval = 0
cumulative_interval = 0
# record all vm states when decide to migrate to 
# keep track of who are idles and who are active later
vm_states_before_migration = []
resume_interval = 0

def read_config():
    Config = ConfigParser.ConfigParser()
    Config.read(SETTING)
    dict1={}
    for section in Config.sections():
        dict1 = dict(dict1.items() + Config.items(section))
    return dict1

configs = read_config()

def run():
    inf = open(configs['input'], 'r')
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    interval = int(configs['interval'])
    
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
            next_vdi_states = make_decision(vm_states, vdi_states, cur_sec)
            # re-init the interval value to  0
            sec_past = 0            
            vdi_states.append(next_vdi_states)
        else:
            sec_past += 1            
            vdi_states.append(cur_vdi_states)
        cur_sec += 1

    inf.close()

def get_migration_interval(nActive, nIdles, configs):
    method = configs['method']
    interval = int(configs['interval'])
    full_migrate = float(configs['full_migrate'])
    partial_migrate = float(configs['full_migrate'])
    assert full_migrate > 0
    assert partial_migrate > 0 

    if method == 'partial':
        return math.ceil( ((nActive + nIdles) * partial_migrate) / interval)
    elif method == "partial + full":
        ret = math.ceil( (nActive * full_migrate + nIdles * partial_migrate) / interval)
        return ret
    else:
        raise Exception("Unknown migration method: %" % method)


# query the traces data and return the minimun number idle VMs that decide whether the vdi is migratable
def get_idle_threshold(cur_sec):
    # FIXME: Only one stategy is implemented here: if the idle ratio is >70%
    ratio = 0.7
    
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDUs'])
    vms_per_vdi = nVMs / nVDIs
    
    threshold = int( vms_per_vdi * ratio )

    return threshold

# assume that migratable_servers is sorted in descending order of idleness
def decide_migrate_plan(migratable_servers):

    nVDIs = int(configs['nVDUs'])
    
    slack = int(config['slack'])
    
    # FIXME: assume all VDI servers all have 100% capacity
    # FIXME: the detailed plan as for which 
    # server migrates to which server is not decided

    assert slack == 1

    # just cut half the vdi servers
    count = 0
    to_migrate = []
    for i in migratable_servers:
        if count < nVDIs / 2:
            to_migrate.append(i)
        count += 1

    return to_migrate

def decide_to_migrate(vm_states,vdi_states, cur_sec):

    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDUs'])
    vms_per_vdi = nVMs / nVDIs

    # get idleness
    vdi_idleness = {}
    for i in range(0, nVDIs):
        idle_vms = 0 
        for j in range(0, vms_per_vdi):
            if vm_states[i*vms_per_vdi + j] == 0:
                idle_vms += 1
        ratio = float(idle_vms) / float(vms_per_vdi) # 
        vdi_idleness[i] = idle_vms

    # sort the vdi server from highest to lowest idle ratio (idle VM#)
    # check if the probability is greater than the threshold
    migratable_servers = []
    threshold = get_idle_threshold(cur_sec) # the minimum number of idle VMs to decide whether a vdi is migratable
    for vdi_num, idle_vms in sorted(vdi_idleness.items(), key=lambda x: x[1], reverse=True):
        if idle_vms > threshold:
            migratable_servers.append(vdi_num)

    # decide the migration plan using a strategy
    to_migrate = decide_migrate_plan(migratable_servers)

    if len(to_migrate) > 0:
        # copy the previous states first
        next_states = []
        c = 0
        for s in vdi_states[-1]:
            next_states[c] = s
            c += 1
        # update the new stats
        for i in to_migrate:
            next_states[i] = MIGRATING
        return (next_states, True)
    else:
        # return the previous setting
        return (vdi_states[-1], False)

def get_overall_state(vdi_states):
    state = "full"
    for i in vdi_states[-1]:    # check the last states
        if i == MIGRATING:
            state = "migrating"
            break
        if i == S3:
            state = "migrated"
            break

    return state

def update_states(vdi_states, prevs, nexts):
    last_states = vdi_states[-1]
    next_states = []
    for i in last_states:
        if i == prevs:
            next_states.append(nexts)
        else:
            next_states.append(i) 
    return next_states

def record_vm_states(vm_states):
    c = 0
    if len(vm_states_before_migration) == 0:
        # init it first
        for i in vm_states:
            vm_states_before_migration.append(i)
    for i in vm_states:
        vm_states_before_migration[c] = i
        c += 1

def resume_policy(vms_awake):
    nVDIs = int(configs['nVDUs'])
    vms_per_vdi = nVMs / nVDIs 
    # FIXME: only implement a policy here: any vm awake  > 5 will lead to the whole cluster to resume
    resume = False
    for i in vms_awake:
        if i > 5:
            resume = True
            break
    return resume

def decide_to_resume(vm_states, vdi_states, cur_sec):

    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDUs'])
    vms_per_vdi = nVMs / nVDIs

    # how many vms becomes active from idle from 
    # FIXME: only considers the partial migration. Full migration is not supported
    # FIXME: only counts the machine idle that turns into active from idle
    last_states = vdi_states[-1]
    vms_woke_up_per_vdi = []
    for i in range(0, nVDIs):   # init the result
        vms_woke_up_per_vdi.append(0)

    c = 0 
    for s in last_states:
        if s == MIGRATED:
            for i in range(0, nVMs): # iterate all vms of that vdi server
                if vm_states[c*vms_per_vdi + i] == 1 and vm_states_before_migration[c*vms_per_vdi + i] == 0:
                    vms_woke_up_per_vdi[c] += 1
    
    resume = resume_policy(vms_woke_up_per_vdi)
    next_states = []
    if resume:
        next_states = update_states(vdi_states, MIGRATED, REINTEGRATING)
    else:
        for i in range(0, nVDI):
            next_states[i] = vdi_states[-1][i]

    return next_states

def make_decision(vm_states,vdi_states, cur_sec):
    
    next_states = []
    overall_state = get_overall_state(vdi_states)
    
    if overall_state == "full":
        # decide whether to migrate
        (next_states, decision) = decide_to_migrate()
        # how many intervals it takes to migrate all VMs
        migration_interval = get_migration_interval(nActives, nIdles, configs)
        assert migration_interval > 0
        if decision == True:
            cumulative_interval = 0
            record_vm_states(vm_states)

    if overall_state == "migrating":
        # see if the migrating interval is reached or not
        assert migration_interval > 0
        assert cumulative_interval >= 0 and cumulative_interval <= migration_interval
        if cumulative_interval < migration_interval:
            cumulative_interval += 1
            next_states = vdi_states[-1]
            # state is the last state
        else:
            cumulative_interval = 0
            # change the state is migrated, put correspoinding vdi server into low power mode
            next_states = update_states(vdi_states, MIGRATING, MIGRATED)
    if overall_state == "migrated":
        next_states = decide_to_resume(vm_states, vdi_states, cur_sec)

    return next_states

