#!/usr/bin/env python

import ConfigParser
import math
# a simple simulator program 

SETTING = "./setting.conf"

# VM states
ORIGINAL = "original"
MIGRATING = "migrating"
MIGRATED = "migrated"
RESUMING = "resuming"

# VDI state
FULL = 4
MIGRATING = 2
S3 = 1
REINTEGRATING = 3

# global variables used in decide_to_migrate
migration_interval = 0
cumulative_interval = 0
# record all vm states when decide to migrate to 
# keep track of who are idles and who are active later
vm_states_before_migration = []
reintegration_interval = 0
cumulative_interval2 = 0

configs = {}
parser = ConfigParser.ConfigParser()
parser.optionxform=str
parser.read(SETTING)

for section in parser.sections():
    for option in parser.options(section):
        try:
            value=parser.getint(section,option)
        except ValueError:
            value=parser.get(section,option)
        configs[option] = value
        # print "Parser >>> ... ", option,value,type(value)

def state_str(s):
    if s == FULL:
        return "Full"
    elif s == MIGRATING:
        return "migrating"
    elif s == S3:
        return "S3"
    else:
        return "reintegrating"

def run(i, outf):
    global configs
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    interval = int(configs['interval'])
    vms_per_vdi = nVMs/nVDIs
    inf = open(i, "r")
    of  = open(outf,"w+")
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
    total_vdi_activeness_arr = []

    active_secs = 0

    for line in inf:
        line = line.rstrip()
        activities = line.split(",")
        vdi_activeness = []
        for i in range(0, nVDIs):
            vdi_activeness.append(0.0)
        
        if cur_sec == 0:
            for i in range(0,nVDIs):
                cur_vdi_states.append(FULL)
        else:
            cur_vdi_states = vdi_states[cur_sec -1]
        # evaluate the current situation
        for i in range(0, nVMs):
            if activities[i] == " 1":
                active_secs += 1
                vm_states[i] = 1
                vdi_activeness[i/vms_per_vdi] += (1.0/float(vms_per_vdi))
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
        total_vdi_activeness_arr.append(vdi_activeness)

    inf.close()

    o = ''
    o += "Time,"
    for v in range(0, nVDIs):
        o+="VDI%d-state,VDI%d-activeness"%(v,v)
        if v != nVDIs-1:
            o+=","
    o+= ",VDIs in Full Power, TotalVMActiveness\n"
    of.write(o)
    i = 0
    # number of seconds where activeness is greater than 0%
    for s,a in zip(vdi_states, total_vdi_activeness_arr):
        h = 14 + i / 3600
        m = i / 60 % 60
        sec = i % 60
        o = ""
        o += "%d:%d:%d, "%(h,m,sec)
        vdis_in_full = 0
        a1 = sum(a)/nVDIs
        for v in range(0, nVDIs):
            o += "%s,%f"%(state_str(s[v]),a[v])
            if s[v] != S3 :
                vdis_in_full += 1
            if v != nVDIs-1:
                o+=","
        o +=",%d,%f"%(vdis_in_full, a1)
        of.write(o+"\n")
        i += 1
    # print "total state num: %d" % len(vdi_states)
    print "Total active seconds: %d" % active_secs 
    of.write("Total active seconds: %d" % active_secs + "\n")
    of.close()
    print "Done. Result is stored in %s" % outf
    # print "Total active seconds: %d" % a1

def get_migration_interval(nActive, nIdles):
    global configs
    method = configs['method']
    interval = int(configs['interval'])
    full_migrate = float(configs['full_migrate'])
    partial_migrate = float(configs['partial_migrate'])
    s3_suspend = float(configs['s3_suspend'])
    assert full_migrate > 0
    assert partial_migrate > 0 

    #print "method is %s" % method
    if method == "partial":
        return math.ceil( ((nActive + nIdles) * partial_migrate + s3_suspend) / interval)
    elif method == "partial + full":
        ret = math.ceil( (nActive * full_migrate + nIdles * partial_migrate + s3_suspend) / interval)
        return ret
    else:
        raise Exception("Unknown migration method: %s" % method)

def get_reintegration_interval(nActive, nIdles):
    global configs
    method = configs['method']
    interval = int(configs['interval'])
    full_migrate = float(configs['full_migrate'])
    partial_resume = float(configs['partial_resume'])
    s3_resume = float(configs['s3_resume'])

    #print "method is %s" % method
    if method == "partial":
        return math.ceil( ((nActive + nIdles) * partial_resume + s3_resume) / interval)
    elif method == "partial + full":
        # active VMs are put in the remote server. don't migrate back
        return math.ceil( (nIdles * partial_resume + s3_resume) / interval)
    else:
        raise Exception("Unknown migration method: %s" % method)


# query the traces data and return the minimun number idle VMs that decide whether the vdi is migratable
def get_idle_threshold(cur_sec):
    global configs
    # FIXME: Only one stategy is implemented here: if the idle ratio is >70%
    ratio = 0.7
    
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    vms_per_vdi = nVMs / nVDIs
    
    threshold = int( vms_per_vdi * ratio )

    return threshold

# assume that migratable_servers is sorted in descending order of idleness
def decide_migrate_plan(migratable_servers, to_migrate):
    global configs
    nVDIs = int(configs['nVDIs'])
    
    slack = int(configs['slack'])
    
    # FIXME: assume all VDI servers all have 100% capacity
    # FIXME: the detailed plan as for which 
    # server migrates to which server is not decided

    assert slack == 1

    # just cut half the vdi servers
    count = 0
    for i in migratable_servers:
        if count < nVDIs / 2:
            to_migrate.append(i)
        count += 1


def decide_to_migrate(vm_states,vdi_states, cur_sec):
    global configs
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
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

    to_migrate = []
    # decide the migration plan using a strategy
    decide_migrate_plan(migratable_servers, to_migrate)

    if len(to_migrate) > 0:
        # copy the previous states first
        next_states = []
        c = 0
        for s in vdi_states[-1]:
            next_states.append(s)
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
        if i == REINTEGRATING:
            state = "reintegrating"
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
    global vm_states_before_migration
    if len(vm_states_before_migration) == 0:
        # init it first
        for i in vm_states:
            vm_states_before_migration.append(i)
    c = 0
    for i in vm_states:
        vm_states_before_migration[c] = i
        c += 1

def resume_policy(vms_awake):
    global configs
    nVDIs = int(configs['nVDIs'])
    nVMs = int(configs['nVMs'])
    vms_per_vdi = nVMs / nVDIs 
    # FIXME: only implement a policy here: any vm awake  > 5 will lead to the whole cluster to resume
    resume = False
    for i in vms_awake:
        if i > 1:
            resume = True
            break
    return resume

def decide_to_resume(vm_states, vdi_states, cur_sec):

    global configs
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
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
        c += 1
    resume = resume_policy(vms_woke_up_per_vdi)
    next_states = []
    if resume:
        next_states = update_states(vdi_states, S3, REINTEGRATING)
    else:
        for i in range(0, nVDIs):
            next_states.append(vdi_states[-1][i])

    return (next_states, resume)

# return how many idle and active vms will be migrated
def get_migrating_vdi_stats(next_states, vm_states):
    global configs
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    vms_per_vdi = nVMs / nVDIs
    nActives = 0
    nIdles = 0
    
    i = 0
    j = 0
    for i in range(0, nVDIs):
        if next_states[i] == MIGRATING:
            for j in range(0, vms_per_vdi):
                if vm_states[i*vms_per_vdi + j] == 0:
                    nIdles += 1
                else:
                    nActives += 1

    return (nActives, nIdles)
def get_reintegrating_vdi_stats(next_states, vm_states):
    global configs
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    vms_per_vdi = nVMs / nVDIs
    nActives = 0
    nIdles = 0
    
    i = 0
    j = 0
    for i in range(0, nVDIs):
        if next_states[i] == REINTEGRATING:
            for j in range(0, vms_per_vdi):
                if vm_states[i*vms_per_vdi + j] == 0:
                    nIdles += 1
                else:
                    nActives += 1

    return (nActives, nIdles)
                
def make_decision(vm_states,vdi_states, cur_sec):
    global configs    
    global migration_interval
    global cumulative_interval
    next_states = []
    overall_state = get_overall_state(vdi_states)
    
    if overall_state == "full":
        # decide whether to migrate
        (next_states, decision) = decide_to_migrate(vm_states,vdi_states, cur_sec)

        if decision == True:
            (nActives, nIdles) = get_migrating_vdi_stats(next_states, vm_states)
            # how many intervals it takes to migrate all VMs
            migration_interval = get_migration_interval(nActives, nIdles)
            assert migration_interval > 0
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
            next_states = update_states(vdi_states, MIGRATING, S3)
    if overall_state == "migrated":
        (next_states, resume) = decide_to_resume(vm_states, vdi_states, cur_sec)
        if resume == True:
            (nActives, nIdles) = get_reintegrating_vdi_stats(next_states, vm_states)
            reintegration_interval = get_reintegration_interval(nActives, nIdles)
            cumulative_interval2 = 0
    if overall_state == "reintegrating":
        assert reintegration_interva > 0
        assert  cumulative_interval2 >= 0  and cumulative_interval2 <= reintegration_interval
        if cumulative_interval2 < reintegration_interval:
            cumulative_interval2 += 1
            next_states = vdi_states[-1]
        else:
            cumulative_interval2 = 0
            next_states = update_states(vdi_states, REINTEGRATING, FULL)

    return next_states


if __name__ == '__main__':
    
    inputs = configs["inputs"]
    
    for inf in inputs.rstrip().split(","):
        if inf != '':
            outf = inf+".out2.csv"
            run(inf,outf)
