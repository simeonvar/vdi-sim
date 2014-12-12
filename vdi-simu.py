#!/usr/bin/env python

import ConfigParser
import math
import linecache
# a simple simulator program 

SETTING = "./setting.conf"
DAYS = 7

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

# current vm-vdi plan map
# by default, vms are assigned according to their index, e.g., vdi_num = vm_index / vms_per_vdi
# after migration, some vms will be assigned to another vdi server, so their vdi num will change
cur_vm_vdi_map = {}
# record the vm-vdi map changes
vm_vdi_logs = {}

configs = {}
parser = ConfigParser.ConfigParser()
parser.optionxform=str
parser.read(SETTING)

# debug print counts
print_cnt = 0
p_print_cnt = 10

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

def check_state(cur_vdi_states, next_vdi_states):
    global configs
    nVDIs = int(configs['nVDIs'])
    resume = False
    migrate = False
    cnt1 = 0
    cnt2 = 0
    for i in range(0, nVDIs):
        if cur_vdi_states[i] == FULL and next_vdi_states[i] == MIGRATING:
            cnt1 += 1
        if cur_vdi_states[i] == S3 and next_vdi_states[i] == REINTEGRATING:
            cnt2 += 1
    if cnt1 > 0 :
        migrate = True
    if cnt2 > 0 :
        resume = True
    
    return (migrate, resume)

# writing a line to the five-interval results
def output_interval(of2, vm_states, vdi_states, cur_sec):
    global configs
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    vms_per_vdi = nVMs / nVDIs
    idle_vm_consumption = float(configs['idle_vm_consumption'])
    
    line = "%d, "% cur_sec
    # format the time
    h = (14 + cur_sec / 3600) % 24
    m = cur_sec / 60 % 60
    sec = cur_sec % 60
    line += "%d:%d:%d, "%(h,m,sec)

    cur_vdi_states = vdi_states[cur_sec -1]
    has_s3 = False
    for i in range(0, nVDIs):
        if cur_vdi_states[i] == S3:
            has_s3 = True
            break
    active_vdis = 0 

    for i in range(0, nVDIs):
        if cur_vdi_states[i] != S3:
            active_vdis += 1
        # get activeness and resource consumption
        active_vm = 0
        resource = 0
        activeness = 0 
        if has_s3:              # it is consolidated state, so scan all vms 
            assert len(cur_vm_vdi_map) > 0 
            for j in range(0, nVMs):
                if cur_vm_vdi_map[j] == i:
                    if vm_states[j] >= 1:
                        active_vm += 1
                        resource += 1
                    else:
                        resource += idle_vm_consumption
            activeness = float(active_vm) / nVMs
        else:
            for j in range(0, vms_per_vdi):
                if vm_states[j + i*vms_per_vdi] >= 1:
                    active_vm += 1
                    resource += 1
                else:
                    resource += idle_vm_consumption
            activeness = float(active_vm) / vms_per_vdi
                    
        line += "%s,"%(state_str(cur_vdi_states[i])) # vdi state
        line += "%f,"% activeness
        line += "%f,"%resource
    line += "%f,"%active_vdis
    of2.write(line)

def run(i, outf):
    global configs, DAYS
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    interval = int(configs['interval'])
    vms_per_vdi = nVMs/nVDIs

    full_power = float(configs['full_power']) 
    low_power =  float(configs['low_power'])

    inf = open(i, "r")
    tmp = outf
    outf = tmp + "-by-second.csv"
    outf2 = tmp + "-by-interval.csv"

    of  = open(outf,"w+")
    of2 = open(outf2,"w+")      # five minute interval results

    # init the of2 header
    of2_header = "Time,Current Second,"
    for i in range(0, nVDIs):
        of2_header += "VDI%d-State,VDI%d-Activeness,VDI%d-Resource-Consumed,"%(i+1,i+1,i+1)
    of2_header += "VDI# in full power, total_resource_avail,len(to_migrate),to_migrate"
    of2_header += "\n"    
    of2.write(of2_header)

    # each line is one second of snapshot of NUM of desktops, 1 is active and 0 is idle
    sec_past = 0
    # current VM states in a time interval
    vm_states = []
    for i in range(0, nVMs):
        vm_states.append(0)

    cur_sec = 0                 # current second of the day, 0 ~ 24*60*60 * DAYS = 86400 * DAYS
    cur_vdi_states =[]
    # vdi states, a list of list, e.g.,  [[FULL, MIGRATING, FULL], [FULL, MIGRATING, FULL], ...]
    vdi_states = []
    total_vdi_activeness_arr = []

    active_secs = 0
    print_cnt2 = 0
    vm_active_vdi_migrated_secs = 0
    migration_times = 0
    resume_times = 0

    for line in inf:
        line = line.rstrip()

        if len(line) <= 1:
            continue
        activities = line.split(",")
        vdi_activeness = []
        for i in range(0, nVDIs):
            vdi_activeness.append(0.0)
        
        if cur_sec == 0:
            for i in range(0,nVDIs):
                cur_vdi_states.append(FULL)
        else:
            cur_vdi_states = vdi_states[cur_sec -1]

        assert len(vm_states) == nVMs

        if cur_sec % interval == 0: # only re-init it to all 0 when reaching the end of the interval
            for j in range(0, nVMs):
                vm_states[j] = 0
        # evaluate the current situation
        for i in range(0, nVMs):
            a = int(activities[i].lstrip())
            assert a >= 0
            if a >= 1:
                active_secs += 1
                vm_states[i] = 1 # assigned to 1, 0 by default
                vdi_activeness[i/vms_per_vdi] += (1.0/float(vms_per_vdi))
                if cur_vdi_states[i/vms_per_vdi] == S3:
                    vm_active_vdi_migrated_secs += 1
            # else:
            # vm_states[i] = 0
        
        if cur_sec % interval == 0 and cur_sec > 0:
            output_interval(of2, vm_states, vdi_states, cur_sec)
            # Reaching the end of the interval, time to make decision
            next_vdi_states = make_decision(vm_states, vdi_states, cur_sec, of2)
            (migrate, resume) = check_state(cur_vdi_states, next_vdi_states)
            
            of2.write("\n")
            if migrate:
                migration_times += 1
            if resume:
                resume_times += 1
            # re-init the interval value to  0
            sec_past = 1            
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
    # total time that all vdi servers running in low power states
    total_low_power_time = 0
    total_power = 0
    # number of seconds where activeness is greater than 0%
    for s,a in zip(vdi_states, total_vdi_activeness_arr):
        h = (14 + i / 3600) % 24
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
                total_power += full_power
            else:
                total_power += low_power
                total_low_power_time += 1
            if v != nVDIs-1:
                o+=","
        o +=",%d,%f"%(vdis_in_full, a1)
        of.write(o+"\n")
        i += 1
    # print "total state num: %d" % len(vdi_states)
    # print "Total active seconds: %d" % active_secs 
    # of.write("Total active seconds: %d" % active_secs + "\n")
    of.seek(0,0)                # write to the beginning
    of.write("Total power consumption: %f Joule\n" % total_power) 
    power_saving = 1 - (total_power /(86400 * DAYS * full_power * nVDIs))
    of.write("Total power saving: %f\n" % power_saving) 
    # rate of all the active seconds when there active VMs running on consolidated hosts
    rate = float(vm_active_vdi_migrated_secs)/active_secs
    of.write("Seconds when active VMs are operating on consolidated host: %d, %f of all total seconds\n"%(vm_active_vdi_migrated_secs, rate))
    of.close()
    of2.close()
    print "Done. Result is stored in %s" % outf
    # print "Total active seconds: %d" % a1

    return (power_saving, total_low_power_time, vm_active_vdi_migrated_secs, active_secs, migration_times, resume_times)

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
# query the traces and find out the probability that the idle VMs will become active at the next e.g., 20 minutes
def get_idle_probability(cur_sec):
    global configs
    traces_file = configs['traces']
    dayofweek = configs['dayofweek'] # weekday or weekend
    interval_ahead = int(configs["interval_ahead"])
    column_num = configs[dayofweek+"_"+str(interval_ahead)+"_min"] # construct the a string key e.g.,weekday_10_min 
    assert column_num > 0
    
    linenum = cur_sec / (5 * 60) + 1 # discounting first line. line start from 1
    line = linecache.getline(traces_file, linenum)
    splits = line.rstrip().split(",")
    probability = float(splits[column_num-1]) / 100 # column_num - 1 because index starts from 0
    assert probability >= 0 and probability <= 1
    return probability

def nCr(n,r):
    f = math.factorial
    return f(n) / f(r) / f(n-r)

# cdf of there will be <= n out of m events that will happen, each of which has the probability p to happen (in the case, p is the probability of an idle VM becoming active in the next, e.g., 20 minutes)
def get_cdf(p, m, n):
    if m <= n:                  # allowed more than actual idle vms. Then the probability is 1
        return 1
    ret = 0
    pow = math.pow
    for i in range(0, n+1):
        # combination
        c = nCr(m, i)
        ret += c * pow(p,i) * pow(1-p, m-i) 
    return ret

# decide whether a vdi server is migratable at the second
def is_migratable(cur_sec, idle_vms, active_vms):
    global configs, p_print_cnt

    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    vms_per_vdi = nVMs / nVDIs
    
    migration_policy_type = configs["migration_policy_type"]
    
    if migration_policy_type == "static":
        ratio = float(configs['idle_threshold'])        
        nVMs = int(configs['nVMs'])
        nVDIs = int(configs['nVDIs'])
        vms_per_vdi = nVMs / nVDIs
        
        threshold = int( (idle_vms + active_vms) * ratio )

        return idle_vms > threshold
    if migration_policy_type == "dynamic":
        interval_ahead = int(configs["interval_ahead"])
        active_vm_num_threshold = int(configs["active_vm_num_threshold"])
        active_vm_cdf_threshold = float(configs["active_vm_cdf_threshold"])
        
        cur_active_vms = vms_per_vdi - idle_vms 
        # FIXME: We do not account for current active at this moment
        # cur_active_vms = 0
        
        # how many idle vms are allowed to become active 
        idle_to_active_allowed = active_vm_num_threshold - cur_active_vms 
        if idle_to_active_allowed <= 0:
            return False        # a shortcut, current active vms are greater than our threshold. that vdi server is not migratable at all
        p = get_idle_probability(cur_sec)
        assert p >= 0 and p <= 1
        if cur_sec > 12 * 60 * 60 and p_print_cnt > 0:
            print "cur sec: %d" % cur_sec
            print "p is %f" % p
            p_print_cnt -= 1

        cdf = get_cdf(p, idle_vms, idle_to_active_allowed)
        assert cdf >= 0 and cdf <= 1
        if cur_sec > 12 * 60 * 60 and  p_print_cnt > 0:
            print "cur sec: %d" % cur_sec
            print "cdf is %f" % cdf
            p_print_cnt -= 1
        
        if cdf >= active_vm_cdf_threshold:
            return True
        else:
            return False
    else:
        raise Exception("Unknown policy type: %s" % migration_policy_type)


# return the total resource needed for this VDI
def get_resource_needed(index, vdi_idleness, vdi_activeness):
    global configs
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    vms_per_vdi = nVMs / nVDIs
    idle_vm_consumption = float(configs['idle_vm_consumption'])

    idle_vms = vdi_idleness[index]
    idle_vm_resource_needed = idle_vms * idle_vm_consumption
    active_vm_resource_needed = vdi_activeness[index]
    resource_needed = (idle_vm_resource_needed + active_vm_resource_needed)
    return resource_needed

# scanning the vm states in the cur_vm_vdi_map
def get_resource_consumed(vdi_index, cur_vm_vdi_map, vm_states):
    global configs
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    vms_per_vdi = nVMs / nVDIs

    idle_vm_consumption = float(configs['idle_vm_consumption'])
    slack = float(configs['slack'])
    tightness = float(configs['tightness'])

    rcsmd = 0
    for i in range(0, nVMs):
        if cur_vm_vdi_map[i] == vdi_index:
            rneeded = 1
            if vm_states[i] == 0: # idle vm
                rneeded = idle_vm_consumption
            rcsmd += rneeded
    return rcsmd

def decide_detailed_migration_plan(to_migrate, vdi_states, vdi_idleness, vdi_activeness,  vm_states, cur_sec, s3_flag):
    global configs
    global cur_vm_vdi_map
    global vm_vdi_logs
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    vms_per_vdi = nVMs / nVDIs

    idle_vm_consumption = float(configs['idle_vm_consumption'])
    slack = float(configs['slack'])
    tightness = float(configs['tightness'])

    # init the map first,
    # by default, the vms are assigned to vdis according to their index
    #cur_vm_vdi_map = {}
    if not s3_flag:
        for i in range(0, nVMs):
            cur_vm_vdi_map[i] = i / vms_per_vdi

    for i in to_migrate:
        # for each vm in this vdi to migrate
        # for v in range(0, vms_per_vdi):
        for v in range(0, nVMs):
            if cur_vm_vdi_map[v] != i: # all the vms residing in this host
                continue
            vm_index = v
            vm_state = vm_states[vm_index]
            rneeded = 1         # by default, active vm needs 100%
            if vm_state == 0:
                rneeded = idle_vm_consumption
            dest = -1
            # look for a dest host to migrate
            for j in range(0, nVDIs):
                if j not in to_migrate and vdi_states[-1][j] == FULL: # make sure the dest host is not a S3 host
                    rconsumed = get_resource_consumed(j, cur_vm_vdi_map, vm_states)
                    #print "rconsumed is %f" % rconsumed
                    if (rconsumed + rneeded) <= tightness*(1+slack)*vms_per_vdi:
                        dest = j
                        break
            
            if dest == -1:
                print "Bug: Couldn't find dest."
                o ="VDIs to_migrate:"
                for i in to_migrate:
                    o += "%d"%i
                    o += ","
                print o
                print "vm_index: %d" % vm_index
                total_resource_needed = 0
                total_resource_avail = 0

                for j in range(0, nVDIs):
                    if j in to_migrate:
                        print "VDI# %d is in to_migrate"% j
                        total_resource_needed += get_resource_consumed(j, cur_vm_vdi_map, vm_states)
                        continue
                    if vdi_states[-1][j] != FULL:
                        print "VDI# %d is not FULL"% j
                        continue
                    if j not in to_migrate and vdi_states[-1][j] == FULL: # make sure the dest host is not a S3 host
                        rconsumed = get_resource_consumed(j, cur_vm_vdi_map, vm_states)
                        #print "rconsumed is %f" % rconsumed
                        print "rconsumed + rneeded = %.1f"%(rconsumed + rneeded)
                        print "tightness*(1+slack)*vms_per_vdi = %.1f"%(tightness*(1+slack)*vms_per_vdi)
                        if (rconsumed + rneeded) <= tightness*(1+slack)*vms_per_vdi:
                            print "found dest: %d" % j
                        else:
                            print "VDI# %d does not have enough resources"% j
                assert False
            # update the map
            cur_vm_vdi_map[vm_index] = dest
    # put the update in the logs
    vm_vdi_logs[cur_sec]= cur_vm_vdi_map.copy()

    assert len(cur_vm_vdi_map) > 0
    assert len(vm_vdi_logs) > 0
    #print "The end of decide to migrate"
# assume that migratable_servers is sorted in descending order of idleness
def decide_what_to_migrate(migratable_servers, dest_host_num,  to_migrate, vdi_idleness, vdi_activeness, total_resource_available, s3_flag):

    # FIXME: refactor this whole chunk of init code
    global configs, cur_vm_vdi_map
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    vms_per_vdi = nVMs / nVDIs

    idle_vm_consumption = float(configs['idle_vm_consumption'])
    slack = float(configs['slack'])
    tightness = float(configs['tightness'])
        
    total_resource_needed = 0  
    for i in migratable_servers:
        total_resource_needed += get_resource_needed(i, vdi_idleness, vdi_activeness)

    while len(migratable_servers) > 0 and (total_resource_available - total_resource_needed) <= (1 - tightness) * dest_host_num * vms_per_vdi:
        last_migratable_index = migratable_servers.pop()
        # del migratable_servers[-1]
        resource = get_resource_needed(last_migratable_index, vdi_idleness, vdi_activeness)
        total_resource_needed -= resource
        total_resource_available += ((1+ slack) * vms_per_vdi - resource)
        dest_host_num += 1
    
    # to_migrate is just a copy of the rest migratable servers
    for i in migratable_servers:    
        to_migrate.append(i)

    # assert len(to_migrate) <= 1
    return (total_resource_available, dest_host_num)

def debug_print(vdi_states, vdi_idleness, vdi_activeness, cur_sec):
    global configs, cur_vm_vdi_map
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    vms_per_vdi = nVMs / nVDIs
    slack = float(configs['slack'])
    idle_vm_consumption = float(configs['idle_vm_consumption'])
    
    print "cur_sec, vdi#, state, resource consumed"
    for i in range(0, nVDIs):
        out = "%d,"%cur_sec
        out += "%d,"%i
        out += "%s,"%state_str(vdi_states[-1][i])
        idles = vdi_idleness[i]
        actives = vdi_activeness[i]
        rc = actives + idles * idle_vm_consumption
        out += "%f"%rc
        print out


# s3_flag means whether there are already consolidated host,
# if true, then we use cur_vm_vdi_map
def decide_to_migrate(vm_states,vdi_states, cur_sec, s3_flag, of2):
    global configs, cur_vm_vdi_map
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    vms_per_vdi = nVMs / nVDIs
    slack = float(configs['slack'])
    idle_vm_consumption = float(configs['idle_vm_consumption'])

    # get idleness
    vdi_idleness = {}
    vdi_activeness = {}

    if not s3_flag:
        for i in range(0, nVDIs):
            idle_vms = 0 
            for j in range(0, vms_per_vdi):
                if vm_states[i*vms_per_vdi + j] == 0:
                    idle_vms += 1
            ratio = float(idle_vms) / float(vms_per_vdi) # 
            vdi_idleness[i] = idle_vms
            vdi_activeness[i] = vms_per_vdi - idle_vms
    else:
        for i in range(0, nVDIs): # init
            vdi_idleness[i] = 0
            vdi_activeness[i] = 0
            
        for i in range(0, nVMs):
            vdi_index = cur_vm_vdi_map[i]
            assert vdi_states[-1][vdi_index] == FULL
            if vm_states[i] == 0:   # idle
                vdi_idleness[vdi_index] += 1
            if vm_states[i] >= 1:   # active
                vdi_activeness[vdi_index] += 1

    # sort the vdi server from highest to lowest idle ratio (idle VM#)
    # check if the probability is greater than the threshold
    migratable_servers = []
    total_resource_avail = 0    # total resource available for the un-migratable servers
    dest_host_num = 0
    total_active_vms = 0
    # threshold = get_idle_threshold(cur_sec) # the minimum number of idle VMs to decide whether a vdi is migratable
    for vdi_num, idle_vms in sorted(vdi_idleness.items(), key=lambda x: x[1], reverse=True):

        if s3_flag and vdi_states[-1][vdi_num] != FULL: # skip the already idle vdis
            continue

        active_vms = vdi_activeness[vdi_num] 
        total_active_vms += active_vms
        # FIXME: we don't migrate destination hosts (i.e., the hosts that have hosts others' VMs before)
        if is_migratable(cur_sec,idle_vms, active_vms) : 
            migratable_servers.append(vdi_num)
        else:
            total_resource_avail += ((slack+1) * vms_per_vdi - active_vms - idle_vms * idle_vm_consumption) 
            dest_host_num += 1

    to_migrate = []
    # decide the migration plan using a strategy
    (total_resource_avail, dest_host_num) = decide_what_to_migrate(migratable_servers, dest_host_num, to_migrate, vdi_idleness, vdi_activeness, total_resource_avail, s3_flag)

    of2.write("%f,%d,"%(total_resource_avail,len(to_migrate)))

    if len(to_migrate) > 0:
        # output the vdi num to migrate
        o = ""
        for m in to_migrate:
            o += "%d-"%m
        of2.write(o)

        decide_detailed_migration_plan(to_migrate, vdi_states,  vdi_idleness, vdi_activeness, vm_states, cur_sec, s3_flag)
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
        # debug: if total vm# < 5, then we print out the reasons why it does not migrate
        # if total_active_vms < 5: 
        # debug_print(vdi_states,vdi_idleness,vdi_activeness,cur_sec)
        # assert False

        # return the previous setting
        return (vdi_states[-1], False)

def get_overall_state(vdi_states):
    state = "full"
    for i in vdi_states[-1]:    # give priority to migrating or reintegrating
        if i == MIGRATING:
            state = "migrating"
        if i == REINTEGRATING:
            state = "reintegrating"

    if state != "full":
        return state

    for i in vdi_states[-1]:    # check the last states
        if i == S3:
            state = "migrated"

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
    resume_threshold = int(configs['resume_threshold'])
    # FIXME: only implement a policy here: any vm awake  > 5 will lead to the whole cluster to resume
    resume = False
    for i in vms_awake:
        if i > resume_threshold:
            resume = True
            break
    return resume

def decide_to_resume(vm_states, vdi_states, cur_sec, of2):

    global configs
    global cur_vm_vdi_map

    assert len(cur_vm_vdi_map) > 0
    
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    vms_per_vdi = nVMs / nVDIs

    idle_vm_consumption = float(configs['idle_vm_consumption'])
    slack = float(configs['slack'])
    tightness = float(configs['tightness'])

    # FIXME: only considers the partial migration. Full migration is not supported
    # FIXME: if Resume, then resume all of the them. I didn't differentiate one or more vdis

    # see if the capacity is exceeded 
    vdi_consumption = []
    for i in range(0, nVDIs):   # init 
        vdi_consumption.append(0)
    for i in range(0, nVMs):
        vdi_index = cur_vm_vdi_map[i]
        assert vdi_states[-1][vdi_index] != S3
        if vm_states[i] == 0:   # idle
            vdi_consumption[vdi_index] += idle_vm_consumption
        if vm_states[i] >= 1:   # active
            vdi_consumption[vdi_index] += 1

    resume = False
    vdis_to_resume = {}
    
    # scan the list and see if any vdi exceeds the capacity
    for i in range(0, nVDIs):
        if vdi_consumption[i] > vms_per_vdi * (slack + 1):
            resume = True
            vdis_to_resume[i] = True
        else:
            vdis_to_resume[i] = False

    next_states = []
    if resume:
        # next_states = update_states(vdi_states, S3, REINTEGRATING)
        last_states = vdi_states[-1]
        for v in range(0, nVDIs):
            if vdis_to_resume[v]:
                for i in range(0, nVMs):
                    vdi_index = cur_vm_vdi_map[i]
                    if vdi_index == v and (i < v * vms_per_vdi or i >= (v+1) * vms_per_vdi):
                        src_host = i / vms_per_vdi
                        # debug output, it should happen.
                        if last_states[src_host] != S3:
                            print "src_host is %d" % src_host                            
                            assert False
                        # end of debug output
                        last_states[src_host] = REINTEGRATING # bring the src host back to live
                        # update the cur map
                        for j in range(0, vms_per_vdi):
                            cur_vm_vdi_map[j+src_host * vms_per_vdi] = src_host

        # update the next_states now
        for i in range(0, nVDIs):
            next_states.append(last_states[i])
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
                
def make_decision(vm_states,vdi_states, cur_sec, of2):
    global configs    
    global migration_interval,reintegration_interval
    global cumulative_interval,cumulative_interval2
    next_states = []
    overall_state = get_overall_state(vdi_states)
    
    if overall_state == "full":
        # decide whether to migrate
        (next_states, decision) = decide_to_migrate(vm_states,vdi_states, cur_sec, False, of2)

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
        (next_states, resume) = decide_to_resume(vm_states, vdi_states, cur_sec, of2)
        if resume == True:
            (nActives, nIdles) = get_reintegrating_vdi_stats(next_states, vm_states)
            reintegration_interval = get_reintegration_interval(nActives, nIdles)
            cumulative_interval2 = 0
        else:
            # decide to whether to migrate again
            (next_states, decision) = decide_to_migrate(vm_states,vdi_states, cur_sec, True, of2)
            if decision == True:
                (nActives, nIdles) = get_migrating_vdi_stats(next_states, vm_states)
                # how many intervals it takes to migrate all VMs
                migration_interval = get_migration_interval(nActives, nIdles)
                assert migration_interval > 0
                cumulative_interval = 0
                record_vm_states(vm_states)
            
    if overall_state == "reintegrating":
        assert reintegration_interval > 0
        assert  cumulative_interval2 >= 0  and cumulative_interval2 <= reintegration_interval
        if cumulative_interval2 < reintegration_interval:
            cumulative_interval2 += 1
            next_states = vdi_states[-1]
        else:
            cumulative_interval2 = 0
            next_states = update_states(vdi_states, REINTEGRATING, FULL)

    return next_states


def run_experiment(inputs, output_str):
    cnt = 0
    tsaving = 0.0
    tcsecs = 0
    tacsecs = 0
    tasecs = 0
    tmt = 0
    trt = 0 
    for inf in inputs.rstrip().split(","):
        if inf != '':
            outf = inf+output_str
            (saving, csecs, tactive_con_secs, tactive_secs, migration_times, resume_times)  = run(inf,outf)
            tsaving += saving
            tcsecs += csecs
            tacsecs += tactive_con_secs
            tasecs += tactive_secs
            tmt += migration_times
            trt += resume_times
            cnt += 1
    ave_saving = tsaving / cnt 
    ave_consolidated_secs = tcsecs / cnt
    ave_active_vm_on_consolidated_secs = tacsecs / cnt
    ave_active_vm_secs = tasecs / cnt
    ave_migration_times = tmt / cnt
    ave_resume_times = trt /cnt
    return (ave_saving, ave_consolidated_secs,\
            ave_active_vm_on_consolidated_secs, ave_active_vm_secs,\
            ave_migration_times, ave_resume_times)


if __name__ == '__main__':

    policy_type = configs['migration_policy_type']
    
    if policy_type == "static":
        
        of = "data/static-all-result.csv"
        f = open(of, "w+")
        header =  "Idle threshold, Resume threshold(aVM#), Policy,"
        header += "Power Saving(wd), Consol.Time(wd), Active Consol. VMs Time(wd), Ttl.Active.Time(wd), Migration#, Resume#,"
        header += "Power Saving(we), Consol.Time(we), Active Consol. VMs Time(we), Ttl.Active.Time(we), Migration#, Resume#,\n"
        f.write(header)
        rts = configs['resume_thresholds'].rstrip().split(",")
        its = configs['idle_thresholds'].rstrip().split(",")
        sts = configs['slacks'].rstrip().split(",")
        tts = configs['tightnesses'].rstrip().split(",")
        i = 0 
        for i in range(0, len(rts)):
            configs['resume_threshold'] = int(rts[i])
            configs['idle_threshold'] = float(its[i])
            configs['slack'] = float(sts[i])
            configs['tightness'] = float(tts[i])

            configs["dayofweek"] = "weekday"            
            inputs = configs["inputs-weekday"]
            output_postfix = ".out-static-%.1f-%.1f-%.1f"%(float(its[i]), float(sts[i]), float(tts[i]))
            (ave_weekday_saving, ave_weekday_consolidated_secs, \
             ave_weekday_active_vm_on_consolidated_secs,ave_weekday_active_vm_secs,\
             ave_weekday_migration_times, ave_weekday_resume_times) = run_experiment(inputs, output_postfix)
            oline = "%.1f,%d,"%(configs['idle_threshold'],configs['resume_threshold'])
            oline += "idle_threshold=%.1f tightness=%.1f,"%(configs['idle_threshold'],configs['tightness'])

            oline += "%f, %d, %d, %d, %d, %d\n"% (ave_weekday_saving, ave_weekday_consolidated_secs/3600, ave_weekday_active_vm_on_consolidated_secs/3600,ave_weekday_active_vm_secs/3600,ave_weekday_migration_times,ave_weekday_resume_times)
            f.write(oline)
            
        f.close()
        print "Done. Result is in %s"%of

    # a hack to let it run on dynamic policies
    # policy_type = "dynamic"
    configs['migration_policy_type'] = "dynamic"    

    if policy_type == "dynamic":
        
        of = "data/dynamic-all-result.csv"
        f = open(of, "w+")
        header = "Active VM# threshold, CDF, Resume threshold(aVM#), Policy,"
        header += "Power Saving(wd), Consol.Time(wd), Active Consol. VMs Time(wd), Ttl.Active.Time(wd), Migration#, Resume#"
        header += "Power Saving(we), Consol.Time(we), Active Consol. VMs Time(we), Ttl.Active.Time(we), Migration#, Resume#\n"

        f.write(header)
        rts = configs['resume_thresholds_dynamic'].rstrip().split(",")
        avs = configs['active_vm_num_thresholds'].rstrip().split(",")
        cdfs = configs['active_vm_cdf_thresholds'].rstrip().split(",")
        sts = configs['slacks'].rstrip().split(",")
        tts = configs['tightnesses'].rstrip().split(",")

        i = 0 
        for i in range(0, len(rts)):
            configs['active_vm_num_threshold'] = int(avs[i])
            configs['active_vm_cdf_threshold'] = float(cdfs[i])
            configs['resume_threshold'] = int(rts[i])
            configs['slack'] = float(sts[i])
            configs['tightness'] = float(tts[i])
            
            inputs = configs["inputs-weekday"]
            configs["dayofweek"] = "weekday"
            output_postfix = ".out-dynamic-slack%.1f-tightness%.1f-%d-%.1f"%(float(sts[i]), float(tts[i]), int(avs[i]),float(cdfs[i]))

            (ave_weekday_saving, ave_weekday_consolidated_secs, \
             ave_weekday_active_vm_on_consolidated_secs,ave_weekday_active_vm_secs,\
             ave_weekday_migration_times, ave_weekday_resume_times) = run_experiment(inputs, output_postfix)
            
            # dealing with weekend
            inputs = configs["inputs-weekend"]
            configs["dayofweek"] = "weekend"
            (ave_weekend_saving, ave_weekend_consolidated_secs, \
             ave_weekend_active_vm_on_consolidated_secs, ave_weekend_active_vm_secs,\
             ave_weekend_migration_times, ave_weekend_resume_times) = run_experiment(inputs, output_postfix)

            oline = "%d,%f,%d,"%(configs['active_vm_num_threshold'], configs['active_vm_cdf_threshold'],configs['resume_threshold'])
            oline += "active_vm=%d cdf=%.1f slack=%.1f tightness=%.1f,"%(configs['active_vm_num_threshold'],configs['active_vm_cdf_threshold'], configs['slack'], configs['tightness'])

            oline += "%f, %d, %d, %d, %d, %d,"% (ave_weekday_saving, ave_weekday_consolidated_secs/3600, ave_weekday_active_vm_on_consolidated_secs/3600,ave_weekday_active_vm_secs/3600,ave_weekday_migration_times,ave_weekday_resume_times)
            oline += "%f, %d, %d, %d, %d, %d \n"% (ave_weekend_saving, ave_weekend_consolidated_secs/3600,ave_weekend_active_vm_on_consolidated_secs/3600,ave_weekend_active_vm_secs/3600,ave_weekend_migration_times,ave_weekend_resume_times)

            f.write(oline)

        f.close()
        print "Done. Result is in %s"%of














