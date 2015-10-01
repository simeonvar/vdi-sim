#!/usr/bin/env python

import ConfigParser
import math
import linecache, time
# a simple simulator program 

target_timestamp = 345600

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

prev_vms = []                   # vms at the previous interval

# vm object, that stores vm state, the original and current host. 
# If origin != curhost, it indicates that it is a remote vms. Ir
class vm:
    origin = -1
    curhost = -1
    state = -1
    def __init__(self, origin, curhost, state):
        self.state = state
        self.curhost = curhost
        self.origin = origin
    def isOriginHost(self, vdi_index):
        return self.origin == vdi_index
    def isCurHost(self, vdi_index):
        return self.curhost == vdi_index

# current vm-vdi plan map
# by default, vms are assigned according to their index, e.g., vdi_num = vm_index / vms_per_vdi
# after migration, some vms will be assigned to another vdi server, so their vdi num will change

# global vm array that stores the vm information
vms = []

# record the vm-vdi map changes
vm_vdi_logs = {}

configs = {}
parser = ConfigParser.ConfigParser()
parser.optionxform=str
parser.read(SETTING)

# debug print counts
print_cnt = 0
p_print_cnt = 0

for section in parser.sections():
    for option in parser.options(section):
        try:
            value=parser.getint(section,option)
        except ValueError:
            value=parser.get(section,option)
        configs[option] = value
        # print "Parser >>> ... ", option,value,type(value)

# policy parameters
nVMs = int(configs['nVMs'])
nVDIs = int(configs['nVDIs'])
vms_per_vdi = nVMs / nVDIs
idle_vm_consumption = float(configs['idle_vm_consumption'])
interval = int(configs['interval'])
full_power = float(configs['full_power']) 
low_power =  float(configs['low_power'])
method = configs['method']
interval = int(configs['interval'])
full_migrate = float(configs['full_migrate'])
partial_migrate = float(configs['partial_migrate'])
s3_suspend = float(configs['s3_suspend'])
traces_file = configs['traces']
interval_ahead = int(configs["interval_ahead"])
partial_resume = float(configs['partial_resume'])
s3_resume = float(configs['s3_resume'])

idle_threshold = float(configs['idle_threshold'])
resume_threshold = int(configs['resume_threshold'])
migration_policy_type = configs["migration_policy_type"]
slack = float(configs['slack'])
tightness = float(configs['tightness'])
dayofweek = configs['dayofweek'] # weekday or weekend
# end of policy parameters


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

# update the origins. Assuming all active VMs will be moved to the cur host
def update_vms(of4, cur_sec, vm_states):
    global prev_vms, vms
    global vms_per_vdi, idle_vm_consumption
    idle_turn_into_active = 0
    # clear prev_vms
    del prev_vms[:]
    prev_vms = []
    (h,m,sec) = format_cur_time(cur_sec)
    for i in range(0, nVMs):
        v = vms[i]
        prev_vms.append(vm(v.origin,v.curhost,v.state))
        if vms[i].state < 1 and vm_states[i] >=1: # idle to transitions
            line = "%d,"% cur_sec
            line += "%d,"%(i)
            line += "%d,%d,"% (vms[i].origin, vms[i].curhost)
            cur_host = vms[i].curhost
            idles_in_curhost = 0
            actives_in_curhost = 0
            for j in range(0, nVMs): # get how idle and active VMs in the current hosts
                if vms[j].curhost == cur_host:
                    if vm_states[j] >= 1:
                        actives_in_curhost += 1
                    else:
                        idles_in_curhost += 1
            line += "%d,%d,"% (idles_in_curhost, actives_in_curhost)
            resource_consumption = idles_in_curhost * idle_vm_consumption + actives_in_curhost
            exceed_capacity = "N"
            capacity = vms_per_vdi
            if resource_consumption > capacity:
                exceed_capacity = "Y"
            line += "%.1f,%s\n"%(resource_consumption, exceed_capacity)
            of4.write(line)
            
    for i in range(0, nVMs):
        vms[i].state = vm_states[i]
        if vm_states[i] >= 1 and vms[i].curhost != vms[i].origin: # remote host turned into origin host
        #    vms[i].origin = vms[i].curhost
            idle_turn_into_active += 1

    return idle_turn_into_active
# format the time
def format_cur_time(cur_sec):
    h = (14 + cur_sec / 3600) % 24
    m = cur_sec / 60 % 60
    sec = cur_sec % 60
    return (h,m,sec)

# writing a line to the five-interval results
def output_interval(of2, vm_states, vdi_states, cur_sec):
    line = "%d, "% cur_sec
    (h,m,sec) = format_cur_time(cur_sec)
    line += "%d:%d:%d, "%(h,m,sec)
    cur_vdi_states = vdi_states[-1]
    active_vdis = 0
    total_active_vms = 0
    for i in range(0, nVDIs):
        if cur_vdi_states[i] != S3:
            active_vdis += 1
        # get activeness and resource consumption
        active_vm = 0
        resource = 0
        activeness = 0
        assert len(vms) == nVMs
        for j, v in enumerate(vms):
            if v.curhost == i:
                if v.state >= 1:
                    active_vm += 1
                    resource += 1
                else:
                    resource += idle_vm_consumption
        activeness = float(active_vm) / nVMs
                    
        line += "%s,"%(state_str(cur_vdi_states[i])) # vdi state
        line += "%f,"% activeness
        line += "%f,"%resource
        total_active_vms += active_vm
    line += "%f,"%active_vdis
    line += "%d,"%total_active_vms
    of2.write(line)

def output_target_timestamp(cur_sec, vdi_states, result_file):
    if True:                    # I'm lazy to fix the block's indent
        if True:
            if cur_sec == target_timestamp or cur_sec == target_timestamp + 2 * interval or cur_sec == target_timestamp - interval:
                # report the results
                rf = open(result_file, "a+")
                # look at the VDI servers and their states
                line = "Overall state: %s\n" % get_overall_state(vdi_states)
                rf.write(line)
                line = "VDI#,"
                for i in range(0, nVDIs):
                    line += "%d,"%i
                line += '\n'
                rf.write(line)

                line = "State,"
                for i in range(0, nVDIs):
                    cur_vdi_states = vdi_states[-1]
                    line += "%s,"%(state_str(cur_vdi_states[i])) # vdi state
                line += '\n'
                rf.write(line)
                # vdi as the current host
                vdi_curhost = {}
                vdi_curhost_idle = {}
                vdi_curhost_active = {}
                vdi_origin = {}
                for i in range(0, nVDIs):
                    vdi_curhost[i] = 0
                    vdi_origin[i] = 0
                    vdi_curhost_idle[i] = 0
                    vdi_curhost_active[i] = 0
                for i in range(0, nVMs):
                    v = vms[i]
                    vdi_curhost[v.curhost] += 1
                    if v.state == 0:
                        vdi_curhost_idle[v.curhost] += 1
                    else:
                        vdi_curhost_active[v.curhost] += 1
                    vdi_origin[v.origin] += 1
                line = "Current host,"
                for i in range(0, nVDIs):
                    line += "%d,"%vdi_curhost[i]
                line += '\n'
                rf.write(line)

                line = "Current hosting idle VMs,"
                for i in range(0, nVDIs):
                    line += "%d,"%vdi_curhost_idle[i]
                line += '\n'
                rf.write(line)

                line = "Current hosting active VMs,"
                for i in range(0, nVDIs):
                    line += "%d,"%vdi_curhost_active[i]
                line += '\n'
                rf.write(line)

                line = "Origin host,"
                for i in range(0, nVDIs):
                    line += "%d,"%vdi_origin[i]
                line += '\n'
                rf.write(line)
                rf.close()


def output_migration_maps(of3, cur_sec, full_migrations, partial_migrations, resume_migrations, post_partial_migrations):
    (h,m,sec) = format_cur_time(cur_sec)

    # a list that records all pairs of list
    migration_pair_list = []
    for pair in full_migrations: 
        migration_pair_list.append(pair) # the list is empty anyway
    for pair in partial_migrations:
        if pair not in migration_pair_list:
            migration_pair_list.append(pair)
    for pair in resume_migrations:
        if pair not in migration_pair_list:
            migration_pair_list.append(pair)
    for pair in post_partial_migrations:
        if pair not in migration_pair_list:
            migration_pair_list.append(pair)
    for pair in migration_pair_list:
        full_cnt = 0
        partial_cnt = 0 
        resume_cnt = 0
        post_cnt = 0
        full_migrated_vms = ""
        partial_migrated_vms = ""
        resume_vms = ""
        post_migrated_vms = ""
        if pair in full_migrations:
            full_cnt = full_migrations[pair][0]
            full_migrated_vms = full_migrations[pair][1]
        if pair in partial_migrations:
            partial_cnt = partial_migrations[pair][0]
            partial_migrated_vms = partial_migrations[pair][1]
        if pair in resume_migrations:
            resume_cnt = resume_migrations[pair][0]
            resume_vms = resume_migrations[pair][1]
        if pair in post_partial_migrations:
            post_cnt = post_partial_migrations[pair][0]
            post_migrated_vms = post_partial_migrations[pair][1]
        line = "%d, "% cur_sec
        line += "%d:%d:%d, "%(h,m,sec)
        line += "%d,%d,"%pair
        line += "%d,%s,%d,%s,%d,%s,%d,%s\n"%(full_cnt, full_migrated_vms, partial_cnt, partial_migrated_vms, resume_cnt, resume_vms, post_cnt, post_migrated_vms)
        of3.write(line)

def run(inf, outf):

    inf = open(inf, "r")
    tmp = outf
    outf = tmp + "-by-second.csv"
    outf2 = tmp + "-by-interval.csv"
    outf3 = tmp + "-migrations-by-interval.csv"
    outf4 = tmp + "-idle-to-active-transitions-by-interval.csv"
    outf5 = tmp + "-just-go-to-sleep-and-then-wake-up-vdis.csv"

    of  = open(outf,"w+")
    of2 = open(outf2,"w+")      # five minute interval results
    of3 = open(outf3,"w+")      # five minute interval results
    of4 = open(outf4, "w+")
    of5 = open(outf5, "w+")

    # init the of2 heade
    of2_header = "Time, Current Second,"
    for i in range(0, nVDIs):
        of2_header += "VDI%d-State,VDI%d-Active VM#,VDI%d-Resource-Consumed,"%(i+1,i+1,i+1)
    of2_header += "VDI# in Full Power %.1f, Total Active VM#, Partial Migration#, Full Migration#, Partial Reintegration#, Post-partial Migration#, Total Idle to Active VM#"%(1-tightness)
    of2_header += "\n"    
    of2.write(of2_header)

    of3_header = "Current Second,Time,Source Host,Destination Host,Full Migration Number, Full Migrated VMs, Partial Migrations Number, Partial Migrated VMs, Reintegration Number, Reintegrated VMs, Post-partial Migrated VMs\n"
    of3.write(of3_header)

    of4_header = "Current Second, VM index, Source Host, Current Host, Current Host idle VMs, Current Host Active VMs, Current Host Resource Consumption, Exceeding Capacity(Y/N) \n"
    of4.write(of4_header)

    of5_header = "Current Second, VDIs\n"
    of5.write(of5_header)

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
    total_full_migration_times = 0
    remote_partial_vm_into_active = 0
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

        # init vms array
        if cur_sec == 0:
            del vms[:]
            for j in range(0, nVMs):
                origin = j / vms_per_vdi
                vms.append(vm(origin, origin, 0))

        assert len(vms) == nVMs
        # if cur_sec % interval == 0: # only re-init it to all 0 when reaching the end of the interval

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
            
        if cur_sec % interval == 0 and cur_sec > 0:
            # update global variable vms state and origin hosts
            itoactive = update_vms(of4, cur_sec, vm_states)
            remote_partial_vm_into_active += itoactive
            result_file = outf + "-before-%d.csv"%cur_sec
            output_target_timestamp(cur_sec, vdi_states, result_file)
            output_interval(of2, vm_states, vdi_states, cur_sec)
            # Reaching the end of the interval, time to make decision
            (next_vdi_states, partial_migration_times, full_migration_times, partial_resume_times, full_migrations, partial_migrations, resume_migrations, post_partial_migrations) = make_decision(vm_states, vdi_states, cur_sec, of2, of5)
            (migrate, resume) = check_state(cur_vdi_states, next_vdi_states)
            of2.write("%d,%d,%d,%d,%d"%(len(partial_migrations),len(full_migrations),len(resume_migrations),len(post_partial_migrations),itoactive))
            of2.write("\n")
            migration_times += len(partial_migrations)
            resume_times += len(resume_migrations)
            total_full_migration_times += len(full_migrations)
            vdi_states.append(next_vdi_states)

            output_migration_maps(of3, cur_sec, full_migrations, partial_migrations, resume_migrations, post_partial_migrations)
            result_file = outf + "-after-%d.csv"%cur_sec
            output_target_timestamp(cur_sec, vdi_states,result_file)
            # re-init the vm_states to 0
            for j in range(0, nVMs):
                vm_states[j] = 0
        else:
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
    # of.write(o)
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
        # of.write(o+"\n")
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
    of3.close()
    of4.close()
    print "Done. Result is stored in %s" % outf
    # print "Total active seconds: %d" % a1

    return (power_saving, total_low_power_time, vm_active_vdi_migrated_secs, active_secs, migration_times, resume_times, total_full_migration_times, remote_partial_vm_into_active)

def get_migration_interval(full_migrations, partial_migrations, post_partial_migrations):
    
    latency = 0
    for k,v in partial_migrations.iteritems():
        latency += v[0] * partial_migrate
    for k,v in full_migrations.iteritems():
        latency += v[0] * full_migrate
    for k,v in post_partial_migrations.iteritems():
        latency += v[0] * full_migrate
    # migration_interval = math.ceil( float(latency)/interval )     
    migration_interval = 1
    return migration_interval 

def get_reintegration_interval(partial_migrations, full_migrations, post_partial_migrations, resume_migrations):
    latency = 0
    for k,v in partial_migrations.iteritems():
        latency += v[0] * partial_migrate
    for k,v in full_migrations.iteritems():
        latency += v[0] * full_migrate
    for k,v in post_partial_migrations.iteritems():
        latency += v[0] * full_migrate
    for k,v in resume_migrations.iteritems():
        latency += v[0] * partial_resume
    migration_interval = math.ceil( float(latency)/interval )     
    if migration_interval <= 0:
        print "Somewrong here" 
        # assert False
    return migration_interval

# query the traces and find out the probability that the idle VMs will become active at the next e.g., 20 minutes
def get_idle_probability(cur_sec):
    
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
    
    assert migration_policy_type == "static"

    if migration_policy_type == "static":
        
        threshold = int( (idle_vms + active_vms) * idle_threshold )
        return idle_vms >= threshold
    else:
        raise Exception("Unknown policy type: %s" % migration_policy_type)


# return the total resource needed for this VDI
def get_resource_needed(index, vdi_idleness, vdi_activeness):

    idle_vms = vdi_idleness[index]
    idle_vm_resource_needed = idle_vms * idle_vm_consumption
    active_vm_resource_needed = vdi_activeness[index]
    resource_needed = (idle_vm_resource_needed + active_vm_resource_needed)
    return resource_needed

# scanning the vm states in the vms array
def get_resource_consumed(vdi_index,vms_copy):

    rcsmd = 0
    for i,v in enumerate(vms_copy):
        if v.curhost == vdi_index:
            rneeded = 1
            if v.state == 0: # idle vm
                rneeded = idle_vm_consumption
            rcsmd += rneeded
    return rcsmd

def decide_detailed_migration_plan(to_migrate, vdi_states, vdi_idleness, vdi_activeness, cur_sec, s3_flag):
    global configs
    global vms
    global vm_vdi_logs
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    vms_per_vdi = nVMs / nVDIs

    partial_migrate_times = 0
    full_migrate_times = 0
    
    # below two maps to keep track of the detailed migrations from src to dest
    full_migrations = {}  # use (src,dest) as the key to keep track of the full migrations
    partial_migrations = {} # only keep track of partial migrations times for each pair of src and host

    idle_vm_consumption = float(configs['idle_vm_consumption'])
    slack = float(configs['slack'])
    tightness = float(configs['tightness'])

    # dest queue length. We try to select the migration host that has the shortest queue
    dest_queue = {}
    for host in range(nVDIs):
        if host not in to_migrate and vdi_states[-1][host] == FULL: # make sure the dest host is not a S3 host
            dest_queue[host] = 0

    vms_copy = []
    # copy the whole thing to a copy
    for v in vms:
        vms_copy.append(vm(v.origin,v.curhost,v.state))

    for i in to_migrate:
        migration_latency = 0   # cumulative migration latency needed. If exceeds, then we stop the migration 
        # for each vm in this vdi to migrate
        for v in range(0, nVMs):
            if vms[v].curhost != i: # all the vms residing in this host
                continue
            vm_index = v
            vm_state = vms[vm_index].state
            rneeded = 1         # by default, active vm needs 100%
            latency_needed = 0
            if vm_state == 0:
                rneeded = idle_vm_consumption
                latency_needed += partial_migrate
            else:
                latency_needed += full_migrate
            dest = -1
            # find the destination that 
            for key in sorted(dest_queue, key=lambda k: dest_queue[k]):
                host = key
                queue_length = dest_queue[host]
                rconsumed = get_resource_consumed(host, vms_copy)
                if ((rconsumed + rneeded) <= tightness*(1+slack)*vms_per_vdi ) and (queue_length + latency_needed) <= interval:
                    dest = host
                    dest_queue[host] += latency_needed
                    if dest_queue[host] > (migration_latency + latency_needed): # the destination latency is the bottleneck
                        migration_latency = dest_queue[host] 
                    else:
                        migration_latency += latency_needed
                    break

            if dest == -1:
                return (False,0,0,{},{})

            # if the migration latency has exceeds an interval, then stop dealing with any VMs belonging to this host
            if migration_latency > interval:
                break

            # update the map
            assert vms_copy[vm_index].curhost != dest
            src = vms_copy[vm_index].curhost
            vms_copy[vm_index].curhost = dest
            migration_pair = (src,dest)
            # We fully migrate the active VMs
            if vm_state >= 1:
                vms_copy[vm_index].origin = dest
                full_migrate_times += 1
                if migration_pair in full_migrations:
                    full_migrations[migration_pair][0] += 1
                    full_migrations[migration_pair][1] += ("-"+str(vm_index))
                else:
                    full_migrations[migration_pair] = [1,str(vm_index)]
            else:

                partial_migrate_times += 1
                if migration_pair in partial_migrations:
                    cur_migration_times = partial_migrations[migration_pair][0]
                    partial_migrations[migration_pair][0] = cur_migration_times + 1
                    partial_migrations[migration_pair][1] += ("-"+str(vm_index))
                else:
                    partial_migrations[migration_pair] = [1,str(vm_index)]
                
    # update the vms 
    del vms[:]
    vms = vms_copy[:]

    assert len(vms) > 0
    return (True, partial_migrate_times, full_migrate_times,full_migrations,partial_migrations)

# assume that migratable_servers is sorted in descending order of idleness
def decide_what_to_migrate(migratable_servers, dest_host_num,  to_migrate, vdi_idleness, vdi_activeness, total_resource_available, s3_flag):

    total_resource_needed = 0  
    for i in migratable_servers:
        total_resource_needed += get_resource_needed(i, vdi_idleness, vdi_activeness)

    while len(migratable_servers) > 0 and (total_resource_available - total_resource_needed) <= (1 - tightness) * dest_host_num * vms_per_vdi:
        last_migratable_index = migratable_servers.pop()

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
# if true, then we use vms
def decide_to_migrate(vdi_states, cur_sec, s3_flag, of2):

    # get idleness
    vdi_idleness = {}
    vdi_activeness = {}

    partial_migrate_times = 0
    full_migrate_times = 0
    
    full_migrations ={}
    partial_migrations = {}
    resume_migrations = {}

    for i in range(0, nVDIs):
        vdi_idleness[i] = 0
        vdi_activeness[i] = 0

    migration_latency_needed = {}

    # get vdi idleness and activeness according to the curhost of the 
    for v in vms:
        vdi_index = v.curhost
        if vdi_index not in migration_latency_needed:
            migration_latency_needed[vdi_index] = 0 

        assert vdi_index >= 0 and vdi_index < nVDIs
        if v.state == 0:
            vdi_idleness[vdi_index] += 1
            migration_latency_needed[vdi_index] += partial_migrate
        else:
            vdi_activeness[vdi_index] += 1
            migration_latency_needed[vdi_index] += full_migrate
        
    assert len(migration_latency_needed) <= nVDIs
    for vdi_num in migration_latency_needed:
        assert vdi_states[-1][vdi_num] != S3
        
    # sort the vdi server from lowest to highest migration latency need to migrate them
    migratable_servers = []
    total_resource_avail = 0    # total resource available for the un-migratable servers
    dest_host_num = 0
    total_active_vms = 0
    # threshold = get_idle_threshold(cur_sec) # the minimum number of idle VMs to decide whether a vdi is migratable
    for vdi_num, migration_latency in sorted(migration_latency_needed.items(), key=lambda x: x[1]):
        migratable_servers.append(vdi_num)

    to_migrate = []
    # decide the migration plan using a strategy
    (total_resource_avail, dest_host_num) = decide_what_to_migrate(migratable_servers, dest_host_num, to_migrate, vdi_idleness, vdi_activeness, total_resource_avail, s3_flag)

    # of2.write("%f,%d,"%(total_resource_avail,len(to_migrate)))

    if len(to_migrate) > 0:
        # output the vdi num to migrate
        o = ""
        for m in to_migrate:
            o += "%d-"%m
        # of2.write(o)

        # there is a chance that we will never find a detailed plan to fit all the vms
        # in that case, we simply don't migrate them
        truly_migratable = False
        (truly_migratable, partial_migrate_times, full_migrate_times, full_migrations, partial_migrations) = decide_detailed_migration_plan(to_migrate, vdi_states,  vdi_idleness, vdi_activeness, cur_sec, s3_flag)
        # copy the previous states first
        next_states = []
        c = 0
        for s in vdi_states[-1]:
            next_states.append(s)
            c += 1

        # only update the server whose vms have been evacuated
        if truly_migratable:
            for i in to_migrate:
                evacuated = True
                for v in range(nVMs):
                    if vms[v].curhost == i:
                        evacuated = False
                        break
                if evacuated:
                    next_states[i] = MIGRATING
        return (next_states, truly_migratable, partial_migrate_times, full_migrate_times, full_migrations, partial_migrations)
    else:
        # return the previous setting
        return (vdi_states[-1], False, partial_migrate_times, full_migrate_times, full_migrations, partial_migrations)

def phv(vms_copy=None):
    global vms
    if vms_copy == None:
        vms_copy = vms
    total_vms = 0
    print "Host,Current Hosting VMs"
    hosts = {}
    for i in range(nVDIs):
        hosts[i] = 0
    for i in range(nVMs):
        hosts[vms_copy[i].curhost] += 1
    for i in range(nVDIs):
        print "%d,%d"%(i, hosts[i])
        total_vms += hosts[i]
    print "Total VMs in recorded hosts: %d" % total_vms

def get_overall_state(vdi_states):
    state = "full"
    for i in vdi_states[-1]:    # give priority to migrating or reintegrating
        if i == MIGRATING:
            state = "migrating"
            break
        if i == REINTEGRATING:
            state = "reintegrating"
            break

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

def record_migration(vm_index, src, dest, migrations):
    i = vm_index
    migration_pair = (src, dest)
    if migration_pair in migrations:
        migrations[migration_pair][0] += 1
        migrations[migration_pair][1] += ("-"+str(i))
    else:
        migrations[migration_pair] = [1,str(i)]    

def try_to_allocate(vdi_set, vms_copy, last_states):
    allocatable = False
    vdi_capacity = vms_per_vdi * ( 1 + slack )
    partial_migrations = {}
    reintegrations = {}
    full_migrations ={}
    post_partial_migrations = {}

    # if there is a newly woke-up vdi server, then resume all of its partial replica and see if the problem is solved
    for i in vdi_set:
        if last_states[i] == S3:
            #for v in vms_copy:
            for j in range(nVMs):
                v = vms_copy[j]
                if v.origin == i:
                    src = v.curhost
                    dest = v.origin
                    record_migration(j, src, dest, reintegrations)
                    v.curhost = i
    # get the vdi consumption of each vdi
    vdi_consumption = get_vdi_consumption(vms_copy)    

    for i in vdi_set:
        while vdi_consumption[i] > vdi_capacity:
            # try to kick out the remote idle vms first
            local_idle_vms = []
            remote_partials_remaining_idle = []
            active_vms = []
            #for v in vms_copy:
            for j in range(nVMs):
                v = vms_copy[j]
                if v.curhost == i and v.state == 0 and v.origin == i:
                    local_idle_vms.append(v)
                elif v.curhost == i and v.state != 0 and v.origin == i:
                    active_vms.append(v)
                elif v.curhost == i and v.state == 0 and v.origin != i:
                    remote_partials_remaining_idle.append(v)
                elif v.curhost == i and v.state != 0 and v.origin != i: # remote partials that become active
                    # find dest.
                    for k in vdi_set:
                        if i == k:
                            continue
                        else:
                            vm_consumption = 1                   # it is active
                            if vdi_consumption[k] + vm_consumption <= vdi_capacity:
                                record_migration(j, v.curhost, k, partial_migrations)
                                v.curhost = k
                                record_migration(j, v.origin, k, post_partial_migrations)
                                v.origin = k # !!! update its origin, do post-partial migrations! 
                                vdi_consumption[k] += vm_consumption
                                vdi_consumption[i] -= vm_consumption
                                break
                    else:
                        # meaning we have not found k that hosts the v
                        allocatable = False
                        return (allocatable, {}, {}, {}, {})
                    # here means the loop has found k, then test whether there is still overloaded vdis
                    if vdi_consumption[i] <= vdi_capacity:
                        break

            if vdi_consumption[i] > vdi_capacity:
                # now to kick out the remote partials vms that remain idle
                for v in remote_partials_remaining_idle:
                    vm_index = vms_copy.index(v)
                    # find dest.
                    for k in vdi_set:
                        if i == k:
                            continue
                        else:
                            # see if it fits
                            if vdi_consumption[k] + idle_vm_consumption <= vdi_capacity:
                                record_migration(vm_index, v.curhost, k, partial_migrations)
                                v.curhost = k
                                vdi_consumption[k] += idle_vm_consumption
                                vdi_consumption[i] -= idle_vm_consumption
                                break
                    else:
                        # meaning we have not found k that hosts the v
                        allocatable = False
                        return (allocatable, {}, {}, {}, {})
                    # here means the loop has found k, then test whether there is still overloaded vdis
                    if vdi_consumption[i] <= vdi_capacity:
                        break

            if vdi_consumption[i] > vdi_capacity:
                # now to kick out the local idle vms
                for v in local_idle_vms:
                    vm_index = vms_copy.index(v)
                    # find dest.
                    for k in vdi_set:
                        if i == k:
                            continue
                        else:
                            # see if it fits
                            if vdi_consumption[k] + idle_vm_consumption <= vdi_capacity:
                                record_migration(vm_index, v.curhost, k, partial_migrations)
                                v.curhost = k
                                vdi_consumption[k] += idle_vm_consumption
                                vdi_consumption[i] -= idle_vm_consumption
                                break
                    else:
                        # meaning we have not found k that hosts the v
                        allocatable = False
                        return (allocatable, {}, {}, {}, {})
                    # here means the loop has found k, then test whether there is still overloaded vdis
                    if vdi_consumption[i] <= vdi_capacity:
                        break

            if vdi_consumption[i] > vdi_capacity:
                 # now to kick out the active vms
                for v in active_vms:
                    vm_index = vms_copy.index(v)
                    # find dest.
                    for k in vdi_set:
                        if i == k:
                            continue
                        else:
                            # see if it fits
                            if vdi_consumption[k] + 1 <= vdi_capacity:
                                record_migration(vm_index, v.curhost, k, full_migrations)
                                v.curhost = k
                                v.origin = k
                                vdi_consumption[k] += 1
                                vdi_consumption[i] -= 1
                                break
                    else:
                        # meaning we have not found k that hosts the v
                        allocatable = False
                        return (allocatable, {}, {}, {}, {})
                    # here means the loop has found k, then test whether there is still overloaded vdis
                    if vdi_consumption[i] <= vdi_capacity:
                        break
                else:
                    print "This is unlikely because evacuating all active VMs should make vdi_consumption never exceed the vdi capacity"
                    assert False
    else:                       
        # loop exits normally, then
        allocatable = True

    return (allocatable, partial_migrations, reintegrations, full_migrations, post_partial_migrations)

# get the vdi_consumption based on the vms_copy. 
def get_vdi_consumption (vms_copy):
    vdi_consumption = []
    # see if the capacity is exceeded 
    for i in range(0, nVDIs):   # init 
        vdi_consumption.append(0)
    for i in range(0, nVMs):
        vdi_index = vms_copy[i].curhost
        # We do not check whether currently the vdi state is S3 or not, because this function can be called in a hypothetical case that it just newly wake up a few servers
        # assert last_states[vdi_index] != S3  
        if vms_copy[i].state == 0:   # idle
            vdi_consumption[vdi_index] += idle_vm_consumption
        if vms_copy[i].state >= 1:   # active
            vdi_consumption[vdi_index] += 1
    return vdi_consumption

# only update the previous S3 state to reintegrating if any VDIs are woken up
def get_next_states(last_states):
    global vms
    next_states = []
    for i in range(nVDIs):
        if last_states[i] == S3:
            state_to_append = S3
            for v in vms:
                if v.curhost == i:
                    state_to_append = REINTEGRATING
                    break
            next_states.append(state_to_append)
        else:
            next_states.append(last_states[i])
    assert len(next_states) == nVDIs
    return next_states

def account_migration_times(vms_copy):
    global prev_vms,vms
    partial_migrate_times = 0
    partial_resume_times = 0
    full_migrate_times = 0
    post_partial_migration_times = 0

    full_migrations ={}
    partial_migrations = {}
    resume_migrations = {}
    post_partial_migrations = {}
    for i in range(nVMs):
        src = vms[i].curhost
        dest = vms_copy[i].curhost
        migration_pair = (src, dest)
        if vms[i].curhost != vms_copy[i].curhost: # if has this pre-conditions that some migrations are happening
            if prev_vms[i].state >= 1 and vms[i].state >= 1 and vms[i].origin == vms[i].curhost and vms_copy[i].origin == vms_copy[i].curhost: # we have to make sure it is full migration. Based on the state alone, it does not necessarily be full migration, it could be a vm to be active 
                full_migrate_times += 1
                if migration_pair in full_migrations:
                    full_migrations[migration_pair][0] += 1
                    full_migrations[migration_pair][1] += ("-"+str(i))
                else:
                    full_migrations[migration_pair] = [1,str(i)]
            else:
                if vms[i].curhost != vms[i].origin and vms_copy[i].curhost == vms_copy[i].origin and  vms[i].origin == vms_copy[i].origin:
                    partial_resume_times += 1
                    if migration_pair in resume_migrations:
                        resume_migrations[migration_pair][0] += 1
                        resume_migrations[migration_pair][1] += ("-"+str(i))
                    else:
                        resume_migrations[migration_pair] = [1,str(i)]
                elif (vms[i].origin == vms[i].curhost and vms_copy[i].origin != vms_copy[i].curhost and vms[i].curhost == vms_copy[i].origin) or \
                     (vms[i].curhost != vms[i].origin and vms_copy[i].curhost != vms_copy[i].origin and vms[i].origin == vms_copy[i].origin):
                    partial_migrate_times += 1
                    if migration_pair in partial_migrations:
                        partial_migrations[migration_pair][0] += 1
                        partial_migrations[migration_pair][1] += ("-"+str(i))
                    else:
                        partial_migrations[migration_pair] = [1,str(i)]
                # 1. idle before, 2. now it becomes active, 3. remote partial before (host != origin) 4. plus it changes the origin
                if vms[i].state >= 1 and vms[i].curhost != vms[i].origin and  vms[i].origin != vms_copy[i].origin:
                    migration_pair = (vms[i].origin, dest)
                    post_partial_migration_times += 1
                    if migration_pair in post_partial_migrations:
                        post_partial_migrations[migration_pair][0] += 1
                        post_partial_migrations[migration_pair][1] += ("-"+str(i))
                    else:
                        post_partial_migrations[migration_pair] = [1,str(i)]
                        
    return (partial_migrate_times, full_migrate_times, partial_resume_times, full_migrations, partial_migrations, resume_migrations, post_partial_migration_times, post_partial_migrations)

def nobody_is_migrating(last_states):
    found_migrating = False
    assert len(last_states) == nVDIs
    for i in range(nVDIs):
        if last_states[i] == MIGRATING:
            found_migrating = True
            break
    return (not found_migrating)

def decide_to_resume(vdi_states, cur_sec, of2, of5):

    global configs, vms
    assert len(vms) == nVMs

    partial_migrate_times = 0
    partial_resume_times = 0
    full_migrate_times = 0
    post_partial_migration_times =0

    full_migrations = {}
    partial_migrations = {}
    resume_migrations = {}
    post_partial_migrations = {}
    last_states = vdi_states[-1]
    # update the vdi last states if one of those are migrating. 
    (last_states, newly_sleep_vdis) = update_last_states(last_states)
    vdi_consumption = get_vdi_consumption (vms)
    assert nobody_is_migrating(last_states)

    resume = False
    vdis_to_resume = {}
    vdi_set = []
    # total resource available, positive means the existing vdi can accommadate the vms,
    # negative means we need to wake up at least one more vdis to migrate the vms
    total_resource = 0 
    
    # scan the list and see if any vdi exceeds the capacity
    for i in range(0, nVDIs):
        if last_states[i] != S3: # only account the full power vdis
            total_resource += (vms_per_vdi * (slack + 1) - vdi_consumption[i])
            vdi_set.append(i)

        if vdi_consumption[i] > vms_per_vdi * (slack + 1):
            resume = True
            vdis_to_resume[i] = True
        else:
            vdis_to_resume[i] = False

    next_states = []
    if resume:
        assert len(vdis_to_resume) > 0
        wake_vdis = False
        if total_resource >= 0:
            # try to allocate 
            allocatable = False
            vms_copy = []
            # copy the whole thing to a copy
            for v in vms:
                vms_copy.append(vm(v.origin,v.curhost,v.state))
            (allocatable, partial_migrations, resume_migrations, full_migrations, post_partial_migrations) = try_to_allocate(vdi_set, vms_copy, last_states)
            if allocatable:
                #(partial_migrate_times, full_migrate_times, partial_resume_times, full_migrations, partial_migrations, resume_migrations, post_partial_migration_times, post_partial_migrations) = account_migration_times(vms_copy)
                del vms[:]
                vms = vms_copy[:]
                next_states = get_next_states(last_states)
            else:
                partial_migrations ={}
                resume_migrations = {}
                full_migrations = {}
                post_partial_migrations = {}
                wake_vdis = True
        else:
            wake_vdis = True
            
        if wake_vdis:
            # wake up new vdi servers
            # sort the sleeping vdi, pick the one with the most remote partial vms
            sleeping_vdis = {}
            for i in range(nVDIs):
                if last_states[i] == S3:
                    sleeping_vdis[i] = 0
            for v in vms:
                if v.origin != v.curhost and last_states[v.origin] == S3:
                    assert sleeping_vdis[v.origin] >= 0
                    sleeping_vdis[v.origin] += 1

            found_solution = False
            for vdi_index, v in sorted(sleeping_vdis.items(), key=lambda x: x[1], reverse=True):
                allocatable = False
                vdi_set.append(vdi_index)
                vms_copy = []
                # copy the whole thing to a copy
                for v in vms:
                    vms_copy.append(vm(v.origin,v.curhost,v.state))
                (allocatable, partial_migrations, resume_migrations, full_migrations, post_partial_migrations) = try_to_allocate(vdi_set, vms_copy, last_states)
                if allocatable:
                    #(partial_migrate_times, full_migrate_times, partial_resume_times, full_migrations, partial_migrations, resume_migrations, post_partial_migration_times, post_partial_migrations) = account_migration_times(vms_copy)

                    del vms[:]
                    vms = vms_copy[:]
                    next_states = get_next_states(last_states)
                    going_back_to_full = []
                    # see if any newly sleeping VDIs are woken up
                    for vdi in newly_sleep_vdis:
                        if next_states[vdi] != S3:
                            going_back_to_full.append(vdi)
                    if len(going_back_to_full) > 0:
                        of5.write("%d,"%cur_sec)
                        for vdi in going_back_to_full:
                            of5.write("%d,"%vdi)
                        of5.write("\n")

                    found_solution = True
                    assert (len(partial_migrations) + len(resume_migrations) + len(full_migrations) + len(post_partial_migrations) ) > 0
                    break
                else:
                    partial_migrations ={}
                    resume_migrations = {}
                    full_migrations = {}
                    post_partial_migrations = {}

            assert found_solution
    else:
        for i in range(0, nVDIs):
            next_states.append(vdi_states[-1][i])

    return (next_states, resume, partial_migrate_times, full_migrate_times, partial_resume_times, full_migrations, partial_migrations, resume_migrations, post_partial_migrations)

# return how many idle and active vms will be migrated
def get_migrating_vdi_stats(next_states):

    nActives = 0
    nIdles = 0
    
    i = 0
    j = 0
    for i in range(0, nVDIs):
        if next_states[i] == MIGRATING:
            for j in range(0, nVMs):
                if vms[j].curhost == i:
                    if vms[j].state == 0:
                        nIdles += 1
                    else:
                        nActives += 1
    return (nActives, nIdles)
def get_reintegrating_vdi_stats(next_states):

    nActives = 0
    nIdles = 0
    
    i = 0
    j = 0
    for i in range(0, nVDIs):
        if next_states[i] == REINTEGRATING:
            for j in range(0, vms_per_vdi):
                if vms[i*vms_per_vdi + j].state== 0:
                    nIdles += 1
                else:
                    nActives += 1

    return (nActives, nIdles)

def update_last_states(last_states):
    global vms
    updated_states = []
    newly_sleep_vdis = []
    for i in range(nVDIs):
        if last_states[i] == MIGRATING:
            state_to_append = S3
            for v in vms:
                if v.curhost == i:
                    state_to_append = FULL
                    break
            updated_states.append(state_to_append)
            if state_to_append == S3:
                newly_sleep_vdis.append(i)
        else:
            updated_states.append(last_states[i])
    assert len(updated_states) == nVDIs
    return (updated_states,newly_sleep_vdis)

def make_decision(vm_states,vdi_states, cur_sec, of2, of5):
    global configs,vms
    global migration_interval,reintegration_interval
    global cumulative_interval,cumulative_interval2
    next_states = []
    overall_state = get_overall_state(vdi_states)

    partial_migration_times = 0
    full_migration_times = 0
    partial_resume_times = 0
    post_partial_migration_times = 0

    full_migrations = {}
    partial_migrations = {}
    resume_migrations = {}
    post_partial_migrations = {}

    if overall_state == "full" or "migrated":
        # update the vdi states first. If there are any servers that are in migrating state, but either switch it to S3 or back to full
        (next_states, resume, partial_migration_times, full_migration_times, partial_resume_times,full_migrations, partial_migrations, resume_migrations, post_partial_migrations) = decide_to_resume(vdi_states, cur_sec, of2, of5)
        if resume == True:
            if cur_sec == 26100:
                print "insert breakpoint here" 
            (nActives, nIdles) = get_reintegrating_vdi_stats(next_states)
            
            reintegration_interval = get_reintegration_interval(partial_migrations, full_migrations, post_partial_migrations, resume_migrations)
            if reintegration_interval <= 0:
                print "no migrations happen in this interval: %d. This is weird." % cur_sec
                assert False
            if reintegration_interval > 1:
                print "Warning: Second: %d, reintegration_interval = %d"%(cur_sec, reintegration_interval)
            cumulative_interval2 = 0
        else:
            # decide to whether to migrate again
            (next_states, decision, partial_migration_times, full_migration_times,full_migrations, partial_migrations) = decide_to_migrate(vdi_states, cur_sec, True, of2)
            if decision == True:
                (nActives, nIdles) = get_migrating_vdi_stats(next_states)
                # how many intervals it takes to migrate all VMs

                migration_interval = get_migration_interval(partial_migrations, full_migrations, post_partial_migrations)
                assert migration_interval > 0
                cumulative_interval = 0
                record_vm_states(vm_states)
            
    if overall_state == "reintegrating":
        assert  cumulative_interval2 >= 0  and cumulative_interval2 <= reintegration_interval
        if cumulative_interval2 + 1 < reintegration_interval:
            cumulative_interval2 += 1
            next_states = vdi_states[-1]
        else:
            cumulative_interval2 = 0
            next_states = update_states(vdi_states, REINTEGRATING, FULL)

    return (next_states, partial_migration_times, full_migration_times, partial_resume_times,full_migrations, partial_migrations, resume_migrations, post_partial_migrations)


def run_experiment(inputs, output_str):
    cnt = 0
    tsaving = 0.0
    tcsecs = 0
    tacsecs = 0
    tasecs = 0
    tmt = 0
    trt = 0
    fmt = 0
    rpva = 0
    for inf in inputs.rstrip().split(","):
        if inf != '':
            outf = inf+output_str
            (saving, csecs, tactive_con_secs, tactive_secs, migration_times, resume_times, full_migrate_times, remote_partial_vm_to_active)  = run(inf,outf)
            tsaving += saving
            tcsecs += csecs
            tacsecs += tactive_con_secs
            tasecs += tactive_secs
            tmt += migration_times
            trt += resume_times
            cnt += 1
            fmt += full_migrate_times
            rpva += remote_partial_vm_to_active
    ave_saving = tsaving / cnt 
    ave_consolidated_secs = tcsecs / cnt
    ave_active_vm_on_consolidated_secs = tacsecs / cnt
    ave_active_vm_secs = tasecs / cnt
    ave_migration_times = tmt / cnt
    ave_resume_times = trt /cnt
    ave_full_migration_times = fmt/cnt
    ave_rpva = rpva /cnt
    return (ave_saving, ave_consolidated_secs,\
            ave_active_vm_on_consolidated_secs, ave_active_vm_secs,\
            ave_migration_times, ave_resume_times, ave_full_migration_times, ave_rpva)


if __name__ == '__main__':

    policy_type = configs['migration_policy_type']
    
    if policy_type == "static":
        timestr = time.strftime("%Y-%m-%d-%H-%M-%S")        
        of = "data/static-all-result-"+timestr+".csv"
        f = open(of, "w+")
        header =  "Idle threshold, Resume threshold(aVM#), Slack Threshold,"
        header += "Power Saving(wd), Consol.Time(wd), Active Consol. VMs Time(wd), Ttl.Active.Time(wd), Partial Migration#, Partial Resume#, Full Migration#, Partial idle VM# turning into Active,\n"
        # header += "Power Saving(we), Consol.Time(we), Active Consol. VMs Time(we), Ttl.Active.Time(we), Migration#, Resume#, Full Migration#\n"
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
            resume_threshold = int(rts[i])
            idle_threshold = float(its[i])
            slack = float(sts[i])
            tightness = float(tts[i])
            dayofweek = "weekday"
            inputs = configs["inputs-weekday"]
            output_postfix = timestr + ".out-static-%.1f-%.1f-%.1f"%(float(its[i]), float(sts[i]), float(tts[i]))
            (ave_weekday_saving, ave_weekday_consolidated_secs, \
             ave_weekday_active_vm_on_consolidated_secs,ave_weekday_active_vm_secs,\
             ave_weekday_migration_times, ave_weekday_resume_times, ave_full_migration_times, ave_remote_partial_to_actives) = run_experiment(inputs, output_postfix)
            oline = "%.1f,%d,"%(configs['idle_threshold'],configs['resume_threshold'])
            oline += "%.1f,"%(1-configs['tightness'])

            oline += "%f, %d, %d, %d, %d, %d, %d, %d\n"% (ave_weekday_saving, ave_weekday_consolidated_secs/3600, ave_weekday_active_vm_on_consolidated_secs/3600,ave_weekday_active_vm_secs/3600,ave_weekday_migration_times,ave_weekday_resume_times, ave_full_migration_times, ave_remote_partial_to_actives)
            f.write(oline)
            
        f.close()
        print "Done. Result is in %s"%of













