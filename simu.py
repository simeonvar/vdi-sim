#!/usr/bin/env python

import ConfigParser
import math
import linecache, time, numpy
import random



#from adjust_local_partial_latency import src

try:
    import Queue as Q  # ver. < 3.0
except ImportError:
    import queue as Q

# a simple simulator program 

# for debug use only
target_timestamp = 60000

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

FULL_MIGRATION_BANDWIDTH = 4.0
PARTIAL_MIGRATION_BANDWIDTH = FULL_MIGRATION_BANDWIDTH * 0.1
RESUME_MIGRATION_BANDWIDTH = FULL_MIGRATION_BANDWIDTH * 0.1
POST_PARTIAL_MIGRATION_BANDWIDTH = FULL_MIGRATION_BANDWIDTH - PARTIAL_MIGRATION_BANDWIDTH

# global variables used in decide_to_migrate
migration_interval = 0
cumulative_interval = 0
# record all vm states when decide to migrate to 
# keep track of who are idles and who are active later
vm_states_before_migration = []
reintegration_interval = 0
cumulative_interval2 = 0

prev_vms = []                   # vms at the previous interval

#vms_threshold = 67

# vm object, that stores vm state, the original and current host. 
# If origin != curhost, it indicates that it is a remote vms. Ir(1
class vm:
    origin = -1
    curhost = -1
    state = -1
    IDLE_LOCAL_PARTIAL = 45
    ACTIVE = 78
    IDLE_REMOTE_PARTIAL = 731
    ACTIVE_REMOTE_PARTIAL = 379
    idle_consumption = -1
    def __init__(self, origin, curhost, state, idle_consumption):
        self.state = state
        self.curhost = curhost
        self.origin = origin
        self.idle_consumption = idle_consumption
    def isOriginHost(self, vdi_index):
        return self.origin == vdi_index
    def isCurHost(self, vdi_index):
        return self.curhost == vdi_index
    
    def get_type(self):
        vm_type = None
        if   self.curhost == self.origin and self.state == 0:
            vm_type = self.IDLE_LOCAL_PARTIAL
        elif self.curhost == self.origin and self.state != 0:
            vm_type = self.ACTIVE
        elif self.origin != self.curhost and self.state == 0:
            vm_type = self.IDLE_REMOTE_PARTIAL
        elif self.origin != self.curhost and self.state != 0: # remote partials that become active
            vm_type = self.ACTIVE_REMOTE_PARTIAL
        return vm_type

class Migration_plan:
    cur_sec = 1 
    partial_migrations = {}
    post_partial_migrations = {}
    full_migrations = {}
    resume_migrations = {}
    migration_cause = None
    partial_migration_times = 0 
    full_migration_times = 0 
    post_partial_migration_times = 0
    resume_migration_times = 0

# a class that is used to store the waiting time for vm in the queue
class Waiting_VM:
    vm_index = -1
    waiting_time = 0
    timestamp_of_becoming_active = 0
    host = 0
    def __init__(self, vm_index, waiting_time, timestamp_of_becoming_active, host):
        self.vm_index = vm_index
        self.waiting_time = waiting_time
        self.timestamp_of_becoming_active = timestamp_of_becoming_active
        self.host = host
        return
    def __cmp__(self, other):
        return self.waiting_time < other.waiting_time

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
nVMs = 200
nServers = 20
vms_per_vdi = nVMs / nServers
nConsolidationServers = 1
nVDIs = nServers + nConsolidationServers
# we randomly sample from the traces and decide the vm assignment
allVMs = int(configs['allVMs'])
vm_host_map = random.sample(range(0, allVMs), nVMs)
optimization = configs['optimization']

idle_vm_consumption = float(configs['idle_vm_consumption'])
idle_vm_consumption_min = float(configs['idle_vm_consumption_min'])
idle_vm_consumption_max = float(configs['idle_vm_consumption_max'])
interval = int(configs['interval'])
full_power = float(configs['full_power']) 
low_power =  float(configs['low_power'])
method = configs['method']
interval = int(configs['interval'])
full_migrate = int(configs['full_migrate'])
partial_migrate = int(configs['partial_migrate'])
s3_suspend = float(configs['s3_suspend'])
traces_file = configs['traces']
interval_ahead = int(configs["interval_ahead"])
partial_resume = int(configs['partial_resume'])
s3_resume = float(configs['s3_resume'])

idle_threshold = float(configs['idle_threshold'])
resume_threshold = int(configs['resume_threshold'])
migration_policy_type = configs["migration_policy_type"]
slack = float(configs['slack'])
tightness = float(configs['tightness'])
dayofweek = configs['dayofweek'] # weekday or weekend
DAYS = configs['days']
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
    #(h,m,sec) = format_cur_time(cur_sec)
    for i in range(0, nVMs):
        v = vms[i]
        prev_vms.append(vm(v.origin,v.curhost,v.state, v.idle_consumption))
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
            resource_consumption = idles_in_curhost * vms[i].idle_consumption + actives_in_curhost
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
                    resource += v.idle_consumption
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
            if cur_sec == target_timestamp or cur_sec == target_timestamp + interval or cur_sec == target_timestamp - interval:
                # report the results
                rf = open(result_file, "a+")
                # look at the VDI nServers and their states
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

def output_migration_plan_details(migration_plan, of):
    header = "VM Index,VM Type,Orginal Host,Current Host,Destination Host,State,Migration Type\n"
    of.write(header)
    all_migrated_vms = []
    (lines,migrated_vms) = extrace_migrated_vms(migration_plan.full_migrations)
    for l in lines:
        of.write(l)
        of.write("full\n")
    all_migrated_vms += migrated_vms
    (lines,migrated_vms)= extrace_migrated_vms(migration_plan.partial_migrations)
    for l in lines:
        of.write(l)
        of.write("partial\n")
    all_migrated_vms += migrated_vms
    (lines,migrated_vms)= extrace_migrated_vms(migration_plan.resume_migrations)
    for l in lines:
        of.write(l)
        of.write("resume\n")
    all_migrated_vms += migrated_vms
    (lines,migrated_vms) = extrace_migrated_vms(migration_plan.post_partial_migrations)
    for l in lines:
        of.write(l)
        of.write("post partial\n")
    # we dont count post partial migrations

    for i in range(nVMs):
        if i not in all_migrated_vms:
            (vm_type,vm_state) = get_vm_info(i)
            of.write("%d,%s,%d,%d,-,%s,No Migration\n"%(i,vm_type,vms[i].origin,vms[i].curhost,vm_state))
    of.close()

def get_vm_info(vm_index):
    vm = vms[int(vm_index)]
    vm_type = "full"
    if vm.curhost != vm.origin:
        vm_type = "remote partial"
    else:
        if vm.state == 0:
            vm_type = "local partial"
    state = "idle"
    if vm.state >= 1:
        state = "active"
    return (vm_type, state)

def extrace_migrated_vms(migrations):
    global vms
    lines = []
    all_vms = []
    for pair in migrations: 
        migrated_vms = migrations[pair][1]
        for vm_index in migrated_vms.split("-"):
            line = ""
            line += (vm_index+",")
            all_vms.append(int(vm_index))
            vm = vms[int(vm_index)]
            vm_type = "full"
            if vm.curhost != vm.origin:
                vm_type = "remote partial"
            else:
                if vm.state == 0:
                    vm_type = "local partial"
            line += (vm_type+",")
            line += (str(vm.origin)+",")
            line += ("%d,%d"%pair + ",")
            state = "idle"
            if vm.state >= 1:
                state = "active"
            line += (state+",")
            lines.append(line)
    return (lines, all_vms)

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

# get the abortion of partial migrations due to VMs suddenly becoming active
def calculate_partial_abortions_due_to_active_transitions(vms_turning_into_active_schedule, migration_plan):
    (schedule, migration_types) = make_migration_schedule(migration_plan)

    # for each transition, we see if that transition happens at the moment of the migration.
    # their curhost should be in the destination of the previous 
    partial_abortions = 0 
    for vm_index in vms_turning_into_active_schedule:
        timestamp = vms_turning_into_active_schedule[vm_index]
        curhost = vms[vm_index].curhost 
        if len(schedule[curhost]) > timestamp: 
            if schedule[curhost][timestamp] == vm_index and migration_types[curhost][timestamp] == "p": 
                # we see an abortion
                print "We see partial abortion at %d because of the VM becomes active" % timestamp 
                print "vm_index, timestamp, curhost"
                line = "%d,%d,%d\n"%(vm_index, timestamp, curhost)
                print line
                partial_abortions += 1
    return partial_abortions

def calculate_abortions_due_to_overloaded(host_becomes_overloaded, migration_plan, of7):
    (schedule, migration_types) = make_migration_schedule(migration_plan)

    # for each transition, we see if that transition happens at the moment of the migration.
    # their curhost should be in the destination of the previous 
    abortions_due_to_overload = 0 
    for host in host_becomes_overloaded:
        timestamp = host_becomes_overloaded[host]
        curhost = host
        if len(schedule[curhost]) > timestamp: 
            aborted_migration_type = migration_types[curhost][timestamp]
            aborted_queue = []
            aborted_full = 0
            aborted_partial = 0
            aborted_post_partial = 0 
            for i in range(timestamp, len(schedule[host])):
                v = schedule[curhost][i]
                if v not in aborted_queue:
                    aborted_queue.append(v)
                    if migration_types[curhost][i] == "p":
                        aborted_partial += 1
                    elif migration_types[curhost][i] == "f":
                        aborted_full += 1
                    elif migration_types[curhost][i] == "pp":
                        aborted_post_partial += 1
            aborted_vms = len(aborted_queue)
            assert aborted_vms > 0
            print "We see abortion of %s migration at timestamp(%d) from host %d" % (aborted_migration_type, timestamp, host)
            #line = "%d,%d,%d,%s,%s,%d,%d,%d\n"%(timestamp, curhost, aborted_vms, str("-".join(aborted_queue)), str(aborted_migration_type), aborted_full, aborted_partial, aborted_post_partial)
            line = "%d,%d,%d,%d,%d,%d\n"%(migration_plan.cur_sec, curhost, aborted_vms, aborted_full, aborted_partial, aborted_post_partial)
            print line
            of7.write(line)
            abortions_due_to_overload += 1
    return abortions_due_to_overload

# make the schedule for destination host
def make_migration_schedule(migration_plan):
    schedule = {}               # schedule by the destination host
    migration_types = {}
    # make a schedule first
    for i in range(nVDIs):
        schedule[i] = []
        migration_types[i] = []
        for (src,dst) in migration_plan.partial_migrations:
            if dst == i: 
                sequence = migration_plan.partial_migrations[(src,dst)][1]
                for v in sequence.split("-"):
                    vm_index = int(v)
                    # append as many second of migration latency of VM number to there 
                    for nSecs in range(int(partial_migrate)):
                        schedule[i].append(vm_index)
                        migration_types[i].append("p")

        for (src,dst) in migration_plan.full_migrations:
            if dst == i: 
                sequence = migration_plan.full_migrations[(src,dst)][1]
                for v in sequence.split("-"):
                    vm_index = int(v)
                    # append as many second of migration latency of VM number to there 
                    for nSecs in range(int(full_migrate)):
                        schedule[i].append(vm_index)
                        migration_types[i].append("f")

        for (src,dst) in migration_plan.post_partial_migrations:
            if dst == i: 
                sequence = migration_plan.post_partial_migrations[(src,dst)][1]
                for v in sequence.split("-"):
                    vm_index = int(v)
                    # append as many second of migration latency of VM number to there 
                    for nSecs in range(int(full_migrate)):
                        schedule[i].append(vm_index)
                        migration_types[i].append("pp")

        for (src,dst) in migration_plan.resume_migrations:
            if dst == i: 
                sequence = migration_plan.resume_migrations[(src,dst)][1]
                for v in sequence.split("-"):
                    vm_index = int(v)
                    for nSecs in range(int(partial_resume)):
                        schedule[i].append(vm_index)
                        migration_types[i].append("r")

        if len(schedule[i]) > interval + 10:
            print "Warning: Cur sec: %d, destination migration plan (length %d) out of interval"%(migration_plan.cur_sec, len(schedule[i]))
        #assert len(migration_types[i]) <= interval + 10

    assert len(schedule) == nVDIs
    assert len(migration_types) == nVDIs
    return (schedule, migration_types)

# make the schedule for source host
def make_migration_source_schedule(migration_plan):
    schedule = {}               # schedule by the destination host
    migration_types = {}
    # make a schedule first
    for i in range(nVDIs):
        schedule[i] = []
        migration_types[i] = []
        for (src,dst) in migration_plan.partial_migrations:
            if src == i: 
                sequence = migration_plan.partial_migrations[(src,dst)][1]
                for v in sequence.split("-"):
                    vm_index = int(v)
                    # append as many second of migration latency of VM number to there 
                    for nSecs in range(int(partial_migrate)):
                        schedule[i].append(vm_index)
                        migration_types[i].append("p")

        for (src,dst) in migration_plan.full_migrations:
            if src == i: 
                sequence = migration_plan.full_migrations[(src,dst)][1]
                for v in sequence.split("-"):
                    vm_index = int(v)
                    # append as many second of migration latency of VM number to there 
                    for nSecs in range(int(full_migrate)):
                        schedule[i].append(vm_index)
                        migration_types[i].append("f")

        for (src,dst) in migration_plan.post_partial_migrations:
            if src == i: 
                sequence = migration_plan.post_partial_migrations[(src,dst)][1]
                for v in sequence.split("-"):
                    vm_index = int(v)
                    # append as many second of migration latency of VM number to there 
                    for nSecs in range(int(full_migrate)):
                        schedule[i].append(vm_index)
                        migration_types[i].append("pp")

        for (src,dst) in migration_plan.resume_migrations:
            if src == i: 
                sequence = migration_plan.resume_migrations[(src,dst)][1]
                for v in sequence.split("-"):
                    vm_index = int(v)
                    for nSecs in range(int(partial_resume)):
                        schedule[i].append(vm_index)
                        migration_types[i].append("r")

        if len(schedule[i]) > interval + 10:
           print "Warning: Cur sec: %d, source migration plan (length %d) out of interval"%(migration_plan.cur_sec, len(schedule[i]))
        #assert len(migration_types[i]) <= interval + 10

    assert len(schedule) == nVDIs
    assert len(migration_types) == nVDIs
    return (schedule, migration_types)

def get_bandwidth(migration_plan):
    bw = 0 
    for pair in migration_plan.full_migrations: 
        bw += (FULL_MIGRATION_BANDWIDTH * migration_plan.full_migrations[pair][0])
    for pair in migration_plan.partial_migrations: 
        bw += (PARTIAL_MIGRATION_BANDWIDTH * migration_plan.partial_migrations[pair][0])
    for pair in migration_plan.resume_migrations: 
        bw += (RESUME_MIGRATION_BANDWIDTH * migration_plan.resume_migrations[pair][0])
    for pair in migration_plan.post_partial_migrations: 
        bw += (POST_PARTIAL_MIGRATION_BANDWIDTH * migration_plan.post_partial_migrations[pair][0])
    return bw

def get_event_details(event):
    splits = event.split(",")
    if len(splits) == 2:
        direction = splits[0]
        vm_index = int(splits[1])
        return (direction, vm_index)
    return (None, None)

# return -1 if it is not in the queue
def get_migration_completion_timestamp(vm, host_schedule):
    pos = -1
    cnt = 0
    for event in host_schedule:
        (direction, vm_index) = get_event_details(event)
        if vm_index == vm and direction == "o":
            pos = cnt 
        cnt += 1
    return pos

def find_last_migration_vm(cur_pos, host_schedule, target_vm_index):
    pos = cur_pos
    migrating_vm_index = -1
    while pos != len(host_schedule):
        event = host_schedule[pos]
        (direction, vm_index) = get_event_details(event)
        pos += 1
        if direction == "o" and vm_index == target_vm_index:
            migrating_vm_index = vm_index
        else:
            break
    return (pos, migrating_vm_index)

def find_next_migration_vm(cur_pos, host_schedule):
    pos = cur_pos
    migrating_vm_index = -1
    while pos != len(host_schedule):
        event = host_schedule[pos]
        (direction, vm_index) = get_event_details(event)
        pos += 1
        if direction == "o":
            migrating_vm_index = vm_index
            break
    return (pos, migrating_vm_index)

def migrate_out_one_vm(cur_pos, host_schedule):
    pos = cur_pos
    resource_released = 0
    (pos, migrating_vm_index) = find_next_migration_vm(pos, host_schedule)
    if (migrating_vm_index != -1):  # can't find the next vm any more. 
        first_pos = pos - 1
        (last_pos, next_migrating_vm_index) = find_last_migration_vm(pos, host_schedule, migrating_vm_index)
        pos = last_pos
        if next_migrating_vm_index != migrating_vm_index:
            print "migrating_vm_index ", migrating_vm_index
            print "next_migrating_vm_index ", next_migrating_vm_index
            print "first_pos ", first_pos
            print "last_pos ", last_pos
            print host_schedule
            print  "len of host schedule: " , len(host_schedule)
            assert False
        diff = last_pos - first_pos
        if diff <= 5:
            resource_released = 0.1
        else:
            resource_released = 1
    return (resource_released,pos) 

def calculate_provision_latency(cur_sec, nVDIs, vm_transitions_to_handle, resource_available, migration_schedule, provision_latencies, of11):
    host_migration_pos = {}
    not_handled_yet = {}
    for i in range(nVDIs):
        not_handled_yet[i] = []
        host_migration_pos[i] = 0
    
    resource_needed = 0.9
    
    for vdi in range(nServers, nVDIs):
        while not vm_transitions_to_handle[vdi].empty():
            waiting_vm = vm_transitions_to_handle[vdi].get()
            vm = waiting_vm.vm_index
            timestamp_of_becoming_active = waiting_vm.timestamp_of_becoming_active
            vm_is_handled = False
            # see if the vm is in the outflow queue, 
            pos = get_migration_completion_timestamp(vm, migration_schedule[vdi])
            latency = 0
            if pos != -1:
                latency  = cur_sec - timestamp_of_becoming_active + pos
                vm_is_handled = True
            else:
                # not moving out, so it is a VM that is waiting for resources
                # move through the schedule, whenever we finish migrating one VM, then we release resources
                cur_pos  = host_migration_pos[vdi]
                while True:
                    (next_resource_released,cur_pos) = migrate_out_one_vm(cur_pos, migration_schedule[vdi])
                    resource_available[vdi] += next_resource_released
                    if next_resource_released != 0:
                        if resource_available[vdi] >= resource_needed:
                            latency = cur_sec - waiting_vm.timestamp_of_becoming_active + cur_pos
                            resource_available[vdi] -= resource_needed   
                            vm_is_handled = True
                            break
                    else:
                        break
                assert cur_pos <= interval
                host_migration_pos[vdi] = cur_pos
            if not vm_is_handled:
                waiting_vm.waiting_time += 1
                not_handled_yet[vdi].append(waiting_vm)
            else:
                provision_latencies.append(latency)
                of11.write("%d,%d,%d,%d,%d\n"%(vm, vdi, timestamp_of_becoming_active, timestamp_of_becoming_active+latency, latency))
    # make sure all is empty
    for vdi in range(nServers, nVDIs):
        assert vm_transitions_to_handle[vdi].empty()
        for waiting_vm in not_handled_yet[vdi]:
            vm_transitions_to_handle[vdi].put(waiting_vm)
    return provision_latencies

def print_partial_vm_number(vms_copy=None):
    global vms
    if vms_copy == None:
        vms_copy = vms
    partial_vms = 0
    for i in range(nVMs):
        if vms_copy[i].curhost != vms_copy[i].origin:
            partial_vms += 1
    return partial_vms

def print_host_schedule(host_schedule):
    print host_schedule
    print "Available full migration slots count: %d" % len(find_available_slots(host_schedule, full_migrate))
    print "Available partial migration slots count: %d" % len(find_available_slots(host_schedule, partial_migrate))

def assert_states(cur_sec, next_vdi_states, vms_copy, anchor_point):
    for i in range(nVMs):
        v = vms_copy[i]
        #print "v.curhost: ", v.curhost
        if next_vdi_states[v.curhost] == S3:
            print "WARNING in point", anchor_point, ": Cur sec: ", cur_sec, " vm index is ", i, " but its host ", v.curhost, " is asleep"
            assert False 
        if v.curhost in range(nServers, nVDIs) and not v.origin in range(nServers, nVDIs) and next_vdi_states[v.origin] == FULL:
            print "WARNING in point", anchor_point, ": Cur sec: ", cur_sec, " vm index ", i, "in curhost, ", v.curhost, ", but its home ", v.origin , " is not sleeping. This vm should be reintegrated"
            #assert False 
def update_vm_transition_to_handle(nVDIs, cur_sec, vm_states_of_this_interval, vm_transitions_to_handle, provision_latencies, the_rest_latencices, of11):
    # find the VM that has been active and now it is idle, and it has been in the interval for more than a while
    cnt = 0 
    for vdi in range(nServers, nVDIs):
        not_handled_yet = []
        while not vm_transitions_to_handle[vdi].empty():
            waiting_vm = vm_transitions_to_handle[vdi].get()
            vm_index = waiting_vm.vm_index
            if vm_states_of_this_interval[vm_index] == 0: # now it is idle
                latency = cur_sec - interval - waiting_vm.timestamp_of_becoming_active # because the last interval it is idle, so we don't count this interval
                the_rest_latencices.append(latency)
                cnt += 0
                of11.write("%d,%d,%d,%d,%d,%s\n"%(vm_index,waiting_vm.host,waiting_vm.timestamp_of_becoming_active,cur_sec,0,"It automatically becomes idle"))
            else:
                not_handled_yet.append(waiting_vm)
        # put the ones that we haven't dealt with back to the queue
        for vm in not_handled_yet:
            vm_transitions_to_handle[vdi].put(vm)
    return cnt 

# throw a dice and decide a number between the lower and upper bound
def random_idle_consumption(consumption_min, consumption_max):
    amplified = random.randint(int(consumption_min*100), int(consumption_max*100))
    # let's don't want that random here
    #amplified = (consumption_min + consumption_max) / 2
    return float(amplified) / 100

def run(inf, outf):

    inf = open(inf, "r")
    tmp = outf
    outf = tmp + "-by-second.csv"
    outf2 = tmp + "-by-interval.csv"
    outf3 = tmp + "-migrations-by-interval.csv"
    outf4 = tmp + "-idle-to-active-transitions-by-interval.csv"
    outf5 = tmp + "-just-go-to-sleep-and-then-wake-up-vdis.csv"
    outf6 = tmp + "-bounce-reintegrations.csv"
    outf7 = tmp + "-aborted-migrations-due-host-overload.csv"
    outf8 = tmp + "-migrations-in-%d.csv"%target_timestamp
    outf9 = tmp + "-migrations-in-%d.csv"%(target_timestamp-interval)
    outf10 = tmp + "-migrations-in-%d.csv"%(target_timestamp+interval)
    outf11 = tmp + "-provision-delays-.csv"

    of  = open(outf,"w+")
    of2 = open(outf2,"w+")      # five minute interval results
    of3 = open(outf3,"w+")      # five minute interval results
    of4 = open(outf4, "w+")
    of5 = open(outf5, "w+")
    of6 = open(outf6, "w+")
    of7 = open(outf7, "w+")
    of8 = open(outf8, "w+")
    of9 = open(outf9, "w+")
    of10 = open(outf10, "w+")
    of11 = open(outf11, "w+")

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

    of6_header = "Current Second,Source Host, Dest. Host, Migration#, Migration Sequence\n"
    of6.write(of6_header)

    of7_header = "Current Second, Destination Host, Aborted VM Count,Aborted Queue,Current Aborted Migration Type, Aborted Full Migrations, Aborted Partial Migrations, Aborted Post Partial Migrations\n"
    of7.write(of7_header)

    of11_header = "VM index, Current Host, Transition Second, Second of being handled, Delay(s)\n"
    of11.write(of11_header)

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
    vm_active_vdi_migrated_secs = 0
    migration_times = 0
    resume_times = 0
    total_full_migration_times = 0
    remote_partial_vm_into_active = 0
    # record which idle vm turns into active at which timestamp of the interval
    vms_turning_into_active_schedule = {}
    host_becomes_overloaded = {}
    previous_migration_plan = Migration_plan()

    bandwidth = 0 
    vm_transitions_to_handle = {}
    provision_latencies = []
    the_rest_latencies = []
    need_post_partial_migrations = {}
    total_itoactive = 0
    total_itoactive2 = 0
    
    active_becomes_idle = 0
    for i in range(nServers, nVDIs):
        vm_transitions_to_handle[i] = Q.PriorityQueue()
        need_post_partial_migrations[i] = Q.PriorityQueue()
        
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
                if i in range(0,nServers):
                    cur_vdi_states.append(FULL)
                else:
                    cur_vdi_states.append(S3)
        else:
            cur_vdi_states = vdi_states[-1]

        assert len(vm_states) == nVMs

        # init vms array
        if cur_sec == 0:
            del vms[:]
            for j in range(0, nVMs):
                origin = j / vms_per_vdi
                
                idle_consumption = random_idle_consumption(idle_vm_consumption_min, idle_vm_consumption_max)
                new_vm = vm(origin, origin, 0, idle_consumption)
                vms.append(new_vm)

        assert len(vms) == nVMs
        # if cur_sec % interval == 0: # only re-init it to all 0 when reaching the end of the interval

        # evaluate the current situation
        for i in range(0, nVMs):
            activity_index = vm_host_map[i]
            a = int(activities[activity_index].lstrip().rstrip())
            assert a >= 0
            if a >= 1:
                vm_states[i] = 1 # assigned to 1, 0 by default, it will get reinited to 0 at the end of the interval

                if vms[i].state == 0:
                    curhost = vms[i].curhost
                    total_consumption = 0
                    transition_timestamp = cur_sec % interval 
                    for j in range(nVMs):
                        if vms[j].curhost == curhost:
                            if vm_states[j] == 0:
                                total_consumption += vms[j].idle_consumption
                            else:
                                total_consumption += 1
                                
                    if total_consumption > vms_per_vdi: # exceeding the destination hosts
                        if curhost not in host_becomes_overloaded:                           
                            host_becomes_overloaded[curhost] = transition_timestamp

                    if i not in vms_turning_into_active_schedule:
                        vms_turning_into_active_schedule[i] = transition_timestamp
                        total_itoactive += 1
                        if total_consumption > vms_per_vdi: # exceeding the destination hosts
                            waiting_vm = Waiting_VM(i,0,cur_sec,curhost)
                            vm_transitions_to_handle[curhost].put(waiting_vm)
                        else:
                            if vms[i].curhost == vms[i].origin:
                                # for a local idle vms turning into active, then its latency i
                                #provision_latencies.append(0)
                                the_rest_latencies.append(0) 
                                of11.write("%d,%d,%d,%d,%d\n"%(i,curhost,cur_sec,cur_sec,0))
                            else:
                                # for a remote partial, it is the latency of post partial migration
                                # provision_latencies.append(full_migrate)
                                # the_rest_latencies.append(full_migrate)  
                                #of11.write("%d,%d,%d,%d,%d,post_partial_migrations\n"%(i,curhost,cur_sec,cur_sec+full_migrate,full_migrate))
                                waiting_vm = Waiting_VM(i,0,cur_sec,curhost)
                                need_post_partial_migrations[curhost].put(waiting_vm)
                                
        if cur_sec == target_timestamp:
                print "Debug here"
                #phv() 
                #pvs(vdi_states)
                #assert False
                pass

        if cur_sec % interval == 0 and cur_sec > 0:
            print cur_sec 
            #prev_partial_vms = print_partial_vm_number()
            
            # update the vm_transition_to_handle map. If a VM has been pushed back twice, and now if it becomes idle, then that means
            # it does not need the resource any more. 
            
            active_becomes_idle += update_vm_transition_to_handle(nVDIs, cur_sec, vm_states, vm_transitions_to_handle, provision_latencies, the_rest_latencies, of11)
            
            if previous_migration_plan.cur_sec - cur_sec <= interval and previous_migration_plan.migration_cause == "consolidation":
                # calculate the abortions for partial migrations
                calculate_partial_abortions_due_to_active_transitions(vms_turning_into_active_schedule, previous_migration_plan)
                calculate_abortions_due_to_overloaded(host_becomes_overloaded, previous_migration_plan, of7)

            # clear the schedule first
            vms_turning_into_active_schedule.clear()
            host_becomes_overloaded.clear()

            # update global variable vms state and origin hosts
            itoactive = update_vms(of4, cur_sec, vm_states)
            remote_partial_vm_into_active += itoactive
            result_file = outf + "-before-%d.csv"%cur_sec
            output_target_timestamp(cur_sec, vdi_states, result_file)
            output_interval(of2, vm_states, vdi_states, cur_sec)
            
            assert_states(cur_sec, vdi_states[-1], vms, "1")
            # Reaching the end of the interval, time to make decision
            (next_vdi_states, plan) = make_decision(vm_states, vdi_states, cur_sec, provision_latencies, vm_transitions_to_handle, need_post_partial_migrations, of2, of5, of6, of11)
            # make sure vm current host and their states are consistent 
            (cs_consumptions, partial_cnt, full_idle_cnt, full_active_cnt)  = get_cs_consumption(vms)
            print "next_vdi_states: ", next_vdi_states
            for cs  in range(nServers, nVDIs):
                print "Consolidation consumption: ", cs, ":", cs_consumptions[cs]
                print "partial_cnt ", partial_cnt[cs]
                print "full_idle_cnt ", full_idle_cnt[cs]
                print "full_active_cnt ", full_active_cnt[cs]
                
            assert_states(cur_sec, next_vdi_states, vms, "2")
            assert len(next_vdi_states) == nVDIs
            
            bandwidth += get_bandwidth(plan)

            if method == "full":
                partial_vms = print_partial_vm_number(vms)
                assert partial_vms == 0
                
                assert len(plan.partial_migrations) == 0
                assert len(plan.post_partial_migrations) == 0
                assert len(plan.resume_migrations) == 0

            #(migrate, resume) = check_state(cur_vdi_states, next_vdi_states)

            of2.write("%d,%d,%d,%d,%d"%(plan.partial_migration_times,plan.full_migration_times,plan.resume_migration_times,plan.post_partial_migration_times,itoactive))
            of2.write("\n")
            
            migration_times += plan.partial_migration_times
            resume_times += plan.resume_migration_times

            assert resume_times <= migration_times

            total_full_migration_times += plan.full_migration_times
            vdi_states.append(next_vdi_states)
            assert_states(cur_sec, vdi_states[-1], vms, "3")

            output_migration_maps(of3, cur_sec, plan.full_migrations, plan.partial_migrations, plan.resume_migrations, plan.post_partial_migrations)
            result_file = outf + "-after-%d.csv"%cur_sec
            output_target_timestamp(cur_sec, vdi_states,result_file)

            if cur_sec == target_timestamp:
                output_migration_plan_details(plan, of8)

            if cur_sec == (target_timestamp - interval):
                output_migration_plan_details(plan, of9)

            if cur_sec == (target_timestamp + interval):
                output_migration_plan_details(plan, of10)

            #calculate_provision_latency(cur_sec, vm_transitions_to_handle, plan, provision_latencies, of11)

            # re-init the vm_states to 0
            for j in range(0, nVMs):
                vm_states[j] = 0
            previous_migration_plan = plan
        else:
            vdi_states.append(cur_vdi_states) 
            assert_states(cur_sec, vdi_states[-1], vms, "4")
        cur_sec += 1
        total_vdi_activeness_arr.append(vdi_activeness)

    inf.close()

    not_handled_vm_transitions = 0
    
    for vdi in range(nServers, nVDIs):
        not_handled_vm_transitions += vm_transitions_to_handle[vdi].qsize()

    if not_handled_vm_transitions > 0:
        print "vm_transitions_to_handle still has %d vm transitions to handle, " % not_handled_vm_transitions
        for vdi in range(nServers, nVDIs):
            while not vm_transitions_to_handle[vdi].empty():
                waiting_vm = vm_transitions_to_handle[vdi].get()
                print "vm_index, ", waiting_vm.vm_index
                print "vm state ", vms[waiting_vm.vm_index].state
                print "second becoming active ",  waiting_vm.timestamp_of_becoming_active
                print "waiting time: ", waiting_vm.waiting_time
                print "latency is ", cur_sec - waiting_vm.timestamp_of_becoming_active
                
        
    print "total_itoactive: ", total_itoactive
    print "total_itoactive2: ", total_itoactive2
    o = ''
    o += "Time,"
    for v in range(0, nVDIs):
        o+="VDI%d-state,VDI%d-activeness"%(v,v)
        if v != nVDIs-1:
            o+=","
    o+= ",VDIs in Full Power, TotalVMActiveness\n"
    # of.write(o)
    i = 0
    # total time that all vdi nServers running in low power states
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
    power_saving = 1 - (total_power /(86400 * DAYS * full_power * nServers))
    of.write("Total power saving: %f\n" % power_saving) 
    # rate of all the active seconds when there active VMs running on consolidated hosts
    #rate = float(vm_active_vdi_migrated_secs)/active_secs
    rate = 0
    of.write("Seconds when active VMs are operating on consolidated host: %d, %f of all total seconds\n"%(vm_active_vdi_migrated_secs, rate))
    of.close()
    of2.close()
    of3.close()
    of4.close()
    print "Done. Result is stored in %s" % outf
    print "Current vm_host_map is ", vm_host_map
    # print "Total active seconds: %d" % a1

    return (power_saving, bandwidth, provision_latencies, the_rest_latencies, active_becomes_idle)

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
    assert False
    idle_vms = vdi_idleness[index]
    idle_vm_resource_needed = idle_vms * idle_vm_consumption
    active_vm_resource_needed = vdi_activeness[index]
    resource_needed = (idle_vm_resource_needed + active_vm_resource_needed)
    return resource_needed

# scanning the vm states in the vms array
def get_resource_consumed(vdi_index,vms_copy):
    assert False
    rcsmd = 0
    for i,v in enumerate(vms_copy):
        if v.curhost == vdi_index:
            rneeded = 1
            if v.state == 0: # idle vm
                rneeded = idle_vm_consumption
            rcsmd += rneeded
    return rcsmd

def decide_detailed_migration_plan(to_migrate, migration_schedule, last_states):
    global configs
    global vms
    global vm_vdi_logs
    global nVMs, vms_per_vdi
    
    # below two maps to keep track of the detailed migrations from src to dest
    full_migrations = {}  # use (src,dest) as the key to keep track of the full migrations
    partial_migrations = {} # only keep track of partial migrations times for each pair of src and host
    post_partial_migrations = {} 
   
    vms_copy = []
    # copy the whole thing to a copy
    for v in vms:
        vms_copy.append(vm(v.origin,v.curhost,v.state, v.idle_consumption))
    
    vdi_consumption = get_vdi_consumption(vms_copy)
    for i in to_migrate:
        for vm_index in range(0, nVMs):
            if vms_copy[vm_index].curhost != i:
                continue
            vm_state = vms[vm_index].state
            rneeded = 1         # by default, active vm needs 100%
            latency_needed = 0
            
            if method == "oasis":                
                if vm_state == 0:
                    rneeded = vms_copy[vm_index].idle_consumption
                    latency_needed += partial_migrate
                else:
                    rneeded = 1
                    latency_needed += partial_migrate
            elif method == "full":
                latency_needed = full_migrate
                rneeded = 1
            
            dest_found = False
            for dest_host in range(nServers, nVDIs):
                src = i
                if vdi_consumption [dest_host] + rneeded <= vms_per_vdi:
                    slot_found = find_migration_slot(src, dest_host, migration_schedule, latency_needed, interval)
                    if slot_found != None:
                        dest_found = True
                        vdi_consumption [dest_host] += rneeded 
                        if method == "oasis":   
                            if vm_state == 0:
                                record_migration(vm_index, src, dest_host, partial_migrations)
                                vms_copy[vm_index].curhost = dest_host
                            else:
                                record_migration(vm_index, src, dest_host, full_migrations)
                                vms_copy[vm_index].curhost = dest_host
                                vms_copy[vm_index].origin = dest_host
                        elif method == "full":
                            record_migration(vm_index, src, dest_host, full_migrations)
                            vms_copy[vm_index].curhost = dest_host
                            vms_copy[vm_index].origin = dest_host
                        else:
                            assert False 
                        update_schedule(src, dest_host, vm_index, migration_schedule, slot_found)
                else:
                    continue
            if not dest_found:
                break
                
    # update the vms 
    del vms[:]
    vms = vms_copy[:]

    assert len(vms) > 0
    return (True,full_migrations,partial_migrations, post_partial_migrations)

def debug_print(vdi_states, vdi_idleness, vdi_activeness, cur_sec):

    print "cur_sec, vdi#, state, resource consumed"
    for i in range(0, nVDIs):
        out = "%d,"%cur_sec
        out += "%d,"%i
        out += "%s,"%state_str(vdi_states[-1][i])
        idles = vdi_idleness[i]
        actives = vdi_activeness[i]
        rc = actives + idles * idle_vm_consumption
        out += "Outdated consumption(assuming idle take 10%): %f"%rc
        print out

def get_available_resources(last_states):
    global vms, nServers, nConsolidationServers, nVDIs, vms_per_vdi
    avail = 0
    
    vdi_consumption = get_vdi_consumption(vms)
    for cs in range(nServers, nVDIs):
        if last_states[cs] != S3:
            diff = vms_per_vdi - vdi_consumption[cs]
            assert diff >= 0
            avail += diff
    return avail

def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z

def update_hosts(vms_copy,  partial_migrations, full_migrations):
    for (src, dest) in partial_migrations:        
        migration_sequence = partial_migrations[(src, dest)][1]
        for vm_index_str in migration_sequence.split("-"):
            vm_index = int(vm_index_str)
            vms_copy[vm_index].curhost = dest
    
    for (src, dest) in full_migrations:        
        migration_sequence = full_migrations[(src, dest)][1]
        for vm_index_str in migration_sequence.split("-"):
            vm_index = int(vm_index_str)
            vms_copy[vm_index].curhost = dest
            vms_copy[vm_index].origin = dest            

def try_to_consolidate(vms_copy, existing_powered_on_servers, existing_consolidation_servers):
    vms_copy2 = []
    for v in vms_copy:
        vms_copy2.append(vm(v.origin,v.curhost,v.state,v.idle_consumption))
        
    (powered_off_servers, migration_plan) = consolidate_into_existing_consolidation_servers(vms_copy2, existing_powered_on_servers, existing_consolidation_servers)
    
    consolidatable = False
    powered_off_server_num = len(powered_off_servers)
    if powered_off_servers > 0:
        consolidatable = True
    
    return (consolidatable, migration_plan, powered_off_server_num) 
      
def consolidate_into_existing_consolidation_servers(vms_copy,hosts_to_migrate, hosts_vms, existing_consolidation_servers):
    global vms_per_vdi, nVDIs, nServers    
    migration_plan = Migration_plan()
    
    powered_off_servers = []
    for vdi in hosts_to_migrate:
        vdi_consumption = get_vdi_consumption(vms_copy)
        partial_migrations = {}
        full_migrations = {}
        evicted_all_vms = True
        for vm_index in hosts_vms[vdi]:
            rneeded = 1
            partial = False
            if vms_copy[vm_index].state == 0:
                rneeded = vms_copy[vm_index].idle_consumption
                partial = True
            migratable = False            
            for cs in range(nServers, nVDIs):
                if  vdi_consumption[cs] + rneeded <= vms_per_vdi:
                    vdi_consumption[cs] -= rneeded
                    migratable = True
                    if partial:
                        record_migration(vm_index, vdi, cs, partial_migrations)
                    else:
                        record_migration(vm_index, vdi, cs, full_migrations)
                    break
            if not migratable:
                evicted_all_vms = False
                break
        if evicted_all_vms:
            powered_off_servers.append(vdi)
            update_hosts(vms_copy,  partial_migrations, full_migrations)
            migration_plan.partial_migrations = merge_two_dicts(migration_plan.partial_migrations, partial_migrations)
            migration_plan.full_migrations = merge_two_dicts(migration_plan.full_migrations, full_migrations)
            
    return (powered_off_servers, migration_plan)

def try_to_consolidate_among_servers(vms_copy):
    consolidated_among_servers = False
    migration_plan = Migration_plan()
    
    vdi_consumption = get_vdi_consumption(vms_copy)
    total_consumption = 0
    powered_up_servers = 0
    for cs in range(nServers, nVDIs):
        total_consumption += vdi_consumption[cs]
        if vdi_consumption[cs] > 0: 
            powered_up_servers += 1
    total_capacity = (powered_up_servers ) * vms_per_vdi
    diff = total_capacity - total_consumption
    how_many_may_be_powered_down = int (diff / vms_per_vdi)
    
    if how_many_may_be_powered_down > 0:
        to_migrate = []
        dest_hosts = []
        for vdi_num in sorted(vdi_consumption):
            if vdi_num in range(nServers, nVDIs):
                if  how_many_may_be_powered_down > 0:
                    to_migrate.append(vdi_num)
                    how_many_may_be_powered_down -= 1
                else:
                    dest_hosts.append(vdi_num)
        hosts_vms = {}
        for vdi in to_migrate:
            hosts_vms[vdi] = []
            for vm_index in range(nVMs):
                if vms_copy[vm_index].curhost == vdi:
                    hosts_vms[vdi].append(vm_index)
        (powered_off_servers, temptative_migration_plan) = consolidate_into_existing_consolidation_servers(vms_copy,to_migrate, hosts_vms, dest_hosts)
        if len(powered_off_servers) > 0: 
            migration_plan = temptative_migration_plan
            consolidated_among_servers = True
    
    return  (consolidated_among_servers, migration_plan)
# s3_flag means whether there are already consolidated host,
# if true, then we use vms
def decide_to_migrate(last_states, cur_sec, migration_schedule, s3_flag, of2):
    global vms
    # get idleness
    vdi_idleness = {}
    vdi_activeness = {}

    #partial_migrate_times = 0
    #full_migrate_times = 0
    
    full_migrations ={}
    partial_migrations = {}
    #resume_migrations = {}
    post_partial_migrations = {}

    rneeded = {}
    
    # get vdi idleness and activeness according to the curhost of the 
    for v in vms:
        vdi_index = v.curhost
        if vdi_index < nServers :  # we only care about the hosts that are not the consolidation host
            if vdi_index not in rneeded:
                rneeded[vdi_index] = 0      
            if v.state == 0:
                rneeded[vdi_index] += v.idle_consumption
            else:
                rneeded[vdi_index] += 1
        
    assert len(rneeded) <= nServers
    
    available_resources = get_available_resources(last_states)
    to_migrate = []
    for vdi_num, consumption in sorted(rneeded.items(), key=lambda x: x[1]):
        if last_states[vdi_num] == S3:
            print vdi_num
            print rneeded
            print last_states
            assert False
        if available_resources >= consumption:
            to_migrate.append(vdi_num)
            available_resources -= consumption
        
    existing_consolidation_servers = []
    for cs in range(nServers, nVDIs):
        if last_states[cs] != S3:
            existing_consolidation_servers.append(cs)
            
    vms_copy = []
    for v in vms:
        vms_copy.append(vm(v.origin,v.curhost,v.state,v.idle_consumption))
        
    hosts_vms1 = {}
    for vdi in to_migrate:
        hosts_vms1[vdi] = []
        for vm_index in range(vdi*vms_per_vdi, (vdi+1)*vms_per_vdi):
            hosts_vms1[vdi].append(vm_index)
    # try to consolidate into the existing consolidation servers.
    (powered_off_servers, migration_plan) = consolidate_into_existing_consolidation_servers(vms_copy, to_migrate, hosts_vms1, existing_consolidation_servers) 
    print "powered_off_servers: ", powered_off_servers
    
    existing_powered_on_servers = []
    for vdi in range(nServers):
        if last_states[vdi] != S3 and vdi not in powered_off_servers:
            existing_powered_on_servers.append(vdi)
    
    wake_cnt = 0
    max_diff = 0
    best_plan = Migration_plan()
    best_vms_copy = None
    consolidatable = False
    sleeping_consolidation_servers = []
    for cs in range(nServers, nVDIs):
        if last_states[cs] == S3:
            sleeping_consolidation_servers.append(cs)
    # after consolidation, then start to see if waking up new servers will get better consolidation opportunity, 
    for cs in sleeping_consolidation_servers:
        wake_cnt += 1
        existing_consolidation_servers.append(cs)
        vms_copy2 = []
        for v in vms_copy:
            vms_copy2.append(vm(v.origin,v.curhost,v.state,v.idle_consumption))
        
        hosts_vms2 = {}
        for vdi in existing_powered_on_servers:
            hosts_vms2[vdi] = []
            for vm_index in range(vdi*vms_per_vdi, (vdi+1)*vms_per_vdi):
                hosts_vms2[vdi].append(vm_index)
        
        (powered_off_servers2, migration_plan) = consolidate_into_existing_consolidation_servers(vms_copy2, existing_powered_on_servers, hosts_vms2, existing_consolidation_servers)
        powered_off_server_num = len(powered_off_servers2)        
        diff = powered_off_server_num - wake_cnt
        if diff > max_diff:
            max_diff = diff
            best_plan = migration_plan
            best_vms_copy = vms_copy2[:]
            consolidatable = True
    consolidated_among_servers = False
    if len(powered_off_servers) == 0 and not consolidatable:
        (consolidated_among_servers, migration_plan) = try_to_consolidate_among_servers(vms_copy)
        full_migrations = migration_plan.full_migrations
        partial_migrations  = migration_plan.partial_migrations
    else:
        if best_vms_copy != None:
            vms = best_vms_copy[:]
        else:
            vms = vms_copy[:]
        # merge the best_plan and migration plan
        full_migrations = merge_two_dicts(migration_plan.full_migrations, best_plan.full_migrations)
        partial_migrations = merge_two_dicts(migration_plan.partial_migrations, best_plan.partial_migrations)   

    #if consolidatable or consolidated_among_servers:
    if True:
        truly_migratable = True
             
        next_states = []
        c = 0
        for s in last_states:
            next_states.append(s)
            c += 1
        # only update the server whose vms have been evacuated
        
        for i in range(nVDIs):
            evacuated = True
            for v in range(nVMs):
                if vms[v].curhost == i:
                    evacuated = False
                    next_states[i] = FULL
                    break
            if evacuated:
                next_states[i] = S3
        return (next_states, truly_migratable, full_migrations, partial_migrations, post_partial_migrations)
    else:
        # return the previous setting
        return (last_states, False, full_migrations, partial_migrations, post_partial_migrations)

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

def pvs(vdi_states):
    last_states = vdi_states[-1]
    for i in range(nVDIs):
        print "VDI %d is %s"%(i,state_str(last_states[i]))
        

def get_overall_state(vdi_states):
    state = "full"
    for i in vdi_states[-1]:    # give priority to migrating or reintegrating
        if i == REINTEGRATING:
            state = "reintegrating"
            break

    if state != "full":
        return state

    for i in vdi_states[-1]:    # check the last states
        if i == S3:
            state = "migrated"
    return state

def update_states(last_states, prevs, nexts):
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
#     c = 0
#     for i in vm_states:
#         vm_states_before_migration[c] = i
#         c += 1

def find_available_slots(host_schedule, length):
    slots = []
    pos = 0
    start = -1
    end = -1
    while pos < len(host_schedule):
        # fnid the start first
        if start == -1:
            if host_schedule[pos] != "r" and host_schedule[pos] != "s":
                pos += 1
                continue
            else:
                start = pos
                pos += 1
        else:
            if host_schedule[pos] != "r" and host_schedule[pos] != "s":
                end = pos - 1
                if (end - start) >= length:
                    slot_start = start
                    slot_end = end
                    slots.append((slot_start,slot_end)) 
                start = -1
                end = -1
            else:                
                pos += 1
                if pos == len(host_schedule) and start != -1:
                    slots.append((start,pos-1)) 
        
    return slots

def slots_contain(target_slot, slots):
    (target_start, target_end) = target_slot
    contain_flag = False
    for (start, end) in slots:
        if start <= target_start and end >= target_end:
            contain_flag = True
            break
    return contain_flag

def find_overlap_slots(slots1, slots2, overlap, max_end):
    target_found = False
    target_slot = None
    i = 0
    while i <= max_end-overlap:
        target_slot = (i, i+overlap-1)
        if slots_contain(target_slot, slots1) and slots_contain(target_slot, slots2):
            target_found = True
            break
        i += 1
    if target_found:
        return target_slot
    else:
        return None

def record_migration(vm_index, src, dest, migrations):
    assert src != dest 
    i = vm_index
    migration_pair = (src, dest)
    if migration_pair in migrations:
        migrations[migration_pair][0] += 1
        migrations[migration_pair][1] += ("-"+str(i))
    else:
        migrations[migration_pair] = [1,str(i)]

def find_migration_slot(src, dst, migration_schedule, migration_latency, interval_length):
    src_slots = find_available_slots(migration_schedule[src], migration_latency)
    dest_slots = find_available_slots(migration_schedule[dst], migration_latency)
    slot_found = find_overlap_slots(src_slots,dest_slots, migration_latency, interval_length)
    return slot_found

def update_schedule(src, dest, vm_index, migration_schedule, slot_found):
    assert slot_found != None
    (start, end) = slot_found
    
    for i in range(start, end+1):
        migration_schedule[src][i] = ('o,'+str(vm_index))
        migration_schedule[dest][i] = ('i,'+str(vm_index))

# this is because the partial migration is not successful, so we need to unupdate the schedule. 
def unupdate_partial_migration(src, dest, partial_slot_found, migration_schedule):
    #recover source schedule
    assert partial_slot_found != None
    (start, end) = partial_slot_found
    for i in range(start, end+1):
        migration_schedule[src][i] = ('r')
        migration_schedule[dest][i] = ('r')


def init_migration_schedule(nVDIs, interval, vdi_states):
    migration_schedule = {}
    # initialized the schedule first, if it is not in the vdi set, then they are sleeping
    for vdi in range(nVDIs):
        migration_schedule[vdi] = []
        for i in range(interval):
            if vdi_states[vdi] == S3 :
                migration_schedule[vdi].append("s") 
            else:
                migration_schedule[vdi].append("r") # for running
                
        assert len(migration_schedule[vdi]) == interval
    return  migration_schedule

def try_to_allocate_full_migration_only(vdi_set, vms_copy, last_states):
    allocatable = False
    if len(vdi_set) == 1:
        return (False, [], {})
    
    vdi_capacity = vms_per_vdi * ( 1 + slack )
    partial_migrations = {}
    reintegrations = {}
    full_migrations ={}
    post_partial_migrations = {}
    
    migration_schedule = init_migration_schedule(nVDIs, vdi_set, interval)
    # dest queue length
    # try to fit the reintegration latency into one interval
    dest_queue = {}
    for i in range(nVDIs):
        dest_queue[i] = 0
    # get the vdi consumption of each vdi
    vdi_consumption = get_vdi_consumption(vms_copy)    

    for i in vdi_set:
        target_capacity = vdi_capacity
        while vdi_consumption[i] > target_capacity:
            # try to kick out the remote idle vms first
            local_idle_vms = []
            active_vms = []   
            for j in range(nVMs):
                v = vms_copy[j]
                if v.curhost == i and v.state == 0 and v.origin == i:
                    local_idle_vms.append(j)
                elif v.curhost == i and v.state != 0 and v.origin == i:
                    active_vms.append(j)

            if vdi_consumption[i] > target_capacity:
                # now to kick out the active vms
                for v in  active_vms +local_idle_vms:
                    vm_index = v
                    # find dest.
                    for k in vdi_set:
                        if i == k:
                            continue
                        else:
                            src = vms_copy[vm_index].curhost
                            dest = k
                            # see if it fits
                            if vdi_consumption[k] + 1 <= vdi_capacity:
                                slot_found = find_migration_slot(src, dest, migration_schedule, full_migrate, interval)
                                if slot_found != None:
                                    record_migration(vm_index, src, dest, full_migrations)
                                    update_schedule(src, dest, vm_index, migration_schedule, slot_found)
                                    vms_copy[vm_index].curhost = k
                                    vms_copy[vm_index].origin = k
                                    
                                    if  vms_copy[vm_index].state != 0:
                                        vdi_consumption[k] += 1
                                        vdi_consumption[i] -= 1
                                    else:
                                        vdi_consumption[k] += 0.1
                                        vdi_consumption[i] -= 0.1
                                break
                    else:
                        available_slots = find_available_slots(migration_schedule[i], full_migrate)
                        if len(available_slots) == 0:
                            target_capacity = vdi_consumption[i] 
                        else:
                            # meaning we have not found k that hosts the v
                            allocatable = False
                            return (allocatable, [], {})
                    # here means the loop has found k, then test whether there is still overloaded vdis
                    if vdi_consumption[i] <= target_capacity:
                        break
                else:
                    print "Evacuating all active VMs still didn't find the solution, probably because the network is congested. "
                    print "making space for host ", i
                    print "Migration schedule: ", migration_schedule[i]
                    print "How many free slots: ", free_slot_count(migration_schedule[i])
                    print "vdi_set: ", vdi_set
                    print "vdi_consumption[i]: ", vdi_consumption[i]
                    print "idle vms: ", len(local_idle_vms) 
                    print "+ active_vms: ",  len(active_vms)
                    #assert False
                    vdi_consumption[i] = target_capacity
    else:                       
        # loop exits normally, then
        allocatable = True
    for i in range(nVDIs):
        assert dest_queue[i] <= interval 

    return (allocatable, [partial_migrations, reintegrations, full_migrations, post_partial_migrations], migration_schedule)

# return how many "r" count there is in 
def free_slot_count(host_schedule):
    count = len(host_schedule)
    r_cnt = 0
    for i in range(count):
        if host_schedule[i] == "r":
            r_cnt += 1
    return r_cnt

def assert_latency(latency, latency_set, host_schedule, starting_index):
    if latency not in latency_set:
        print "latency ", latency, " not in set: ", latency_set
        print host_schedule
        print "starting_index ", starting_index
        print  "event ", host_schedule[starting_index]
        assert False

# make sure the schedule is correct for all in or out vms        
def validate_schedule(migration_schedule, nVDIs, interval, possible_latencies):
    for i in range(nVDIs):
        host_schedule = migration_schedule[i]
        last_event = ""
        last_index = -1
        for j in range(interval):
            event = host_schedule[j]
            if event != "s" and event != "r":
                if last_event == "" and last_index == -1:
                    last_event = event
                    last_index = j
                elif event == last_event:
                    continue
                else:
                    difference = j - last_index 
                    assert_latency(difference, possible_latencies, migration_schedule[i], last_index) 
                    last_event = event
                    last_index = j
                    
            else:
                if last_event != "" and last_index != -1:
                    difference = j - last_index 
                    assert_latency(difference, possible_latencies, migration_schedule[i], last_index) 
                    last_event = ""
                    last_index = -1
def debug_migration(vm_index, src, dest, anchor_point):
    
    print "Migrating vm %d from host %d to %d Point "%(vm_index, src, dest), anchor_point
        
def debug_vm(vms_copy, anchor_point):
    print "vm_copy[2].curhost is %d at point"%(vms_copy[2].curhost), anchor_point
# best_effort means whether we are waking up the maximum number of servers   
def try_to_allocate(migration_schedule, consolidation_server_set, newly_wakeup_servers, vms_copy, last_states, best_effort):
    allocatable = False
    
    vdi_capacity = vms_per_vdi
    partial_migrations = {}
    reintegrations = {}
    full_migrations ={}
    post_partial_migrations = {}
    possible_latencies = []
    possible_latencies.append(partial_migrate)
    possible_latencies.append(partial_resume)
    possible_latencies.append(full_migrate)
    possible_latencies.append(full_migrate+partial_migrate)
    # dest queue length
    # try to fit the reintegration latency into one interval
    dest_queue = {}
    for i in range(nVDIs):
        dest_queue[i] = 0

    # if there is a newly woke-up vdi server, then resume all of its remote partials  (becoming active), remote partails that are still idle, and then active VMs
    for server_index in newly_wakeup_servers:
        #assert last_states[server_index] == S3
        remote_partials_remaining_idle = []
        remote_partials_becoming_active = []
        full_vms = []
        for j in range(server_index * vms_per_vdi, (server_index + 1)* vms_per_vdi):
            v = vms_copy[j]
            if v.origin != server_index: # this is a fully migrated VM
                full_vms.append(j)
            elif v.curhost != server_index and v.state != 0:
                remote_partials_becoming_active.append(j)
            elif v.curhost != server_index and v.state == 0:
                remote_partials_remaining_idle.append(j)
        
        # we prioritize the remote_partials_becoming_active vms first because we want them to experience the least latency
        for j in remote_partials_becoming_active + remote_partials_remaining_idle:
            vm_index = j
            v = vms_copy[j]
            src = v.curhost
            dest = v.origin
            assert v.origin == server_index
            slot_found = find_migration_slot(src, dest, migration_schedule, partial_resume, interval)
            if slot_found != None:
                record_migration(j, src, dest, reintegrations)
                update_schedule(src, dest, vm_index, migration_schedule, slot_found)
                #validate_schedule(migration_schedule, nVDIs, interval, possible_latencies)
                vms_copy[j].curhost = server_index
            else:
                print "Slot not found for partial VMs. Because the migration_schedule (either the source or dest) is full!" 
                break
        
        for j in full_vms:
            vm_index = j
            v = vms_copy[j]
            src = v.curhost
            dest = server_index
            assert v.origin != server_index
            slot_found = find_migration_slot(src, dest, migration_schedule, full_migrate, interval)
            if slot_found != None:
                record_migration(j, src, dest, full_migrations)
                update_schedule(src, dest, vm_index, migration_schedule, slot_found)
                #validate_schedule(migration_schedule, nVDIs, interval, possible_latencies)
                vms_copy[j].curhost = server_index
                vms_copy[j].origin = server_index
            else:
                print "Slot not found for full VMs. Because the migration_schedule (either the source or dest) is full!" 
                break
        
    #validate_schedule(migration_schedule, nVDIs, interval, possible_latencies)
    
    # get the vdi consumption of each vdi
    vdi_consumption = get_vdi_consumption(vms_copy)  
    
    for i in consolidation_server_set:
        print "host %d in vdi_set" %i
        print "vdi_set size: ", len(consolidation_server_set) 
        target_capacity = vdi_capacity
        while vdi_consumption[i] > target_capacity:
            local_idle_vms = []
            remote_partials_remaining_idle = []
            remote_partials_becoming_active = []
            active_vms = []
            for j in range(nVMs):
                v = vms_copy[j]
                if v.curhost == i and v.state == 0 and v.origin == i:
                    local_idle_vms.append(j)
                elif v.curhost == i and v.state != 0 and v.origin == i:
                    active_vms.append(j)
                elif v.curhost == i and v.state == 0 and v.origin != i:
                    remote_partials_remaining_idle.append(j)
                elif v.curhost == i and v.state != 0 and v.origin != i: # remote partials that become active
                    remote_partials_becoming_active.append(j)
            
            for j in remote_partials_becoming_active:
                if True:        # I am lazy to fix the indent
                    # find dest.
                    for k in consolidation_server_set:
                        if i != k:
                            origin = vms_copy[j].origin
                            assert i == vms_copy[j].curhost                            
                            vm_consumption = 1                   # it is active
                            if origin == k:                      # exit the loop. there is no time to handle this any more
                                target_capacity = vdi_consumption[i] 
                                break 
                            if vdi_consumption[k] + vm_consumption <= vdi_capacity:
                                src = i
                                dest = k
                                vm_index = j
                                assert src != dest                                
                                partial_slot_found = find_migration_slot(src, dest, migration_schedule, partial_migrate, interval)
                                if partial_slot_found != None:
                                    # record the old schedule. If not successfully allocate the post partial migration, then we recover the old schedule 
                                    old_src_schedule = []
                                    old_dest_schedule = []
                                    for sec in range(interval):
                                        old_src_schedule.append(migration_schedule[src][sec][:])
                                        old_dest_schedule.append(migration_schedule[dest][sec][:])
                                    # update the schedule first, then try to do post_partial_migration
                                    assert src != dest                                                                        
                                    update_schedule(src, dest, vm_index, migration_schedule, partial_slot_found)                                                                            
                                    post_partial_slot_found = find_migration_slot(origin, dest, migration_schedule, full_migrate,interval)
                                    if post_partial_slot_found != None: 
                                        print "post_partial_slot_found is ", post_partial_slot_found
                                        vdi_consumption[k] += vm_consumption
                                        vdi_consumption[i] -= vm_consumption
                                        print "src ", src, "  dest ", dest
                                        record_migration(j, src, dest, partial_migrations)
                                        record_migration(j, origin, dest, post_partial_migrations)
                                        update_schedule(vms_copy[j].origin, k, vm_index, migration_schedule, post_partial_slot_found)
                                        #validate_schedule(migration_schedule, nVDIs, interval, possible_latencies)
                                        vms_copy[j].origin = k # !!! update its origin, do post-partial migrations! 
                                        vms_copy[j].curhost = k
                                        break
                                    else:
                                        #print "recover the old schedule" 
                                        for sec in range(interval):
                                            migration_schedule[src][sec] = old_src_schedule[sec]
                                            migration_schedule[dest][sec] = old_dest_schedule[sec]
                    
                    if vdi_consumption[i] <= target_capacity:
                        break
                    # here means the loop has found k, then test whether there is still overloaded vdis
                    elif  len(find_available_slots(migration_schedule[i], partial_migrate)) == 0:
                        target_capacity = vdi_consumption[i]
                        break
                    
            available_slots = find_available_slots(migration_schedule[i], partial_migrate)
            max_space_to_make = 0
            for (start, end) in available_slots:
                nSecs = end - start + 1
                max_space_to_make += nSecs/full_migrate * 1
                max_space_to_make += ((nSecs%full_migrate) / partial_migrate) * 0.1   
                
            consumption_diff = vdi_consumption[i]  - target_capacity
            if max_space_to_make < consumption_diff:
                target_capacity = vdi_consumption[i] - max_space_to_make
            
            if vdi_consumption[i] > target_capacity:
                # now to kick out the remote partials vms that remain idle
                for v in remote_partials_remaining_idle:
                    vm_index = v
                    # find dest.
                    for k in consolidation_server_set:
                        if i == k:
                            continue
                        else:
                            src = vms_copy[vm_index].curhost
                            dest = k 
                            assert src != dest 
                            # see if it fits
                            if vdi_consumption[k] + vms_copy[vm_index].idle_consumption <= vdi_capacity:
                                partial_slot_found = find_migration_slot(src, dest, migration_schedule, partial_migrate,interval)
                                if partial_slot_found != None:
                                    vdi_consumption[k] += vms_copy[vm_index].idle_consumption
                                    vdi_consumption[i] -= vms_copy[vm_index].idle_consumption
    
                                    record_migration(vm_index, src, dest, partial_migrations)
                                    assert src != dest
                                    update_schedule(src, dest, vm_index, migration_schedule, partial_slot_found)
                                    #validate_schedule(migration_schedule, nVDIs, interval, possible_latencies)
                                    vms_copy[vm_index].curhost = k                                    
                                    break
                        
                    if vdi_consumption[i] <= target_capacity:
                        break
                    # here means the loop has found k, then test whether there is still overloaded vdis
                    elif  len(find_available_slots(migration_schedule[i], partial_migrate)) == 0:
                        target_capacity = vdi_consumption[i]
                        break
            #validate_schedule(migration_schedule, nVDIs, interval, possible_latencies)
            available_slots = find_available_slots(migration_schedule[i], full_migrate)
            if len(available_slots) == 0:
                target_capacity = vdi_consumption[i]            
            if vdi_consumption[i] > target_capacity:
                # now to kick out the active vms
                for v in active_vms + local_idle_vms:
                    vm_index = v
                    # find dest.
                    for k in consolidation_server_set:
                        if i == k:
                            continue
                        else:
                            src = vms_copy[vm_index].curhost
                            dest = k
                            # see if it fits
                            if vdi_consumption[k] + 1 <= vdi_capacity:
                                slot_found = find_migration_slot(src, dest, migration_schedule, full_migrate,interval)
                                if slot_found != None:
                                    record_migration(vm_index, src, dest, full_migrations)
                                    update_schedule(src, dest, vm_index, migration_schedule, slot_found)
                                    vms_copy[vm_index].curhost = k
                                    vms_copy[vm_index].origin = k
                                    vdi_consumption[k] += 1
                                    vdi_consumption[i] -= 1
                                break
                    if vdi_consumption[i] <= target_capacity:
                        break
                    # here means the loop has found k, then test whether there is still overloaded vdis
                    elif  len(find_available_slots(migration_schedule[i], full_migrate)) == 0:
                        target_capacity = vdi_consumption[i]
                        break

            if vdi_consumption[i] > target_capacity:
                # meaning we have not found k that hosts the v
                if len(find_available_slots(migration_schedule[i], full_migrate)) > 0:
                    print_host_schedule(migration_schedule[i])
                    print "Failed to allocate the resources. Probably need to wake up one more VDI"
                    print "vdi_set size: ", len(consolidation_server_set), " nVDIs", nVDIs
                    if len(consolidation_server_set) < nVDIs:
                        print "len(vdi_set) < nVDIs is True"
                    if not best_effort:
                        print  "len(newly_wakeup_servers): ", len(newly_wakeup_servers)
                        print last_states
                        allocatable = False
                        return (allocatable, [], {})
                    else:
                        target_capacity = vdi_consumption[i]
                else:
                    target_capacity = vdi_consumption[i]
    else:                       
        # loop exits normally, then
        allocatable = True
    for i in range(nVDIs):
        assert dest_queue[i] <= interval 

    return (allocatable, [partial_migrations, reintegrations, full_migrations, post_partial_migrations], migration_schedule)

def get_cs_consumption(vms_copy):
    vdi_consumption = []
    full_active_cnt = []
    full_idle_cnt = []
    partial_cnt = []
    # see if the capacity is exceeded 
    for i in range(0, nVDIs):   # init 
        vdi_consumption.append(0)
        full_idle_cnt.append(0)
        full_active_cnt.append(0)
        partial_cnt.append(0)
    for i in range(0, nVMs):
        vdi_index = vms_copy[i].curhost
        if vms_copy[i].state == 0 and vms_copy[i].curhost != vms_copy[i].origin: # only remote partials exists
            vdi_consumption[vdi_index] += vms_copy[i].idle_consumption
            partial_cnt[vdi_index]  += 1
        else:
            vdi_consumption[vdi_index] += 1
            if vms_copy[i].state == 0: 
                full_idle_cnt[vdi_index]  += 1
            else:
                full_active_cnt[vdi_index]  += 1
    return (vdi_consumption, partial_cnt, full_idle_cnt, full_active_cnt)  

# get the vdi_consumption based on the vms_copy. 
def get_vdi_consumption (vms_copy):
    vdi_consumption = []
    # see if the capacity is exceeded 
    for i in range(0, nVDIs):   # init 
        vdi_consumption.append(0)
    for i in range(0, nVMs):
        vdi_index = vms_copy[i].curhost
        if vms_copy[i].state == 0 and vms_copy[i].curhost != vms_copy[i].origin: # only remote partials exists
            vdi_consumption[vdi_index] += vms_copy[i].idle_consumption
        else:
            vdi_consumption[vdi_index] += 1
    return vdi_consumption

# only update the previous S3 state to reintegrating if any VDIs are woken up
def get_next_states(last_states, vms_copy = None):
    global vms
    if vms_copy == None:
        vms_copy = vms[:]
    next_states = []
    for i in range(nVDIs):
        if last_states[i] == S3:
            state_to_append = S3
            for v in vms_copy:
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

def reintegrate_newly_active_remote_idles(updated_states):
    global vms 
    resume_migrations = {}
    
    for i in range(nVMs):
        if vms[i].state > 0 and vms[i].curhost != vms[i].origin and updated_states[vms[i].origin] == FULL: 
            vm_index = i
            src = vms[i].curhost
            dst = vms[i].origin
            migration_pair = (src, dst)
            if migration_pair in resume_migrations:
                resume_migrations[migration_pair][0] += 1
                resume_migrations[migration_pair][1] += ("-"+str(vm_index))
            else:
                resume_migrations[migration_pair] = [1,str(vm_index)]
            #vms[i].curhost = vms[i].origin

    #return resume_migrations
    return {}

# assert that the nServer consumption is less than the capacity
def assert_consumptions(vdi_consumption, nServers, capacity):
    for i in range(nServers):
        assert vdi_consumption[i] <= capacity

def refresh_vdi_states(vdi_states):
    global vms
    refreshed_states = []
    last_states = vdi_states[-1]
    assert len(last_states) == nVDIs
    # update the vdi last states if one of those are migrating. 
    (last_states, newly_sleep_vdis) = update_last_states(last_states)
    # if an remote idle VMs becomes active and its origin is still on, then we should reintegrate it immediately 
    bounce_resume_migrations = reintegrate_newly_active_remote_idles(last_states)
    if len(bounce_resume_migrations) > 0:
        for (src,dst) in bounce_resume_migrations:
            #of6.write("%d,%d,%d,%d,%s\n"%(cur_sec,src,dst,bounce_resume_migrations[(src,dst)][0],bounce_resume_migrations[(src,dst)][1]))
            pass

    assert nobody_is_migrating(last_states)
    
    for i in range(nVDIs):
        refreshed_states.append(last_states[i])

    if len(refreshed_states) != nVDIs:
        print "len(refreshed_states) =", len(refreshed_states)
        assert False
    return (refreshed_states, bounce_resume_migrations)

def post_partial_migrate(vms_copy, migration_schedule, need_post_partial_migrations, last_states):
    post_partial_migrations = {}
    for server_index in range(nServers, nVDIs):
        consolidation_server_queue = need_post_partial_migrations[server_index]
        while not consolidation_server_queue.empty():
            waiting_vm = consolidation_server_queue.get()
            vm_index = waiting_vm.vm_index
            src = vms_copy[vm_index].origin
            dest = vms_copy[vm_index].curhost
            if src != dest: # this VM may have been handled in the try_to_resume
                slot_found = find_migration_slot(src, dest, migration_schedule, full_migrate, interval)
                if slot_found != None:
                    record_migration(vm_index, src, dest, post_partial_migrations)
                    update_schedule(src, dest, vm_index, migration_schedule, slot_found)
                    #validate_schedule(migration_schedule, nVDIs, interval, possible_latencies)
                    vms_copy[vm_index].origin =  vms_copy[vm_index].curhost
                else:
                    consolidation_server_queue.put(waiting_vm)
                    break
    return (migration_schedule, post_partial_migrations)

def kickout_full_idle_vms(vms_copy):
    global vms_per_vdi
    full_migrations = {}
    partial_migrations = {}
    for cs in range(nServers, nVDIs):
        for vm_index in range(nVMs): 
            v = vms_copy[vm_index]
            if v.curhost == cs and v.origin == cs and v.state == 0:
                home_host = vm_index / vms_per_vdi
                src = v.curhost
                dest = home_host
                record_migration(vm_index, src, dest, full_migrations)
                src = home_host
                dest = v.curhost
                record_migration(vm_index, src, dest, partial_migrations)
                v.origin = home_host
    return (full_migrations, partial_migrations)

def merge_migrations(dest_migrations, src_migrations):
    for pair in src_migrations:
        if  pair not in dest_migrations:
            dest_migrations[pair] = src_migrations[pair]
        else:
            dest_migrations[pair][0] += src_migrations[pair][0]
            dest_migrations[pair][1] += ("-"+src_migrations[pair][1])

def decide_to_resume(last_states, cur_sec, migration_schedule, provision_latencies, vm_transitions_to_handle, need_post_partial_migrations, of2, of5, of6, of11):
    global configs, vms
    assert len(vms) == nVMs

    full_migrations = {}
    partial_migrations = {}
    resume_migrations = {}
    post_partial_migrations = {}
    
    optimized_full_migrations = {}
    optimized_partial_migrations = {}

    resume = False
    vdis_to_resume = {}
    consolidation_server_set = []
    
    # we turn the full idle vms into remote partials
    if optimization == "yes":
        vms_copy = []
        for v in vms:
            vms_copy.append(vm(v.origin,v.curhost,v.state,v.idle_consumption))
        (optimized_full_migrations, optimized_partial_migrations) = kickout_full_idle_vms(vms_copy)
        del vms[:]
        vms = vms_copy[:]
    
    vdi_consumption = get_vdi_consumption (vms)
    
    assert_consumptions(vdi_consumption, nServers, vms_per_vdi)
    
    # total resource available, positive means the existing vdi can accommadate the vms,
    # negative means we need to wake up at least one more vdis to migrate the vms
    total_resource = 0 
    resource_available = {}
 
    # scan the consolidation server list and see if any vdi exceeds the capacity
    for i in range(nServers, nVDIs):
        consolidation_server_set.append(i)
        if vdi_consumption[i] > vms_per_vdi * (slack + 1):
            resume = True
            vdis_to_resume[i] = True
        else:
            vdis_to_resume[i] = False
        if vdi_consumption[i] < vms_per_vdi:
            resource_available[i] = vms_per_vdi - vdi_consumption[i]
        else:
            resource_available[i] = 0
            
    next_states = []
    if resume:  # if not resume, then we need to reintegrate the idle VMs that turn into active
        print "Decide to resume"
        assert len(vdis_to_resume) > 0
        wake_vdis = False
        if total_resource >= 0:
            # try to allocate 
            allocatable = False
            vms_copy = []
            for v in vms:
                vms_copy.append(vm(v.origin,v.curhost,v.state,v.idle_consumption))
            allocatable =False 
            migrations =[]
            existing_servers = []
            for i in range(nServers):
                if last_states[i] != S3:
                    existing_servers.append(i)
            if method == "oasis":                
                (allocatable, migrations, migration_schedule) = try_to_allocate(migration_schedule, consolidation_server_set, existing_servers, vms_copy, last_states, False)
            elif method == "full":
                (allocatable, migrations, migration_schedule) = try_to_allocate_full_migration_only(migration_schedule, consolidation_server_set, vms_copy, last_states)
            if allocatable:
                #(partial_migrate_times, full_migrate_times, partial_resume_times, full_migrations, partial_migrations, resume_migrations, post_partial_migration_times, post_partial_migrations) = account_migration_times(vms_copy)
                del vms[:]
                vms = vms_copy[:]
                next_states = get_next_states(last_states, vms_copy)
                assert_states(cur_sec, next_states, vms_copy, "after get_next_states 1 ")
                partial_migrations = migrations[0]
                resume_migrations  = migrations[1]
                full_migrations  = migrations[2] 
                post_partial_migrations  = migrations[3]
            else:
                partial_migrations.clear()
                resume_migrations.clear()
                full_migrations.clear() 
                post_partial_migrations.clear()
                wake_vdis = True
                migration_schedule.clear()
                migration_schedule = init_migration_schedule(nVDIs, interval, last_states)
        else:
            wake_vdis = True
        
        if wake_vdis:
            # wake up the vdi nServers
            # sort the sleeping vdi, pick the one with the most remote partial vms
            sleeping_vdis = {}
            wake_cnt = 1
            for i in range(nVDIs):
                if last_states[i] == S3:
                    sleeping_vdis[i] = 0
            for v in vms:
                if v.origin != v.curhost and last_states[v.origin] == S3:
                    assert sleeping_vdis[v.origin] >= 0
                    sleeping_vdis[v.origin] += 1

            found_solution = False
            newly_wakenup_vdi = []
            for vdi_index, v in sorted(sleeping_vdis.items(), key=lambda x: x[1], reverse=True):
                allocatable = False
                if vdi_index not in newly_wakenup_vdi:
                    newly_wakenup_vdi.append(vdi_index)
                else:
                    assert False
                vms_copy = []
                # copy the whole thing to a copy
                for v in vms:
                    vms_copy.append(vm(v.origin,v.curhost,v.state,v.idle_consumption))
                allocatable =False 
                migrations =[]
                wake_cnt += 1
                best_effort = False
                if wake_cnt == len(existing_servers) + len(sleeping_vdis):
                    best_effort = True

                if method == "oasis":                    
                    (allocatable, migrations, migration_schedule) = try_to_allocate(migration_schedule, consolidation_server_set, existing_servers + newly_wakenup_vdi, vms_copy, last_states, best_effort)
                elif method == "full":
                    (allocatable, migrations, migration_schedule) = try_to_allocate_full_migration_only(migration_schedule, consolidation_server_set, vms_copy, last_states)
                if allocatable:
                    del vms[:]
                    vms = vms_copy[:]
                    next_states = get_next_states(last_states,vms_copy)
                    assert_states(cur_sec, next_states, vms_copy, "after get_next_states 2 ")
                    found_solution = True
                    partial_migrations = migrations[0]
                    resume_migrations  = migrations[1]
                    full_migrations  = migrations[2] 
                    post_partial_migrations  = migrations[3]
                    assert (len(partial_migrations) + len(resume_migrations) + len(full_migrations) + len(post_partial_migrations) ) > 0
                    break
                else:
                    partial_migrations ={}
                    resume_migrations = {}
                    full_migrations = {}
                    post_partial_migrations = {}
                    migration_schedule.clear()
                    migration_schedule = init_migration_schedule(nVDIs, interval, last_states)

            assert found_solution
        
        # now we post partial migrate the vms that are waiting in the queue
        if True:
            vms_copy = []
            for v in vms:
                vms_copy.append(vm(v.origin,v.curhost,v.state,v.idle_consumption))
            (migration_schedule, post_partial_migrations) = post_partial_migrate(vms_copy, migration_schedule, need_post_partial_migrations, last_states)
            del vms[:]
            vms = vms_copy[:]    
            calculate_provision_latency(cur_sec, nVDIs, vm_transitions_to_handle, resource_available, migration_schedule, provision_latencies, of11)
    else:
        for i in range(0, nVDIs):
            next_states.append(last_states[i])
    #print "next_states :", next_states
    assert len(next_states) == nVDIs
    assert_states(cur_sec, next_states, vms, "before return try to allocate")
    merge_migrations(full_migrations, optimized_full_migrations)
    merge_migrations(partial_migrations, optimized_partial_migrations)
    
    return (next_states, resume, full_migrations, partial_migrations, resume_migrations, post_partial_migrations)

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

def make_decision(vm_states,vdi_states, cur_sec, provision_latencies, vm_transitions_to_handle, need_post_partial_migrations, of2, of5, of6, of11):
    global configs,vms
    global migration_interval,reintegration_interval
    global cumulative_interval,cumulative_interval2
    next_states = []
    
    full_migrations = {}
    partial_migrations = {}
    resume_migrations = {}
    post_partial_migrations = {}

    migration_cause = None
    overall_state = get_overall_state(vdi_states)

    (refreshed_states, bounce_resume_migrations) = refresh_vdi_states(vdi_states)
    
    migration_schedule = init_migration_schedule(nVDIs, interval, refreshed_states)
    
    # refresh the states, make sure nobody is migrating any more. 
    if len(refreshed_states) != nVDIs:
        print "len(refreshed_states) =", len(refreshed_states)
        assert False
    
    if overall_state == "full" or overall_state == "migrated":
        # update the vdi states first. If there are any nServers that are in migrating state, but either switch it to S3 or back to full
        (next_states, resume, full_migrations, partial_migrations, resume_migrations, post_partial_migrations) = decide_to_resume(refreshed_states, cur_sec, migration_schedule, provision_latencies, vm_transitions_to_handle, need_post_partial_migrations, of2, of5, of6, of11)
        assert_states(cur_sec, next_states, vms, "after decide_to_resume ")
        if resume == True:
            migration_cause = "Resume"
            (nActives, nIdles) = get_reintegrating_vdi_stats(next_states)
            reintegration_interval = get_reintegration_interval(partial_migrations, full_migrations, post_partial_migrations, resume_migrations)
            if reintegration_interval <= 0:
                print "no migrations happen in this interval: %d. This is weird." % cur_sec
                #assert False
            cumulative_interval2 = 0
            
        else:
            full_migrations.clear()
            partial_migrations.clear()
            resume_migrations.clear()
            post_partial_migrations.clear()
            
            # No matter whether we need to resume, we include bounce back migrations in resume migrations
            if len(bounce_resume_migrations) > 0:
                for migration_pair in bounce_resume_migrations:
                    if migration_pair not in resume_migrations:
                        resume_migrations[migration_pair] = bounce_resume_migrations[migration_pair]
                    else:
                        resume_migrations[migration_pair][0] += bounce_resume_migrations[migration_pair][0]
                        resume_migrations[migration_pair][1] += ("-" + bounce_resume_migrations[migration_pair][1])
            
            # decide to whether to migrate again
            (next_states, decision,full_migrations, partial_migrations, post_partial_migrations) = decide_to_migrate(refreshed_states, cur_sec, migration_schedule, True, of2)
            assert_states(cur_sec, next_states, vms, "after decide_to_migrate ")
            if decision == True:
                migration_cause = "consolidation" 
                (nActives, nIdles) = get_migrating_vdi_stats(next_states)
                # how many intervals it takes to migrate all VMs

                migration_interval = get_migration_interval(partial_migrations, full_migrations, post_partial_migrations)
                assert migration_interval > 0
                cumulative_interval = 0
                record_vm_states(vm_states)
            
    if overall_state == "reintegrating":
        migration_cause = "reintegrating"
        #print migration_cause, cur_sec
        assert_states(cur_sec, refreshed_states, vms, "before update_states ")
        assert  cumulative_interval2 >= 0  and cumulative_interval2 <= reintegration_interval
        if cumulative_interval2 + 1 < reintegration_interval:
            cumulative_interval2 += 1
            next_states = refreshed_states
        else:
            cumulative_interval2 = 0
            next_states = update_states(refreshed_states, REINTEGRATING, FULL)

    plan = Migration_plan()
    plan.cur_sec = cur_sec
    plan.migration_cause = migration_cause
    plan.full_migrations = full_migrations
    plan.post_partial_migrations = post_partial_migrations
    plan.partial_migrations = partial_migrations
    plan.resume_migrations = resume_migrations
    update_migration_times(plan)

    if len(next_states) != nVDIs:
        print next_states
        print cur_sec
        assert False
    return (next_states,plan)

def update_migration_times(migration_plan):
    full_migration_times = 0 
    post_partial_migration_times = 0
    resume_migration_times = 0 
    partial_migration_times = 0 

    for pair in migration_plan.full_migrations: 
        full_migration_times += (migration_plan.full_migrations[pair][0])
    for pair in migration_plan.partial_migrations: 
        partial_migration_times += (migration_plan.partial_migrations[pair][0])
    for pair in migration_plan.resume_migrations: 
        resume_migration_times += (migration_plan.resume_migrations[pair][0])
    for pair in migration_plan.post_partial_migrations: 
        post_partial_migration_times += (migration_plan.post_partial_migrations[pair][0])

    migration_plan.post_partial_migration_times = post_partial_migration_times
    migration_plan.resume_migration_times = resume_migration_times
    migration_plan.partial_migration_times = partial_migration_times
    migration_plan.full_migration_times = full_migration_times

def run_experiment(inputs, output_str):
    cnt = 0
    tsaving = 0.0
    tbw = 0 
    apl = 0
    stdpl = 0 
    maxpl = 0
    for inf in inputs.rstrip().split(","):
        if inf != '':
            outf = inf+output_str
            (saving, bandwidth, provision_latencies,the_rest_latencices,active_becomes_idle)  = run(inf,outf)
            tsaving += saving
            cnt += 1
            tbw += bandwidth
            apl = numpy.median(provision_latencies)
            stdpl = numpy.std(provision_latencies)
            maxpl = max(provision_latencies)
    ave_saving = tsaving / cnt 
    ave_bw = float(tbw)/cnt
    return (ave_saving, ave_bw, apl, stdpl, maxpl)

if __name__ == '__main__':

    policy_type = configs['migration_policy_type']
    
    if policy_type == "static":
        timestr = time.strftime("%Y-%m-%d-%H-%M-%S")        
        of = "data/static-all-result-"+timestr+".csv"
        f = open(of, "w+")
        header =  "Users, Approach, Slack Threshold, VDI Server#+Consolidation Server#, Power Saving(wd),Bandwidth(GB), Average Provision Latency(s), Mean Provision Latency(s), Provision Latency Standard Deviaion(s), Max Provision Latency(s), Active Becomes Idle Count, Provision Latency Count, All Transition Counts, Average Provision Latency(s), Median Provision Latency(s)\n"
        f.write(header)
        
        sts = configs['slacks'].rstrip().split(",")
        tts = configs['tightnesses'].rstrip().split(",")
        for i in range(0, len(tts)):
            configs['slack'] = float(sts[i])
            configs['tightness'] = float(tts[i])
            configs["dayofweek"] = "weekday"
            slack = float(sts[i])
            tightness = float(tts[i])
            dayofweek = "weekday"
            input_file = configs["inputs-weekday"]
            experiment_users = str(configs['nVMs'])
            nServers_splits = str(configs['nServers']).split(",")
            nConsolidationServers_splits = str(configs['nConsolidationServers']).split(",")
            # output the latency
            cnt = 0 
            for users in experiment_users.split(","):
                # update the global variables
                nVMs = int(users)
                nServers = int(nServers_splits[cnt])
                nConsolidationServers = int(nConsolidationServers_splits[cnt])
                vms_per_vdi = nVMs / nServers
                nVDIs = nServers + nConsolidationServers
                cnt += 1
                
                print "nVMs=", nVMs
                vm_host_map = random.sample(range(0, allVMs), nVMs)
                output_file = input_file+ ("-%d-users"%nVMs) + timestr + ".slack-%.1f"%(1-float(tts[i]))
                (saving, bandwidth, provision_latencies, the_rest_latencices, active_becomes_idle) = run(input_file, output_file)
                mpl = numpy.median(provision_latencies)
                apl = numpy.mean(provision_latencies)
                stdpl = numpy.std(provision_latencies)
                maxpl = 0 
                if len(provision_latencies) != 0:
                    maxpl = max(provision_latencies)
                provision_latency_cnt = len(provision_latencies)
                all_cnt = len(provision_latencies + the_rest_latencices)
                all_mpl = numpy.median(provision_latencies + the_rest_latencices)
                all_apl = numpy.mean(provision_latencies + the_rest_latencices)
                oline = "%d,%s,%.1f,%d+%d,"%(nVMs, method, 1-configs['tightness'], nServers, nConsolidationServers)
                oline += "%f,%.1f, %.1f,%.1f,%.1f,%.1f,%d,%d,%d,%.1f,%.1f\n"% (saving, bandwidth, apl, mpl, stdpl, maxpl, active_becomes_idle, provision_latency_cnt, all_cnt, all_apl, all_mpl)                
                f.write(oline)
                latency_outf = open("data/latencies/Provision-Latencies"+("-%d-users"%nVMs)  + timestr+".csv", "w+")
                for l in provision_latencies:
                    latency_outf.write("%f\n"%l)
                latency_outf.close()
                
        f.close()
        print "Done. Result is in %s"%of













