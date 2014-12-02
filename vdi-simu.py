#!/usr/bin/env python

import ConfigParser
import math
import linecache
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

def run(i, outf):
    global configs
    nVMs = int(configs['nVMs'])
    nVDIs = int(configs['nVDIs'])
    interval = int(configs['interval'])
    vms_per_vdi = nVMs/nVDIs

    full_power = float(configs['full_power']) 
    low_power =  float(configs['low_power'])

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
    print_cnt2 = 0
    vm_active_vdi_migrated_secs = 0

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
        for j in range(0, nVMs):
            vm_states[j] = 0
        # evaluate the current situation
        for i in range(0, nVMs):
            a = int(activities[i].lstrip())
            assert a >= 0
            if a >= 1:
                active_secs += 1
                vm_states[i] = 1
                vdi_activeness[i/vms_per_vdi] += (1.0/float(vms_per_vdi))
                if cur_vdi_states[i/vms_per_vdi] == S3:
                    vm_active_vdi_migrated_secs += 1
            else:
                vm_states[i] = 0
        if cur_sec > 3*60*60* + 5  and print_cnt2 > 0:
            print "vm_states: %s" % vm_states
            print "line is %s" % line
            print_cnt2 -= 1

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

    total_power = 0
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
                total_power += full_power
            else:
                total_power += low_power
            if v != nVDIs-1:
                o+=","
        o +=",%d,%f"%(vdis_in_full, a1)
        of.write(o+"\n")
        i += 1
    # print "total state num: %d" % len(vdi_states)
    print "Total active seconds: %d" % active_secs 
    of.write("Total active seconds: %d" % active_secs + "\n")
    of.seek(0,0)                # write to the beginning
    of.write("Total power consumption: %f Joule\n" % total_power) 
    power_saving = 1 - (total_power /(86400 * full_power * nVDIs))
    of.write("Total power saving: %f\n" % power_saving) 
    # rate of all the active seconds when there active VMs running on consolidated hosts
    rate = float(vm_active_vdi_migrated_secs)/active_secs
    of.write("Seconds when active VMs are operating on consolidated host: %d, %f of all total seconds\n"%(vm_active_vdi_migrated_secs, rate))
    of.close()
    print "Done. Result is stored in %s" % outf
    # print "Total active seconds: %d" % a1
    return (power_saving, rate, vm_active_vdi_migrated_secs)

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
def is_migratable(cur_sec, idle_vms):
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
        
        threshold = int( vms_per_vdi * ratio )

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
    # threshold = get_idle_threshold(cur_sec) # the minimum number of idle VMs to decide whether a vdi is migratable
    for vdi_num, idle_vms in sorted(vdi_idleness.items(), key=lambda x: x[1], reverse=True):
        if is_migratable(cur_sec,idle_vms) :
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
    resume_threshold = int(configs['resume_threshold'])
    # FIXME: only implement a policy here: any vm awake  > 5 will lead to the whole cluster to resume
    resume = False
    for i in vms_awake:
        if i > resume_threshold:
            resume = True
            break
    return resume

def decide_to_resume(vm_states, vdi_states, cur_sec):

    global configs
    global print_cnt
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
        if s == S3:
            for j in range(c*vms_per_vdi, (c+1)*vms_per_vdi): # iterate all vms of that vdi server
                # print "j is %d" % j
                if vm_states[j] == 1 and vm_states_before_migration[j] == 0:
                    vms_woke_up_per_vdi[c] += 1
        c += 1

    if cur_sec > 3*60*60+1 and print_cnt > 0:
        print "Current sec is %d" % cur_sec
        print "Vm states: %s" % vm_states
        print "vm_states_before_migration is %s" % vm_states_before_migration
        print "vms_woke_up_per_vdi is %s" % vms_woke_up_per_vdi
        print_cnt -= 1

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
    global migration_interval,reintegration_interval
    global cumulative_interval,cumulative_interval2
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
        assert reintegration_interval > 0
        assert  cumulative_interval2 >= 0  and cumulative_interval2 <= reintegration_interval
        if cumulative_interval2 < reintegration_interval:
            cumulative_interval2 += 1
            next_states = vdi_states[-1]
        else:
            cumulative_interval2 = 0
            next_states = update_states(vdi_states, REINTEGRATING, FULL)

    return next_states


if __name__ == '__main__':

    policy_type = configs['migration_policy_type']
    
    if policy_type == "static":
        
        of = "data/static-all-result"
        f = open(of, "w+")
        header = "Static migration threshold (idle%),Static resume threshold (active#), Power Saving (weekday),Penalty Seconds (weekday), Penalty Rate (weekday)"
        header += ",Power Saving (weekend), Penalty Seconds (weekday), Penalty Rate (weekend)\n"
        f.write(header)
        rts = configs['resume_thresholds'].rstrip().split(",")
        its = configs['idle_thresholds'].rstrip().split(",")
        i = 0 
        for i in range(0, len(rts)):
            configs['resume_threshold'] = int(rts[i])
            configs['idle_threshold'] = float(its[i])
            
            inputs = configs["inputs-weekday"]
            configs["dayofweek"] = "weekday"
            cnt = 0
            tsaving = 0.0
            trate = 0.0
            tsecs = 0
            for inf in inputs.rstrip().split(","):
                if inf != '':
                    outf = inf+".out-static-%d-%f.csv"%(int(rts[i]), float(its[i]))
                    (saving, rate, active_secs)  = run(inf,outf)
                    tsaving += saving
                    trate += rate
                    tsecs += active_secs
                    cnt += 1
            ave_weekday_saving = tsaving / cnt 
            ave_weekday_secs = tsecs / cnt
            ave_weekday_rate = trate / cnt
            # dealing with weekend
            inputs = configs["inputs-weekend"]
            configs["dayofweek"] = "weekend"
            cnt = 0
            tsaving = 0.0
            trate = 0.0
            tsecs = 0
            for inf in inputs.rstrip().split(","):
                if inf != '':
                    outf = inf+".out-static-%d-%f.csv"%(int(rts[i]), float(its[i]))
                    #outf = inf+".out-weekend.csv"
                    (saving, rate, active_secs)  = run(inf,outf)
                    tsaving += saving
                    trate += rate
                    tsecs += active_secs
                    cnt += 1
            ave_weekend_saving = tsaving / cnt 
            ave_weekend_secs = tsecs / cnt
            ave_weekend_rate = trate / cnt
            f.write("%f,%d,%f,%d,%f,%f,%d,%f\n"%(configs['idle_threshold'],configs['resume_threshold'],\
                                         ave_weekday_saving, ave_weekday_secs, ave_weekday_rate, ave_weekend_saving,\
                                           ave_weekend_secs, ave_weekend_rate))
            

        f.close()
        print "Done. Result is in %s"%of

    if policy_type == "dynamic":
        
        of = "data/dynamic-all-result"
        f = open(of, "w+")
        header = "Static Active VM# threshold, CDF threshold, Static resume threshold (active#)"
        header += "Power Saving (weekday),Penalty Seconds (weekday), Penalty Rate (weekday)"
        header += ",Power Saving (weekend), Penalty Seconds (weekday), Penalty Rate (weekend)\n"
        f.write(header)
        rts = configs['resume_thresholds_dynamic'].rstrip().split(",")
        avs = configs['active_vm_num_thresholds'].rstrip().split(",")
        cdfs = configs['active_vm_cdf_thresholds'].rstrip().split(",")
        i = 0 
        for i in range(0, len(rts)):
            configs['active_vm_num_threshold'] = int(avs[i])
            configs['active_vm_cdf_threshold'] = float(cdfs[i])
            configs['resume_threshold'] = int(rts[i])
            
            inputs = configs["inputs-weekday"]
            configs["dayofweek"] = "weekday"
            cnt = 0
            tsaving = 0.0
            trate = 0.0
            tsecs = 0
            for inf in inputs.rstrip().split(","):
                if inf != '':
                    outf = inf+".out-dynamic-%d-%d-%f.csv"%(int(rts[i]), int(avs[i]),float(cdfs[i]))
                    (saving, rate, active_secs)  = run(inf,outf)
                    tsaving += saving
                    trate += rate
                    tsecs += active_secs
                    cnt += 1
            ave_weekday_saving = tsaving / cnt 
            ave_weekday_secs = tsecs / cnt
            ave_weekday_rate = trate / cnt
            # dealing with weekend
            inputs = configs["inputs-weekend"]
            configs["dayofweek"] = "weekend"
            cnt = 0
            tsaving = 0.0
            trate = 0.0
            tsecs = 0
            for inf in inputs.rstrip().split(","):
                if inf != '':
                    outf = inf+".out-dynamic-%d-%d-%f.csv"%(int(rts[i]), int(avs[i]),float(cdfs[i]))
                    #outf = inf+".out-weekend.csv"
                    (saving, rate, active_secs)  = run(inf,outf)
                    tsaving += saving
                    trate += rate
                    tsecs += active_secs
                    cnt += 1
            ave_weekend_saving = tsaving / cnt 
            ave_weekend_secs = tsecs / cnt
            ave_weekend_rate = trate / cnt
            f.write("%d,%f,%f,%f,%d,%f,%f,%d,%f\n"%(configs['active_vm_num_threshold'],configs['active_vm_cdf_threshold'], configs['resume_threshold'],\
                                         ave_weekday_saving, ave_weekday_secs, ave_weekday_rate, ave_weekend_saving,\
                                           ave_weekend_secs, ave_weekend_rate))
            

        f.close()
        print "Done. Result is in %s"%of
