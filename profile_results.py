import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import mlab

log_file = sys.argv[1]


localPartials = 0
localPartials_a = []
localPartials_no_migration = 0
localPartials_need_kickout = 0
localPartials_max = -1
localPartials_max_timestamp = -1


remotePartials = 0
remotePartials_a = []
remotePartials_do_post_partials = 0
remotePartials_reintegrate = 0
remotePartials_max = -1
remotePartials_max_timestamp = -1

total_localPartial = 0
total_remotePartial = 0

for l in open(log_file, "r"):
    splits = l.rstrip().lstrip().split(",")
    exceed = splits[7]
    vm_index = int(splits[1])
    src = int(splits[2])
    cur = int(splits[3])
    timestamp = int(splits[0])
    latency = int(splits[8])
    source_queue = splits[9]
    if src != cur: # remote partials that need to wait for others to vacate
        remotePartials += 1
        remotePartials_a.append(latency)
        total_remotePartial += latency
        if latency > remotePartials_max:
            remotePartials_max = latency
            remotePartials_max_timestamp = timestamp
        if exceed == "Y":        
            source_host_wakeup = False # see if any of the source host is wakeup
            dest_queues = splits[10:-1]
            for q in dest_queues:
                dst = q.split(":")[0]
                if str(src) == dst:
                    source_host_wakeup = True
                    break
            if source_host_wakeup:
                remotePartials_reintegrate += 1
            else:
                remotePartials_do_post_partials += 1
        else:
            remotePartials_do_post_partials += 1
    if src == cur: # remote partials that need to wait for others to vacate
        localPartials += 1
        localPartials_a.append(latency)
        total_localPartial += latency
        if latency > localPartials_max:
            localPartials_max = latency
            localPartials_max_timestamp = timestamp
        if exceed == "Y":
            localPartials_need_kickout += 1
        else:
            localPartials_no_migration += 1

print "1) local partial -> active VM#: %d\ 2 categories of host action:" %localPartials
print "  1.1) host has enough resource, no migration is needed:  %d     or    "%localPartials_no_migration
print " 1.2) host kicks out other VMs to make space: %d"% localPartials_need_kickout
print " 2) remote partial -> active VM#: %d  "%remotePartials
print "  2.1) host does the post-partial migration: %d     or   "%remotePartials_do_post_partials
print " 2.2) host decide to reintegrate it back to its origin: %d "%remotePartials_reintegrate
print "local partial -> active average latency: %.1f"%(float(total_localPartial)/localPartials)
print "Max of local partials: %d" % localPartials_max
print "Timestamp of Max of local partials: %d" % (localPartials_max_timestamp)
print "Standard Deviation of local partials: %d" % np.std(localPartials_a)
print "remote partial -> active average latency: %.1f"%( float(total_remotePartial)/remotePartials)
print "Max of remote partials: %d" % remotePartials_max
print "Timestamp of Max of remote partials: %d" % (remotePartials_max_timestamp)
print "Standard Deviation of remote partials: %d" % np.std(remotePartials_a)
print "all idle -> active average latency: %.1f" %(float(total_remotePartial+total_localPartial)/(localPartials+remotePartials))
mergedlist = remotePartials_a + localPartials_a
print "Max of all: %d" % max(mergedlist)
print "Standard Deviation of all: %d" % np.std(mergedlist)

data = localPartials_a
#data = remotePartials_a
sorted_data = np.sort(data)
yvals=np.arange(len(sorted_data))/float(len(sorted_data))
plt.plot(sorted_data,yvals)
plt.xlabel('Latency (s)')
plt.ylabel('CDF')
plt.title("Remote Partials VMs -> Active")
plt.show()
