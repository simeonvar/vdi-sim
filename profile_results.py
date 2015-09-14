import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import mlab

log_file = sys.argv[1]


localPartials = 0
localPartials_a = []
localPartials_no_migration = 0
localPartials_need_kickout = 0

remotePartials = 0
remotePartials_a = []
remotePartials_do_post_partials = 0
remotePartials_reintegrate = 0

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
        if exceed == "Y":
            localPartials_need_kickout += 1
        else:
            localPartials_no_migration += 1

print "1) local partial -> active VM#: %d\ 2 categories of host action:     1.1) host has enough resource, no migration is needed:  %d     or     1.2) host kicks out other VMs to make space: %d 2) remote partial -> active VM#: %d     2.1) host does the post-partial migration: %d     or    2.2) host decide to reintegrate it back to its origin: %d local partial -> active latency: %.1f  remote partial -> active latency: %.1f all idle -> active latency: %.1f" %( localPartials, localPartials_no_migration, localPartials_need_kickout, remotePartials, remotePartials_do_post_partials, remotePartials_reintegrate, float(total_localPartial)/localPartials, float(total_remotePartial)/remotePartials, float(total_remotePartial+total_localPartial)/(localPartials+remotePartials))

data = localPartials_a
sorted_data = np.sort(data)
yvals=np.arange(len(sorted_data))/float(len(sorted_data))
plt.plot(sorted_data,yvals)
plt.show()
