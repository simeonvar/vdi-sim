# Please the udpates on dev-log

import sys
import random
log_file = sys.argv[1]
of = open(log_file+"-updated-remote-partials.csv", "w+")

reintegration_count = 0
post_partial_no_migration = 0 
post_partial_somewhere_else = 0
not_handled_in_this_inteval = 0
source_queue_inconsistency = 0
dest_queue_inconsistency = 0

for l in open(log_file, "r"):
    splits = l.rstrip().lstrip().split(",")
    exceed = splits[7]
    vm_index = int(splits[1])
    src = int(splits[2])
    cur = int(splits[3])
    timestamp = int(splits[0])
    latency = int(splits[8])
    if exceed == "N" and src != cur: # no need to wait for others to vacate
        latency = 40
        post_partial_no_migration += 1
    if exceed == "Y" and src != cur: # remote partials that need to wait for others to vacate
        # need to see whether it will get integrated back to its original host
        source_queue = splits[9] 
        vm_sequence = source_queue.split("-")
        dst_queues = splits[10:-1]
        reintegration = False
        source_queue_len = len(vm_sequence)
        queue_len = source_queue_len          # max of source or dest queue len. This variable will be updated in the below if-else block. In the case where there is an inconsistency in the data, this variale will be used to randomly assign a queue pos for the reintegration
        data_inconsistent = False
        pos = -1
        for d in dst_queues:
            dst = d.split(":")[0]
            q = d.split(":")[1]
            dest_queue_len = len(q.split("-"))
            assert dest_queue_len > 0
            if dst == str(src):
                reintegration = True
                if str(vm_index) not in vm_sequence:
                    print "Inconsistency in ", l
                    print "The source is woken up but the vm index %d is not in source queue" % vm_index
                    source_queue_inconsistency += 1
                    data_inconsistent = True
                else:
                    pos = vm_sequence.index(str(vm_index))    
                if str(vm_index) not in q.split("-"):
                    print "Inconsistency in ", l
                    print "Its source is woken up but the vm index %d is not in the dest queue: %s"% (vm_index, q)
                    dest_queue_inconsistency += 1
                    data_inconsistent = True
                break
        if reintegration:
            #print "vm %d being reintegrated back to: %d"%(vm_index, src)
            if data_inconsistent: 
                pos = random.randint(0, queue_len)
            assert pos != -1
            latency = (pos+1) * 4 # not considering destination queue
            reintegration_count += 1
        elif str(vm_index) not in vm_sequence:
            latency = 300 + 40  # TODO: we need to consider the case where it was handled in the next next inteval
            not_handled_in_this_inteval += 1
        else:
            # it is in the source queue, then get the position of the source queue
            pos = vm_sequence.index(str(vm_index))    
            assert pos != -1
            #print "vm %d is NOT being reintegrated"%(vm_index)
            latency = (pos) * 4 + 40 # not considering destination queue
            post_partial_somewhere_else += 1
    splits[8] = str(latency)

    of.write(",".join(splits))
    of.write("\n")

of.close()
print "Reintegration count: %d" % reintegration_count
print "post_partial_no_migration %d" %post_partial_no_migration
print "post_partial_somewhere_else %d" % post_partial_somewhere_else 
print "not_handled_in_this_inteval %d" % not_handled_in_this_inteval
print "source_queue_inconsistency: %d"%source_queue_inconsistency
print "dest_queue_inconsistency: %d" % dest_queue_inconsistency
