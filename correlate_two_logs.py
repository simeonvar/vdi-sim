import sys

FULL_MIGRATION_LATENCY = 40
PARTIAL_MIGRATION_LATENCY = 4
LAGS_BETWEEN_TWO_LOGS = 300

vm_logs = sys.argv[1]
host_logs = sys.argv[2]
output = vm_logs + "appended_with_latency.csv"
of = open(output, "w+")

cnt = 0
for l in open(vm_logs):
    cnt += 1
    if cnt == 1:
        newline = l.rstrip() + ",Latency, Destination Host, Reintegration(Y/N), Lagging behind, Queue in Destination Host\n"
        of.write(newline)
        continue                # skip the header
    splits = l.rstrip().split(",")
    timestamp = int(splits[0])
    if timestamp == 300:
        continue                # skip the rows with 300
    latency = 0                 # in seconds
    exceed = splits[7]
    vm_index = int(splits[1])
    src = int(splits[2])
    cur = int(splits[3])
    dest = -1
    final_queue = ""
    lagging = 0
    reintegration = "N"
    if exceed == "N" and src == cur:
        latency = 0             # local partial -> full when there is resources at the host
    if exceed == "N" and src != cur:
        latency = FULL_MIGRATION_LATENCY # finish the rest of the migration
    if exceed == "Y": 
        first_line = True
        #print "looking for the dest of vm %d" % vm_index
        found_timestamp = -1
        for l2 in open(host_logs):
            if first_line:
                first_line =False
                continue
            splits2 = l2.rstrip().split(",")
            timestamp2 = int(splits2[0])
            if timestamp2 < timestamp: # should be ahead of the current timestamp
                continue
            lines = []
            
            # find the destination host
            src2 = int(splits2[2])
            if cur == src2: # if the current host of the vm is equal to the source host of the migration logs
                partial_vm_sequence = splits2[7]
                all_partials = partial_vm_sequence.split("-")
                if str(vm_index) in all_partials:
                    dest = int(splits2[3])
                    found_timestamp = int(splits2[0])
                    break
        if  dest == -1 :
            print "Dest is -1. Failed to find dest. Skipping line %d" % cnt 
            continue
        
        lagging = found_timestamp - timestamp
        if dest == src:
            reintegration = "Y"
        lines = []
        # find all lines that in that same timestamp
        first_line = True
        for l2 in open(host_logs):
            if first_line:
                first_line =False
                continue
            splits2 = l2.rstrip().split(",")
            if int(splits2[0]) == found_timestamp:
                lines.append(l2.rstrip().lstrip())

        # loop through the sequence and get the queue sequence of the destination
        dest_queue = ""     # dest_queue for partials
        dest_queue_full = ""    
        for l3 in lines:
            splits3 = l3.split(",")
            dest3 = int(splits3[3])
            if dest3 == dest:
                partial_vm_sequence = splits3[7].lstrip().rstrip()
                if len(partial_vm_sequence) > 0:
                    if len(dest_queue) == 0:
                        dest_queue += partial_vm_sequence
                    else:
                        dest_queue += ("-"+partial_vm_sequence)
                # now handle the full vm sequence
                full_vm_sequence = splits3[5].lstrip().rstrip()
                if len(full_vm_sequence) > 0:
                    if len(dest_queue_full) == 0:
                        dest_queue_full += full_vm_sequence
                    else:
                        dest_queue_full += ("-"+full_vm_sequence)
                            
        final_queue = dest_queue + "-" + dest_queue_full
        vms_in_queue = dest_queue.split("-")
        index_in_queue = vms_in_queue.index(str(vm_index))
        latency = (index_in_queue + 1) * PARTIAL_MIGRATION_LATENCY 

    newline = l.rstrip() + ",%d,%d,%s,%d,%s\n"%(latency,dest,reintegration,lagging,final_queue)
    of.write(newline)

of.close()
print "Done"
