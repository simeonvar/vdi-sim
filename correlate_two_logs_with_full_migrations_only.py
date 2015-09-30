import sys
FULL_MIGRATION_LATENCY = 40
PARTIAL_MIGRATION_LATENCY = 4
LAGS_BETWEEN_TWO_LOGS = 0

vm_logs = sys.argv[1]
host_logs = sys.argv[2]

output = "data/"+ vm_logs.split(".csv")[1].split(".out")[0] + "_with_full_migrations_only.csv"
of = open(output, "w+")

def get_partial_sequence(post_partial_vm_sequence):
    splits3 = post_partial_vm_sequence.split("-")
    sequence = ""
    for s in splits3:
        sequence = ""
        try:
            vm = int(s)
            sequence = "%d"%(vm)
            for j in range(8): # post partial migration is 9 times slower than partial migration                                
                sequence += "-%d"%(vm)
        except:
            print "Exception converting %s (if it is empty then it is harmless." 
            continue
    return sequence

def get_full_sequence(full_vm_sequence):
    splits3 = full_vm_sequence.split("-")
    sequence = ""
    for s in splits3:
        sequence = ""
        try:
            vm = int(s)
            sequence = "%d"%(vm)
            for j in range(9): # post partial migration is 9 times slower than partial migration                                
                sequence += "-%d"%(vm)
        except:
            print "Exception converting %s (if it is empty then it is harmless." 
            continue
    return sequence

cnt = 0
for l in open(vm_logs):
    cnt += 1
    if cnt == 1:
        newline = l.rstrip() + ",Latency,Source Queue,Destination Queue(s)\n"
        #of.write(newline)
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
    source_queue = ""
    destination_queues = []
    if exceed == "N" and src == cur:
        latency = 0             # local partial -> full when there is resources at the host
    if exceed == "N" and src != cur:
        latency = 0 # finish the rest of the migration

    skip_this_line = False
    if exceed == "Y": 
        dests = {}              # key is the destination host, value is the dest queue.
        first_line = True
        skip_this_line = True   # by default, we assign it to be True
        for l2 in open(host_logs):
            if first_line:
                first_line =False # skip the header row
                continue
            splits2 = l2.rstrip().split(",")
            timestamp2 = int(splits2[0])
            if timestamp2 != timestamp+LAGS_BETWEEN_TWO_LOGS and timestamp2 != timestamp: # should be ahead of the current timestamp
                continue
            else:
                skip_this_line = False # we find that timestamp that matches the action. otherwise, we skip this line

            src2 = int(splits2[2])
            if cur == src2: # if the current host of the vm is equal to the source host of the migration logs
                partial_migration_number = int(splits2[6])
                if partial_migration_number > 0:
                    partial_vm_sequence = splits2[7]
                    if source_queue == "":
                        source_queue += (partial_vm_sequence)                
                    else:
                        source_queue += ("-"+partial_vm_sequence)                
                    dst =(int(splits2[3]))
                    if dst not in dests:
                        dests[dst] = ""
                reintegration_number = int(splits2[8])
                if reintegration_number > 0:
                    partial_vm_sequence = splits2[9]
                    if source_queue == "":
                        source_queue += (partial_vm_sequence)                
                    else:
                        source_queue += ("-"+partial_vm_sequence)                
                    dst =(int(splits2[3]))
                    if dst not in dests:
                        dests[dst] = ""
                post_partial_number = int(splits2[10])
                if post_partial_number > 0:
                    post_partial_vm_sequence = splits2[11]
                    sequence = get_partial_sequence(post_partial_vm_sequence)
                    if source_queue == "":
                        source_queue += (sequence)                
                    else:
                        if sequence != "":
                            source_queue += ("-"+sequence)                                                                           
                    dst =(int(splits2[3]))
                    if dst not in dests:
                        dests[dst] = ""

                full_number = int(splits2[4])
                if full_number > 0:
                    full_vm_sequence = splits2[5]
                    sequence = get_full_sequence(full_vm_sequence)
                    if source_queue == "":
                        source_queue += (sequence)                
                    else:
                        if sequence != "":
                            source_queue += ("-"+sequence)                                                                           
                    dst =(int(splits2[3]))
                    if dst not in dests:
                        dests[dst] = ""
                        
        first_line = True
        for l2 in open(host_logs):
            if first_line:
                first_line =False # skip the header row
                continue
            splits2 = l2.rstrip().split(",")
            timestamp2 = int(splits2[0])
            if timestamp2 != timestamp+LAGS_BETWEEN_TWO_LOGS: # should be ahead of the current timestamp
                continue
            dst2 = int(splits2[3])
            if dst2 in dests:
                if int(splits2[6]) > 0: # partial number
                    if dests[dst2] == "":
                        dests[dst2] += (splits2[7])
                    else:
                        dests[dst2] += ("-" + splits2[7])
                if int(splits2[8]) > 0: # reintegration number
                    if dests[dst2] == "":
                        dests[dst2] += (splits2[9])
                    else:
                        dests[dst2] += ("-" + splits2[9])
                if int(splits2[10]) > 0: # post partial number
                    post_partial_vm_sequence = splits2[11]
                    sequence = get_partial_sequence(post_partial_vm_sequence)
                    if dests[dst2] == "":
                        dests[dst2] += sequence
                    else:
                        if sequence != "":
                            dests[dst2] += ("-" + sequence)
                if int(splits2[4]) > 0: # full migration number
                    full_vm_sequence = splits2[5]
                    sequence = get_full_sequence(full_vm_sequence)
                    if dests[dst2] == "":
                        dests[dst2] += sequence
                    else:
                        if sequence != "":
                            dests[dst2] += ("-" + sequence)
                    
        for dst in dests:
            assert dests[dst][0] != '-'
            while dests[dst][-1] == '-':
                dests[dst] = dests[dst][0:-1]
            if dests[dst][-1] == '-':
                skip_this_line = True
            dst_str = str(dst)+":"+dests[dst]
            destination_queues.append(dst_str)

    if skip_this_line:
        print "skipping ", l
        continue

    # ",Latency,Source Queue, Destination Queue(s)\n"
    final_destination_queue = ""
    for queue in destination_queues:
        final_destination_queue += (queue+",")
    newline = l.rstrip() + ",%d,%s,%s\n"%(latency,source_queue,final_destination_queue)
    of.write(newline)

of.close()
print "Done"
