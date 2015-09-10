# adjust local partial resume latency. These local partials have to wait for all the other remote partials to vacate so that they can be swapped back. 

import sys
PARTAIL_MIGRATION_LATENCY = 4

logs_file = sys.argv[1]
outf = logs_file+"-adjusted-local-partail-latencies.csv"
of = open(outf, "w+")
for l in open(logs_file, "r"):
    splits = l.rstrip().lstrip().split(",")
    exceed = splits[7]
    vm_index = int(splits[1])
    src = int(splits[2])
    cur = int(splits[3])
    timestamp = int(splits[0])
    latency = int(splits[8])
    if exceed == "Y" and src == cur: # local partials that need to wait for other vacate
        old_dst_queues = splits[10:-1] # -1 -> skip the last one element
        all_empty = False
        for dst in old_dst_queues:
            if len(dst.rstrip().lstrip()) == 0:
                all_empty = True
                break
            else:
                continue
        if all_empty:
            print "It is empty for all dest queues. Now skipping line %d, vm_index: %d" % (timestamp, vm_index)
            continue
        # get the lines for the same interval
        lines = []
        for l2 in open(logs_file, "r"):
            splits2 = l2.rstrip().lstrip().split(",")
            timestamp2 = int(splits2[0])
            if timestamp == timestamp2:
                lines.append(l2)

        dests = {}
        last_element_pos = {}
        for l3 in lines:
            splits3 = l3.rstrip().lstrip().split(",")

            # find all dests first, then update them
            for q in splits3[10:-1]: # skip the last one empty
                if len(q) > 0:
                    dst = q.split(":")[0]
                    try:
                        dst_queue = q.split(":")[1]
                        dests[dst] = dst_queue
                    except:
                        print "Exception: %s" % q
                        pass

            # find all source queue, and their last elements, and their last position
            source_queue = splits3[9]
            source_splits = source_queue.split("-")
            last_element = source_splits[-1]
            pos = source_splits.index(last_element)
            last_element_pos[last_element] = pos
        
        for last in last_element_pos:
            pos = last_element_pos[last]
            for dst in dests:
                dst_queue = dests[dst]
                dst_splits = dst_queue.split("-")
                if last in dst_splits:
                    pos_in_dst = dst_splits.index(last)
                    diff = pos - pos_in_dst 
                    if diff > 0:
                        for i in range(diff):
                            dst_splits.insert(pos_in_dst, "A")
                        assert pos == dst_splits.index(last)
                        new_dst_queue = "-".join(dst_splits)
                        dests[dst] = new_dst_queue
                        continue
        # now update the latency
        source_queue = splits[9]
        source_splits = source_queue.split("-")
        last_element = source_splits[-1]

        if last_element == '':
            print "Skipping ", l
            continue
        assert int(last_element) >= 0

        found_pos = -1 
        # go to find the position in the dst queues
        for d in old_dst_queues:
            dst = d.split(":")[0]
            if len(dst) == 0:
                print "Exception: dst queue is ", d
                sys.exit(1)
            
            adjusted_dst_queue = dests[dst]
            dst_splits = adjusted_dst_queue.split("-")
            if last_element in dst_splits:
                found_pos = dst_splits.index(last_element)
                break

        assert found_pos != -1 
        latency =  (found_pos+1) * PARTAIL_MIGRATION_LATENCY
        print "Found_pos is %d" % found_pos
        assert latency > 0
    splits[8] = str(latency)
    of.write(",".join(splits)+"\n")
    
of.close()
print "Done"
