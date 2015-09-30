import sys
log_file = sys.argv[1]
host_logs = sys.argv[2]
target_timestamp = int(sys.argv[3])

full_migrations = 0


for l in open(log_file, "r"):
    splits = l.rstrip().lstrip().split(",")
    exceed = splits[7]
    vm_index = int(splits[1])
    src = int(splits[2])
    cur = int(splits[3])
    timestamp = int(splits[0])
    latency = int(splits[8])
    source_queue = splits[9]
    
