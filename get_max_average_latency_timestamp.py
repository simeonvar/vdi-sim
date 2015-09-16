import operator
import sys

log_file = sys.argv[1]
timestamp_latencies = {}
timestamp_ave_latencies = {}
timestamp_count = {}

for l in open(log_file, "r"):
    splits = l.rstrip().lstrip().split(",")
    timestamp = int(splits[0])
    latency = int(splits[8])
    if timestamp in timestamp_latencies:
        timestamp_latencies[timestamp] += latency
        timestamp_count[timestamp] += 1
    else:
        timestamp_latencies[timestamp] = latency
        timestamp_count[timestamp] = 1
for ts in timestamp_count:
    cnt = timestamp_count[ts]
    timestamp_ave_latencies[ts] = timestamp_latencies[ts]/cnt

print "Max total latency:",max(timestamp_latencies.iteritems(), key=operator.itemgetter(1))[1]
print "timestamp =", max(timestamp_latencies.iteritems(), key=operator.itemgetter(1))[0]

print "Max ave latency:", max(timestamp_ave_latencies.iteritems(), key=operator.itemgetter(1))[1]
print "timestamp =",max(timestamp_ave_latencies.iteritems(), key=operator.itemgetter(1))[0]

print "Max idle->active transitions:", max(timestamp_count.iteritems(), key=operator.itemgetter(1))[1]
ts = max(timestamp_count.iteritems(), key=operator.itemgetter(1))[0]
print "timestamp =", ts
print "total latency =",timestamp_latencies[ts]
print "Average latency =",timestamp_ave_latencies[ts]
