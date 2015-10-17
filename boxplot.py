
from pylab import *
import sys


inf = open(sys.argv[1], "r")
config_column = 3
saving_column = 4

configs_saving = {}

first_line = True
for l in inf:
    if first_line:
        first_line = False
        continue
    else:
        splits = l.rstrip().split(",")
        config = splits[config_column]
        saving = float(splits[saving_column])
        if config not in configs_saving:
            configs_saving[config] = [saving]
        else:
            configs_saving[config].append(saving)


#data = concatenate( (data, d2), 1 )
# Making a 2-D array only works if all the columns are the
# same length.  If they are not, then use a list instead.
# This is actually more efficient because boxplot converts
# a 2-D array into a list of vectors internally anyway.
data = []
cnt = 1
ticks = []
labels = []
for config in configs_saving:
    data.append(configs_saving[config])
    ticks.append(cnt)
    labels.append(config)
    cnt += 1
# multiple box plots on one figure
figure()
boxplot(data)
xticks(ticks, labels)
show()