
# from pylab import *
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import sys

def to_percent(y, position):
    # Ignore the passed in position. This has the effect of scaling the default
    # tick locations.
    s = str(100 * y)

    # The percent symbol needs escaping in latex
    if matplotlib.rcParams['text.usetex'] == True:
        return s + r'$\%$'
    else:
        return s + '%'



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
# figure()
# boxplot(data)

#show()

fs = 15 # fontsize

# demonstrate how to toggle the display of different elements:
fig, ax1 = plt.subplots(nrows=1, ncols=1)
ax1.boxplot(data)
plt.xticks(ticks, labels)
ax1.set_title('Unoptimized memory server', fontsize=fs)
ax1.set_xlabel(' VDI Server# + Consolidation Server#')
ax1.set_ylabel('Power Saving (%)')
formatter = FuncFormatter(to_percent)
# Set the formatter
plt.gca().yaxis.set_major_formatter(formatter)
#ax1.yaxis.set_major_formatter(yticks)

#fig.subplots_adjust(hspace=0.4)
plt.show()