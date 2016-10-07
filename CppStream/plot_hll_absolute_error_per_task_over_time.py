import math
import numpy as np
import matplotlib.pyplot as plt
import matplotlib

main_file = 'imbalance_plot.csv'

max_error = 0
task_number = 10
element_sequence_id = []

absolute_error = dict()
for i in range(1, task_number + 1):
    absolute_error[i] = list()
    #print "added task-%d" % i

lines = list()
with open(main_file) as abs_error_file:
    lines = abs_error_file.read().splitlines()

for l in lines:
    tokens = l.split(',')
    element_sequence_id.append(long(tokens[0]))
    for i in range(1, task_number + 1):
        if max_error < float(tokens[i]):
            max_error = float(tokens[i])
        absolute_error[i].append(float(tokens[i]))

matplotlib.rcParams.update({'font.size' : 24, 'font.family' : 'monospace'})

markers = ['b.', 'ro', 'b^', 'gx', 'yo', 'cv', 'k1', 'gD', 'm.', 'r,', 'bp']

colors = [ 'red', 'blue', 'green', 'purple', 'orange', 'yellow', 'black', 'grey', 'purple', 'pink', 'red']
fig, ax = plt.subplots(1)
for i in range(1, task_number + 1):
    #print "index: %d" % i
    task_label = 't-' + str(i)
    ax.plot(element_sequence_id, absolute_error[i], markers[i], label=task_label, linewidth=2, markersize=5)

ax.set_ylabel('absolute error')
ax.set_xlabel('stream progress')
ax.yaxis.grid(True)
plt.tight_layout()

plt.show()
fig.savefig('absolute_error.pdf')
plt.close()