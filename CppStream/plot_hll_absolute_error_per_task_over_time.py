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
    print "added task-%d" % i

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
        #print "token index: %d" % i

matplotlib.rcParams.update({'font.size' : 24, 'font.family' : 'sans-serif'})

markers = ['ro', 'b^', 'gx', 'yo', 'cv', 'k1', 'gD', 'm.', 'r,', 'bp']

colors = [ 'red', 'blue', 'green', 'purple', 'orange', 'yellow', 'black', 'grey', 'purple', 'pink']
fig = plt.figure(1)
plt.xlabel('stream progress')
plt.ylabel('Absolute error')
plt.grid(True)
plt.tight_layout()
plt.plot(element_sequence_id, 
absolute_error[1], markers[1], absolute_error[2], markers[2], 
absolute_error[3], markers[3], absolute_error[4], markers[4], absolute_error[5], markers[5], 
absolute_error[6], markers[6], absolute_error[7], markers[7], absolute_error[8], markers[8])
#absolute_error[9], markers[9], absolute_error[10], markers[10])


plt.show()
fig.savefig('rel_error.pdf')
plt.close()