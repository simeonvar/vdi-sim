v = {}
v[1] = 3
v[2] = 89
v[4] = 2
v[45] = 5

for k,v1 in sorted(v.items(), key=lambda x: x[1], reverse=True) :
    print "%d=>%d" %(k,v1)

for i in v:
    print i
