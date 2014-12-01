vdi-sim
=======

A simple cluster-wide VDI server simulator that supports different policies of combining full and partial VM migrations to simulate power consumption and performance impact. 

Some sample policies:
A. Whether to migrate, the decision is made only if
   1. The chances of there will be at least 10 VMs idle for the next 10 minutes is greater than 50%
   2. The destination server has enough capacity to host that many migrated VM
B. When to resume:
   1. When there are > 5 idle VMs turning active

Parameters:
1. Migration method: 1. Partial migration only, 2. Full + Partial migration
2. interval: 1 min, 5 min, etc.
3. Capacity slack of VDI servers: 50% 100%

Algorithm:
1. calculate how many intervals it takes for the migration [method] set in the setting file to migrate
2. If it decides to migrate, then that many intervals will be in migration, so calculate the interval marco state first, then deal with the micro

Simplication: 
1. Every VDI server has the same capacity slack
2. We are using a greedy strategy: 1) We migrate the vdi servers with the most
   idle VM number first, 2) We migrate the maximum number of migratable VDI servers, e.g.,
   if there are 3 vdi *migratable* servers, then we migrate them all, instead of 2
   or 1. 
3. We only support migration/consolidate once. No multiple migration is implemented.
4. When we resume, we resume the whole cluster. 
5. I have not implemented the second migration optimization yet. 

Next: 
3. Select where to migrate
4. 





