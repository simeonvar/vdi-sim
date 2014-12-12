vdi-sim
=======

A simple cluster-wide VDI server simulator that supports different policies of combining full and partial VM migrations to simulate power consumption and performance impacts. 

The policies are in 4 parts:

A. Whether to migrate, the decision is made only if
   1. when there is enough capacites in the destination host(s)

B. Where to migrate:
   1. the remote host has capacity
   2. we favor the case when we consolidate more active VMs together into one host

C. When and how to resume:
   1. When partially migrated VMs become active, and the remote host does not have enough capacity any more, we need to migrate some VMs out. 
   2. There are two choices when dealing with this situation:
      2.1. Migrate the Idle->Active VMs out to another host
      2.2. Kick some idle VMs out, preferrably to some other hosts

There are 3 types of VM state transitions:
      1. Active -> idle (All active VMs are treated the same, even though they were fully migrated from somewhere else originally)
      2. Local idle -> active 
      3. Remote idle -> active

For Case 1, If swap is not enabled, then local idle VMs' RAM are not swapped out, they are still using that many resources, so we can do nothing about them. If swap is enabled, then we swapped it out if there is resources. 

For Case 2, if we have enough resources, then we use them and put the swaps back. Otherwise, we need to kick some others out. Refer to Policy C. 

For Case 3, the same as Case 2. 

Parameters:
1. Migration method: Full + Partial migration
2. interval: 5 min
3. Capacity slack for each VDI server: 30%

Algorithm:
1. calculate how many intervals it takes for the migration [method] set in the setting file to migrate
2. If it decides to migrate, then that many intervals will be in migration, so calculate the interval marco state first, then deal with the micro

Assumption: 
1. Every VDI server has the same capacity slack
2. Hypervisor supports swapping. 






