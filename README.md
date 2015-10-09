vdi-sim
=======
-Document structure
The data/ folder stores the results. 
The data-processing stores the processed results and graphs. 

-How to run simulator
A: After we edit the ./setting files, simply hit:
      python vdi-sim.py'
It will take a while to finish.

-Where is the output file? 
A: The output file will be in the same directory of inputs-weekday. The output file name will be the trace file appended with the idle threshold value. 

FAQ
1. What was the resume threshold for? 
A: It is the number of active VMs in the consolidated host. If it exceeds this threshold, then we need to kick someone out. We discard this notion in our refactoring. We don't need this threshold to complicate our policy. We simly let space be our single base for kicking out VMs. 

2. In the run_experiment() logic, the simulator decides what to migrate, and then what to resume? So when the overall state is in resume state, the simulator only does one thing? 

*Can we simply print the migration plan? Simply print all the VMs state very interval? 
Does it record the current hosts, and their master hosts? How do we account for full and partial migrations?
A: Yes. We have a function to record the migration times. Simply add a printing functions close to there to output the migration plan. 

-Detailed explanations
Vdi-sim is a simple cluster-wide Virtual Destktop Infrastructure(VDI) server simulator that replays the desktop traces and simulates the power consumption and performance impacts. It supports the hybrid policies of combining full and partial VM migration techniques. The simulator assumes there exists a centralized controller software that coordinates the power management of a VDI server cluster. 

Vdi-sim takes VM activity traces as input. Each VDI server hosts a number of desktop virtual machines(VM). At each second, we measure the state of each VM. There are two defined VM states: idle and active. If there is no keyboard or mouse activity in a VM during a second, then the VM is defined to be idle, otherwise active. So a traces file for 8 VMs looks like below(Each line specifies the VM states during a particular second of the date):

1,0,1,0,0,0,0,0
1,0,1,0,1,0,0,1
....

Vdi-sim reads the traces file and makes decisions of whether or how to consolidate the VDI servers so as to save power. A very naive approach is to fully migrate all VMs when there are enough resources in the destination host(s) and power off a VDI server. 

There are a few questions to be answered: 

Q1. Is it worthwhile to migrate? If more VMs are to be waken up and to turn into acitve, then the destination host(s) do not have cacpacity to host them any more. In this case, we may have to power on the original host and migrate them back again? If we know the VMs become active soon, can we make smarter decisions so that we save network bandwidth and migration downtime? Vdi-sim enables different policies, e.g., aggressive ones such as migrate VMs whenever possible, or mild policies such as only migrate whenever there is enough room of resource in the destination host to allow the resource consumption to grow. 

Q2. How do we migrate VMs? Full migration is expensive in terms of migration downtime(40+ seconds), network bandwidth (e.g., transfering 4GB RAM per VM) and the destination host RAW or disk space. Partial migration is swift, but it will cause severe performance degradation for active VMs. In vdi-sim, the default choice is to partially migrate idle VMs and fully migrate active VMs. 

Q3. Where to migrate? The question is concerned with the VM placement strategy. The default strategy in vdi-sim is to attempt to assemble active VMs into host. If the host does not have enough capacities to host all VMs, the priority to vacuate based on the VM state is: remote partial idle VMs > local idle VMs > active VMs. 

More specifically, the policies are in 4 parts:

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

For Case 1, If hypervisor level swapping is not enabled, then local idle VMs' RAM are not swapped out, they are still using that many resources, so we can do nothing about them. If swapping is enabled, then we swapped it out if there is resources. 

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

The post-partial migration tuple (src, dest) is transfering from (origin, curhost), so the vm's origin will be changed to curhost. 

I decided to use the existing nested api to make it easy to change my code.

The complexity with the heavy policy thing is to make sure the result is 100% correct. We make a schedule once at the beginning of the interval, then we execute that schedule throughout the interval.  

So the decide_to_migrate and decide_to_resume need to handle the remote partial vms becoming active. This logic goes like this:
if the remote VM turning into active has enough resource in the current host, and its host and origin has enough bandwidth, then we reintegrat it immediately. i.e., we update the bandwidth, otherwise, we wait for the make_decision to make schedule for the next round.    