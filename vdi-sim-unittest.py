import unittest

from simu import find_available_slots
from simu import find_migration_slot
from simu import get_migration_completion_timestamp
from simu import Waiting_VM
from simu import find_next_migration_vm
from simu import calculate_provision_latency

try:
    import Queue as Q  # ver. < 3.0
except ImportError:
    import queue as Q

class TestMigrationFinder(unittest.TestCase):
    def setUp(self):
        self.init_schedule()
        
    def init_schedule(self):
        self.length = 10
        self.host_schedule1 =  []
        self.host_schedule2 =  []
        for i in range(self.length):
            self.host_schedule1.append("r")
        for i in range(self.length):
            self.host_schedule2.append("r")

    def tearDown(self):
        pass
    def test_find_available_slots(self):
        self.host_schedule1[0] = "i1"
        length = 3
        slots = find_available_slots(self.host_schedule1, length)      
        self.assertEqual(len(slots), 1)
        #first_slot 
        (start, end) = slots[0]
        self.assertEqual(end-start + 1, len(self.host_schedule1) - 1)
        
        # another test
        self.host_schedule1[6] = "i1"
        length = 3
        slots = find_available_slots(self.host_schedule1, length)   
        
        #first_slot 
        (start, end) = slots[0]
        self.assertEqual(end-start + 1, 5)   
        self.assertEqual(len(slots), 2)
        (start, end) = slots[1]
        self.assertEqual(end-start + 1, 3)

    def test_find_migration_slot(self):
        src = 0
        dst = 1
        migration_schedule = {}
        migration_schedule[src] = self.host_schedule1
        migration_schedule[dst] = self.host_schedule2
        migration_latency = 4
        interval_length = len(self.host_schedule1)
        
        slot = find_migration_slot(src, dst, migration_schedule, migration_latency, interval_length)
        self.assertNotEqual(slot, None)
        (start, end) = slot
        self.assertEqual(end-start + 1, migration_latency)
        
    def test_find_migration_slot_2(self):
        src = 0
        dst = 1
        migration_schedule = {}
        migration_schedule[src] = self.host_schedule1
        migration_schedule[dst] = self.host_schedule2
        migration_schedule[dst][2] = "isd"
        migration_schedule[src][5] = "t6"
        migration_latency = 4
        interval_length = len(self.host_schedule1)
        
        slot = find_migration_slot(src, dst, migration_schedule, migration_latency, interval_length)
        self.assertNotEqual(slot, None)

    def test_is_vm_in_outflow_queue(self):
        vm = 3
        pos = get_migration_completion_timestamp(vm, self.host_schedule1)
        self.assertEqual(pos, -1)
        
        expected_pos = 6
        self.host_schedule1[1] = "o," +str(vm)
        self.host_schedule1[2] = "i," +str(vm)
        self.host_schedule1[4] = "o," +str(vm)
        self.host_schedule1[5] = "o," +str(vm)
        self.host_schedule1[6] = "o," +str(vm)
        pos = get_migration_completion_timestamp(vm, self.host_schedule1)
        self.assertEqual(pos, expected_pos)
        
    def test_waiting_time(self):
        q = Q.PriorityQueue()
        q.put(Waiting_VM(1, 0,0,0))
        q.put(Waiting_VM(2, 5,0,0))
        q.put(Waiting_VM(3, 3,0,0))
        q.put(Waiting_VM(4, 4,0,0))
        self.assertEqual(q.get().vm_index, 2)
        self.assertEqual(q.get().vm_index, 4)
        self.assertEqual(q.get().vm_index, 3)
        self.assertEqual(q.get().vm_index, 1)
        self.assertEqual(q.empty(), True)
    
    def test_find_next_migration_vm(self):
        cur_pos = 0
        vm1 = 1
        expected_pos1 = 1
        self.host_schedule1[expected_pos1] = "o," +str(vm1)
        
        vm2 = 2
        expected_pos2 = 4
        self.host_schedule1[expected_pos2] = "o," +str(vm2)
        
        (cur_pos, migrating_vm_index) = find_next_migration_vm(cur_pos, self.host_schedule1)
        self.assertEqual(migrating_vm_index, vm1)
        self.assertEqual(cur_pos, expected_pos1+1)
        
        (cur_pos, migrating_vm_index) = find_next_migration_vm(cur_pos, self.host_schedule1)
        self.assertEqual(migrating_vm_index, vm2)
        self.assertEqual(cur_pos, expected_pos2+1)
        
        
    def test_calculate_provision_latency(self):
        cur_sec = 300
        nVDIs = 10
        vm_transitions_to_handle = {}
        resource_available = {}
        migration_schedule = {}
        interval = 300
        provision_latencies = []
        for i in range(nVDIs):
            vm_transitions_to_handle[i] = Q.PriorityQueue()
            resource_available[i] = 0
            migration_schedule[i] = []
            for j in range(interval):
                migration_schedule[i].append("r")
        vm_index1 = 1
        waiting_time1 = 0
        timestamp_of_becoming_active1 = 5
        host1 = 1
        waiting_vm1 = Waiting_VM(vm_index1, waiting_time1, timestamp_of_becoming_active1, host1)
        
        vm_index2 = 2
        waiting_time2 = 1
        timestamp_of_becoming_active2 = 205
        host2 = 2
        waiting_vm2 = Waiting_VM(vm_index2, waiting_time2, timestamp_of_becoming_active2, host2)
        
        vm_index3 = 3
        waiting_time3 = 1
        timestamp_of_becoming_active3 = 155
        waiting_vm3 = Waiting_VM(vm_index3, waiting_time3, timestamp_of_becoming_active3, host1)
        
        vm_index4 = 4
        waiting_time4 = 0
        timestamp_of_becoming_active4 = 12
        waiting_vm4 = Waiting_VM(vm_index4, waiting_time4, timestamp_of_becoming_active4, host2)
        
        vm_transitions_to_handle[host1].put(waiting_vm1)
        vm_transitions_to_handle[host2].put(waiting_vm2)
        vm_transitions_to_handle[host1].put(waiting_vm3)
        vm_transitions_to_handle[host2].put(waiting_vm4)
        
        calculate_provision_latency(cur_sec, nVDIs, vm_transitions_to_handle, resource_available, migration_schedule, provision_latencies)
        self.assertEqual(len(provision_latencies), 0)
        self.assertEqual(vm_transitions_to_handle[host1].qsize(), 2)
        self.assertEqual((vm_transitions_to_handle[host2].qsize()), 2)
        
        migration_pos = 50
        migration_schedule[host1][migration_pos] = "o," + str(vm_index1)
        migration_schedule[host1][migration_pos+1] = "o," + str(vm_index1)
        migration_schedule[host1][migration_pos+2] = "o," + str(vm_index1)
        
        
        calculate_provision_latency(cur_sec, nVDIs, vm_transitions_to_handle, resource_available, migration_schedule, provision_latencies)
        self.assertEqual(len(provision_latencies), 1)
        self.assertEqual(provision_latencies[0], cur_sec - timestamp_of_becoming_active1 + migration_pos + 2)
        self.assertEqual((vm_transitions_to_handle[host1].qsize()), 1)
        self.assertEqual((vm_transitions_to_handle[host2].qsize()), 2)
        
        migration_pos = 0
        for i in range(migration_pos, migration_pos+40):
            migration_schedule[host2][i] = "o," + str(9)
        migration_pos2 = 260   
        for i in range(migration_pos2, migration_pos2+40):
            migration_schedule[host2][i] = "o," + str(10)
            
        for i in range(migration_pos2-41, migration_pos2):
            migration_schedule[host2][i] = "s"
        calculate_provision_latency(cur_sec, nVDIs, vm_transitions_to_handle, resource_available, migration_schedule, provision_latencies)
        self.assertEqual(len(provision_latencies), 3)
        self.assertEqual(provision_latencies[1], cur_sec - timestamp_of_becoming_active2 + migration_pos + 40 + 1)
        self.assertEqual(provision_latencies[2], cur_sec - timestamp_of_becoming_active4 + migration_pos2 + 40)
        self.assertEqual((vm_transitions_to_handle[host1].qsize()), 1)
        self.assertEqual((vm_transitions_to_handle[host2].qsize()), 0)
        
        # migrate out one large VM other than any of the other VM, and see it is the one that gets the resources
        
if __name__ == '__main__':
    unittest.main()