"""Exercise managed hot updates with spawned callback ownership."""
import test_managed_cluster
from dhdgs.managed_cluster import ClusterHead

class ReceiverManagedTests(test_managed_cluster.ManagedClusterTests):
    def setUp(self):
        super().setUp()
        old=self.head;old.close()
        old.c['peer']['head_receiver_processes']=2
        self.head=ClusterHead(old.c,old.settings,self.token,old.truth)
