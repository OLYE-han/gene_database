import test_receiver_managed
from dhdgs.managed_cluster import ClusterHead

class FlowManagedTests(test_receiver_managed.ReceiverManagedTests):
    def setUp(self):
        super().setUp()
        old=self.head;old.close()
        old.c['peer'].update(head_receiver_ipc='split',head_query_workers=2,head_entry_workers=2)
        self.head=ClusterHead(old.c,old.settings,self.token,old.truth)
