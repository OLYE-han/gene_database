import test_managed_cluster
from dhdgs.managed_cluster import ClusterHead

class PipelineManagedTests(test_managed_cluster.ManagedClusterTests):
    def setUp(self):
        super().setUp();old=self.head;old.close()
        old.c['peer']['head_pipeline']['enabled']=True
        self.head=ClusterHead(old.c,old.settings,self.token,old.truth)
