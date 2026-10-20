from test_direct_managed import DirectManagedTests
from dhdgs.managed_cluster import ClusterHead


class ProcessChannelsManagedTests(DirectManagedTests):
    def setUp(self):
        super().setUp();old=self.head;old.close()
        old.c['peer']['head_direct'].update(queued_reports=True,report_backend='process',result_channels=2)
        self.head=ClusterHead(old.c,old.settings,self.token,old.truth)


class SharedChannelsManagedTests(ProcessChannelsManagedTests):
    def setUp(self):
        super().setUp();old=self.head;old.close()
        old.c['peer']['head_direct']['report_registration']='shared'
        self.head=ClusterHead(old.c,old.settings,self.token,old.truth)


class InlineChannelsManagedTests(SharedChannelsManagedTests):
    def setUp(self):
        super().setUp();old=self.head;old.close()
        old.c['peer']['head_direct']['completion_dispatch']='inline'
        self.head=ClusterHead(old.c,old.settings,self.token,old.truth)
