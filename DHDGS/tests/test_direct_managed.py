import test_managed_cluster
from dhdgs.managed_cluster import ClusterHead
import asyncio
from unittest.mock import patch
import dhdgs.head_direct as direct

class DirectManagedTests(test_managed_cluster.ManagedClusterTests):
    def setUp(self):
        super().setUp();old=self.head;old.close()
        old.c['peer']['head_direct'].update(enabled=True,distribution_threads=2,
            confirmation_threads=2,result_processes=2,save_results=False)
        self.head=ClusterHead(old.c,old.settings,self.token,old.truth)

    def test_real_data_node_deduplicates_lost_ack_retry(self):
        self.head.configure({},recover=True)
        original=direct.read_frame;replies=[]
        async def lose_first(reader,limit):
            reply,rx=await original(reader,limit)
            if asyncio.current_task().get_coro().__qualname__=='DirectHub._confirm':
                replies.append(reply)
                if len(replies)==1:raise ConnectionError('test: drop entry ACK after data accepted')
            return reply,rx
        with patch.object(direct,'read_frame',lose_first):result=self.head.query(self.x[0],0)
        self.assertEqual(result['status'],'ok')
        self.assertEqual(len(replies),2)
        self.assertTrue(replies[1]['result']['duplicate'])
        self.assertEqual(result['stats']['entry_submit_retries'],1)
        self.assertEqual(result['stats']['node_revisit_count'],0)
        self.assertLessEqual(result['stats']['expanded'],self.head.c['search']['max_expansions'])
