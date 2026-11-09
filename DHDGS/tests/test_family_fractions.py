"""Administrative family updates preserve aggregate routing shares atomically."""
import copy,threading,unittest
from dhdgs.load_balance import LoadBalancer
class FamilyFractionsTests(unittest.TestCase):
 def setUp(self):
  self.b=LoadBalancer.__new__(LoadBalancer);self.b.lock=threading.RLock();self.b.b={'max_fraction':.8}
  self.b.regions={'a':dict(family_id='f',target=1,fraction=.3),'b':dict(family_id='f',target=2,fraction=.4),'c':dict(family_id='other',target=2,fraction=.1)}
  self.events=[];self.b.log=lambda *a,**kw:self.events.append((a,kw))
 def test_joint_update(self):
  self.b.set_family_fractions('f',{1:.35,2:.45})
  self.assertEqual([r['fraction'] for r in self.b.regions.values()],[.35,.45,.1]);self.assertEqual(len(self.events),1)
 def test_rejected_updates_do_not_partially_apply(self):
  for values in [{1:.7,2:.2},{1:.4},{1:.3,2:.3,3:.1},{1:-.1,2:.2},{1:float('nan'),2:.2},{1:True,2:.2},{'1':.3,2:.3}]:
   before=copy.deepcopy(self.b.regions)
   with self.assertRaises(ValueError):self.b.set_family_fractions('f',values)
   self.assertEqual(self.b.regions,before)
  self.assertEqual(self.events,[])
 def test_unknown_and_duplicate_targets_rejected(self):
  with self.assertRaises(ValueError):self.b.set_family_fractions('missing',{1:.2})
  self.b.regions['d']=dict(family_id='f',target=1,fraction=.1)
  with self.assertRaises(ValueError):self.b.set_family_fractions('f',{1:.3,2:.3})
 def test_zero_share_allowed_without_removing_copy(self):
  self.b.set_family_fractions('f',{1:0,2:.8});self.assertEqual(len(self.b.regions),3)
if __name__=='__main__':unittest.main()
