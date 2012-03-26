from unittest import TestCase

from octopus.dispatcher.model.dispatchtree import DispatchTree
from octopus.dispatcher.model.task import Task


class TestCluster(TestCase):
    """
    At least one cluster has more than one element
    """
    def setUp(self):        
        self.dispatchTree = DispatchTree()
        stream = """\
pools
 renderfarm
  rn_1|4|1.5
  rn_2|4|1.5
  rn_3|2|2
  rn_4|2|2
view
 toto
  a|2
   t_c|2|1| 5|3:renderfarm
   t_d|1|1| 5|2:renderfarm
  b|2|1| 5|2:renderfarm
   t_e|3|1| 5|4:renderfarm
   t_f|2|1| 5|1:renderfarm
   t_g|1|1| 5|2:renderfarm
"""
        self.dispatchTree.getDatas(stream)
    
    def test_getnext(self):
        
        possibleField = [['t_c', 't_e', 't_d', 't_f', 't_g'],['t_e','t_c', 't_f',  't_d', 't_g']]
        
        
        
        alias = self.dispatchTree.getAliasByName("toto")
        cluster = alias.clusters[0]
        cpt = alias.comCount
        self.assertEqual(cpt, 5)
        tasks = []
        
        
        while 1:
            try:
                b = alias.getNextTopPriorityAlias().next()
                a = b
                while not isinstance(a , Task):
                    try:
                        a = a.getNextTopPriorityAlias().next()
                    except StopIteration:
                        break
                tasks.append(a.content.name)
                b.callCount += 1
                a.content.comCount -= 1
                cpt -= 1
            except StopIteration:
                break
        
        self.assertEqual(cpt, 0)
        self.assertEqual(alias.comCount, 0)
        self.assertTrue( tasks in possibleField)
     
     
        
class TestCluster2(TestCase):
    """
    All clusters have exactly one element
    """
    
    def setUp(self):        
        self.dispatchTree = DispatchTree()
        stream = """\
pools
 renderfarm
  rn_1|4|1.5
  rn_2|4|1.5
  rn_3|2|2
  rn_4|2|2
view
 toto
  a|3
   t_c|2|1| 5|3:renderfarm
   t_d|1|1| 5|2:renderfarm
  b|2|1| 5|2:renderfarm
   t_e|3|1| 5|4:renderfarm
   t_f|2|1| 5|1:renderfarm
   t_g|1|1| 5|2:renderfarm
"""
        self.dispatchTree.getDatas(stream)
    
    def test_getnext(self):
        
        possibleField = [['t_c', 't_d','t_e', 't_f', 't_g']]
        
        
        
        alias = self.dispatchTree.getAliasByName("toto")
        cluster = alias.clusters[0]
        cpt = alias.comCount
        self.assertEqual(cpt, 5)
        tasks = []
        
        
        while 1:
            try:
                b = alias.getNextTopPriorityAlias().next()
                a = b
                while not isinstance(a , Task):
                    try:
                        a = a.getNextTopPriorityAlias().next()
                    except StopIteration:
                        break
                tasks.append(a.content.name)
                b.callCount += 1
                a.content.comCount -= 1
                cpt -= 1
            except StopIteration:
                break
        
        self.assertEqual(cpt, 0)
        self.assertEqual(alias.comCount, 0)
        self.assertTrue( tasks in possibleField)    
