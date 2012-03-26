from unittest import TestCase

from octopus.dispatcher.model.view import View
from octopus.dispatcher.model.dispatchtree import DispatchTree

class TestViewConstructor(TestCase):
    
    def test_root_raises(self):
        self.assertRaises(ValueError,View.__call__,0)
        
class TestAliasList(TestCase):
    
    def setUp(self):
        self.dispatchTree = DispatchTree()
        stream = """\
pools
 renderfarm
  rn_1|4|1.5
view
 a|2
  c|2|1| 5|3:renderfarm
  d|1|1| 5|3:renderfarm
 b|1|1| 5|3:renderfarm
  e|3|1| 5|3:renderfarm
  f|2|1| 5|3:renderfarm
  g|1|1| 5|3:renderfarm
"""
        self.dispatchTree.getDatas(stream)
        
    
    def test_sort_alias_list(self):
        # r - a
        #     - c (EP)
        #     - d (EP)
        #   - b (EP)
        #     - e (EP)
        #     - f (EP)
        #     - g (EP)
        # --> depth-first, prioritized entry point (EP) list : c, d, e, f, g, b
        self.dispatchTree.pools["renderfarm"].getAvailableRenderNodes()
        view = self.dispatchTree.views[0]
        view.sortEntryPoints()
        t = [alias.content.name for alias in view.sortedEntryPoints]
        self.assertEqual(['c', 'd', 'e', 'f', 'g', 'b'], t)
        
  