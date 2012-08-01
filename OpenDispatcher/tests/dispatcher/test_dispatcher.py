from unittest import TestCase

from octopus.dispatcher.dispatcher import Dispatcher
import octopus.dispatcher.dispatcher as dispatcher
from octopus.dispatcher.model.dispatchtree import DispatchTree
from octopus.dispatcher.model.command import Command

class MockRequester(object):
    
    def __init__(self, *args):
        pass
    
    def addRequest(self, *args):
        pass

dispatcher.HTTPRequestersSync = MockRequester

class TestDispatcherOnePool(TestCase):
    
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
 a|2
  t_c|2|1| 5|3:renderfarm
  t_d|1|1| 5|2:renderfarm
 b|1|1| 5|2:renderfarm
  t_e|3|1| 5|4:renderfarm
  t_f|2|1| 5|1:renderfarm
  t_g|1|1| 5|2:renderfarm
"""
        self.dispatchTree.getDatas(stream)
        
    def test_compute_assignable_rendernodes(self):
        
        refValue = ['t_c,3,renderfarm', 't_d,2,renderfarm', 't_e,4,renderfarm', 't_f,1,renderfarm', 't_g,2,renderfarm', 'b,2,renderfarm']
        modValue = ['t_c,2,renderfarm', 't_d,2,renderfarm', 't_e,4,renderfarm', 't_f,1,renderfarm', 't_g,2,renderfarm', 'b,1,renderfarm']
        
        self.dispatchTree.pools["renderfarm"].getAvailableRenderNodes()
        #Sort the entry points of the concerned view
        view = self.dispatchTree.views[0]
        view.sortEntryPoints()
        
        poolsPerEntryPoints = view.getPoolsPerEntryPoint()
        cmpValues = [",".join((alias.content.name,str(number),pool.name)) for alias,number,pool in poolsPerEntryPoints]
        #ensure we have correctly parsed the dispatchtree
        self.assertEqual( cmpValues , refValue)
        
        #get two rendernodes from the concerned pool
        pool = self.dispatchTree.pools["renderfarm"]
        rn1 = pool.renderNodes[0]
        rn2 = pool.renderNodes[1]
        
        #get two aliases from the dispatchtree
        aliasNames = [alias.content.name for alias in self.dispatchTree.aliases]
        idC = aliasNames.index("t_c")
        idF = aliasNames.index("t_f")
        
        #assign the two rendernodes to the two aliases
        d = Dispatcher(None)
        d.assignCommandToRenderNode(Command(), rn1, self.dispatchTree.aliases[idC], view.sortedEntryPoints[0])
        d.assignCommandToRenderNode(Command(), rn2, self.dispatchTree.aliases[idF], view.sortedEntryPoints[-1])
        
        poolsPerEntryPoints = view.getPoolsPerEntryPoint()
        cmpValues = [",".join((alias.content.name,str(number),pool.name)) for alias,number,pool in poolsPerEntryPoints]
        #ensure we have the expected values for the assigned rendernodes
        self.assertEqual( cmpValues , modValue)
        
        #unassign and ensure we have the original numbers of rendernodes
        d.unassignRenderNode(rn1)
        d.unassignRenderNode(rn2)
        poolsPerEntryPoints = view.getPoolsPerEntryPoint()
        cmpValues = [",".join((alias.content.name,str(number),pool.name)) for alias,number,pool in poolsPerEntryPoints]
        self.assertEqual( cmpValues , refValue)
       
        
class TestDispatcherMultiPool(TestCase):
    
    def setUp(self):
        self.dispatchTree = DispatchTree()
        stream = """\
pools
 renderfarm
  rn_1|4|1.5
  rn_2|4|1.5
  rn_3|2|2
  rn_4|2|2
 poule
  rn_toto|4|1.5
  rn_titi|1|2
  rn_tata|1|2
view
 a|2
  t_c|2|1| 5|3:renderfarm
  t_d|1|1| 5|1:renderfarm:1,2:poule:3
 b|1|1| 5|2:renderfarm:2,2:poule:1
  t_e|3|1| 5|4:renderfarm
  t_f|2|1| 5|1:renderfarm    
  t_g|1|1| 5|2:renderfarm
"""
        self.dispatchTree.getDatas(stream)
        
    def test_compute_assignable_rendernodes(self):
        
        refValue = ['t_c,3,renderfarm', 't_d,2,poule', 't_d,1,renderfarm', 't_e,4,renderfarm', 't_f,1,renderfarm', 't_g,2,renderfarm', 'b,2,renderfarm', 'b,2,poule']
        modValue = ['t_c,2,renderfarm', 't_d,2,poule', 't_d,1,renderfarm', 't_e,4,renderfarm', 't_f,1,renderfarm', 't_g,2,renderfarm', 'b,2,renderfarm', 'b,1,poule']
        
        #fetch the available rendernodes of each pool
        self.dispatchTree.pools["renderfarm"].getAvailableRenderNodes()
        self.dispatchTree.pools["poule"].getAvailableRenderNodes()
        #sort the entry points of the concerned view
        view = self.dispatchTree.views[0]
        view.sortEntryPoints()
        
        poolsPerEntryPoints = view.getPoolsPerEntryPoint()
        cmpValues = [",".join((alias.content.name,str(number),pool.name)) for alias,number,pool in poolsPerEntryPoints]
        #ensure we have correctly parsed the dispatchtree
        self.assertEqual( cmpValues , refValue)
        
        #get three rendernodes from the concerned pools
        firstPool = self.dispatchTree.pools["renderfarm"]
        rn1 = firstPool.renderNodes[0]
        secondPool = self.dispatchTree.pools["poule"]
        tata = secondPool.renderNodes[2]
        
        #get three aliases from the dispatchtree
        aliasNames = [alias.content.name for alias in self.dispatchTree.aliases]
        idC = aliasNames.index("t_c")
        idF = aliasNames.index("t_f")
        
        #assign the rendernodes to the aliases
        d = Dispatcher(None)
        d.assignCommandToRenderNode(Command(), rn1, self.dispatchTree.aliases[idC], view.sortedEntryPoints[0])
        d.assignCommandToRenderNode(Command(), tata, self.dispatchTree.aliases[idF], view.sortedEntryPoints[-1])
        
        poolsPerEntryPoints = view.getPoolsPerEntryPoint()
        cmpValues = [",".join((alias.content.name,str(number),pool.name)) for alias,number,pool in poolsPerEntryPoints]
        #ensure we have the expected values for the assigned rendernodes
        self.assertEqual(cmpValues , modValue)
        
        #unassign and ensure we have the original numbers of rendernodes
        d.unassignRenderNode(rn1)
        d.unassignRenderNode(tata)
        poolsPerEntryPoints = view.getPoolsPerEntryPoint()
        cmpValues = [",".join((alias.content.name,str(number),pool.name)) for alias,number,pool in poolsPerEntryPoints]
        self.assertEqual( cmpValues , refValue)

class TestAssignRenderNode(TestCase):
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
 a|2
  t_c|2|1| 5|3:renderfarm
  t_d|1|1| 5|2:renderfarm
 b|1|1| 5|2:renderfarm
  t_e|3|1| 5|4:renderfarm
  t_f|2|1| 5|1:renderfarm
  t_g|1|1| 5|2:renderfarm
"""
        self.dispatchTree.getDatas(stream)
        
    def test_raise_value_error_on_folder_assign(self):
        self.dispatchTree.pools["renderfarm"].getAvailableRenderNodes()
        view = self.dispatchTree.views[0]
        view.sortEntryPoints()
        rn1 = self.dispatchTree.pools["renderfarm"].renderNodes[0]
        aliasNames = [alias.content.name for alias in self.dispatchTree.aliases]
        idA = aliasNames.index("a")
        
        d = Dispatcher(None)
        self.assertRaises(ValueError, d.assignCommandToRenderNode, Command(), rn1, self.dispatchTree.aliases[idA], view.sortedEntryPoints[0])
