from unittest import TestCase

from octopus.dispatcher.model.alias import Alias,View,Folder,Task
from octopus.dispatcher.model.dispatchtree import DispatchTree

class TestAliasConstructor(TestCase):
    
    def setUp(self):

        self.tree = DispatchTree()
        self.tree.getDatas()
        
        self.view = None
        self.content = Folder("folderTest")
        self.priority = 1
        self.dispatch_key = 1
        self.max_rn = 1
        
        self.raw_pools = "3:%s,4:%s," %(self.tree.pools.keys()[0],self.tree.pools.keys()[1])
        self.depth = 0
    
    def check_raises(self, ExceptionType=ValueError):
        self.assertRaises(ExceptionType, Alias.__call__, self.tree, self.view, self.content, self.priority, self.dispatch_key, self.max_rn, self.raw_pools, self.depth)
    
    def test_init_raises_value_error_on_depth_and_view_both_null(self):
        self.view,self.depth = range(2) # view only should be null for the test
        self.check_raises(ValueError)

    def test_init_raises_value_error_on_nonstring_raw_pools(self):

        self.raw_pools = None
        self.check_raises(ValueError)

    def test_init_raises_value_error_on_nonint_depth(self):

        self.depth = None
        self.check_raises(ValueError)
        
    def test_init_valid(self):         
           
        
        a = Alias( self.tree, self.view, self.content, self.priority, self.dispatch_key, self.max_rn, self.raw_pools, self.depth)
        
        self.assertTrue(isinstance(a.view, View))
        self.assertEqual(a.view.root, a)
        
        self.assertEqual(self.tree, a.tree)
        self.assertTrue(isinstance(a.view,View))        
        
        for pool in a.pools:
            self.assertTrue(a in pool[1].concernedAlias)
        
        self.assertEqual(len(a.pools),self.raw_pools.count(",")+int(self.raw_pools[-1] != ","))
        
import random      
        
class TestAliasSorters(TestCase): 
   
    def test_alias_satisfaction_sorter(self):
       class P: pass
      
       a = P()
       b = P()
       c = P()
       a.satisfaction = 50
       b.satisfaction = 30 
       c.satisfaction = 20
       t = [a,b,c]
       for i in range(10):
          random.shuffle(t)
          t.sort(Alias.SATISFACTION_SORTER)
          self.assertEqual(t,[a,b,c])
        
    def test_alias_pool_sorter(self):
       
      
       a = [1,0,40]
       b = [1,0,20]
       c = [1,0,10]
       t = [a,b,c]
       for i in range(10):
          random.shuffle(t)
          t.sort(Alias.POOL_SORTER)
          self.assertEqual(t,[a,b,c])
        
class TestSettree(TestCase): 
   
   def setUp(self):
      
        self.tree = DispatchTree()
        self.tree.getDatas()
        
        self.view = None
        self.content = Folder("folderTest")
        self.content2 = Folder("folderTest2")
        self.priority = 1
        self.dispatch_key = 1
        self.max_rn = 1
        
        self.raw_pools = "3:%s,4:%s," %(self.tree.pools.keys()[0],self.tree.pools.keys()[1])
        self.depth = 0
        
        self.alias = Alias(self.tree, self.view, self.content, self.priority, self.dispatch_key, self.max_rn, self.raw_pools, self.depth)                  
        self.view = self.alias.view
        self.depth = 1
        self.alias2 = Alias(self.tree, self.view, self.content2, self.priority, self.dispatch_key, self.max_rn, self.raw_pools, self.depth)                  
          
   def test_set_parent(self):
      self.alias.setParent(self.alias2)
      self.assertTrue(self.alias in self.alias2.children)
      self.assertEqual(self.alias.parent, self.alias2)
   

class TestComCountPropagation(TestCase):
    
    def setUp(self):
        
        self.tree = DispatchTree()
        self.tree.pools
        