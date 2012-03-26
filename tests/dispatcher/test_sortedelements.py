from unittest import TestCase
from random import shuffle

import octopus.dispatcher.model.sorters as sorters
from octopus.dispatcher.model.sortedelements import SortedElements


class TestSortedElementsConstructor(TestCase):
    
    def setUp(self):
        self.elements = SortedElements()
    
    def test_default_sorter(self):
        val = 10
        numTest = 10
        self.elements += range(val)
        
        for i in range(numTest):
            shuffle(self.elements)
            self.elements.sort()
            self.assertEqual(self.elements,range(val))
        
        self.assertEqual(self.elements.sorter,sorters.DEFAULT)
        self.elements.sorter = sorters.DEFAULT
        
        for i in range(numTest):
            shuffle(self.elements)
            self.elements.sort()
            self.assertEqual(self.elements,range(val))
    
    def test_priority_base_sorter(self):
      class P: pass
           
      a = P()
      b = P()
      c = P()
      a.priority = 50
      b.priority = 30 
      c.priority = 20
      t = [a,b,c]
      for i in range(10):
         shuffle(t)
         t.sort(sorters.PRIORITY_BASE)
         self.assertEqual(t,[a,b,c])
         
    def test_fifo_sorter(self):
      class P: pass
           
      a = P()
      b = P()
      c = P()
      a.id = 50
      b.id = 30 
      c.id = 20
      t = [a,b,c]
      for i in range(10):
         shuffle(t)
         t.sort(sorters.FIFO)
         self.assertEqual(t,[c,b,a])        

