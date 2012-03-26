from unittest import TestCase

from octopus.dispatcher.model.dispatchtree import DispatchTree
from octopus.dispatcher.model.task import Task

class TestDispatchTreeConstructor(TestCase):

    def setUp(self):
        self.dispatchTree = DispatchTree()
        self.dispatchTree.getDatas()
        
    def test_attributes_must_be_defined(self):
        
        self.assertTrue(self.dispatchTree.pools)
        self.assertTrue(self.dispatchTree.folders)
        self.assertTrue(self.dispatchTree.aliases)
        self.assertTrue(self.dispatchTree.renderNodes)
        
    def test_aliases_parents_are_well_set(self):
        
        for i in range(len(self.dispatchTree.aliases)):
            
            if not i: 
                
                self.assertTrue(not self.dispatchTree.aliases[i].parent)
                continue
            
            currentDepth = self.dispatchTree.aliases[i].depth
            previousDepth = self.dispatchTree.aliases[i-1].depth
            diff =  currentDepth - previousDepth
            
            if diff > 0:
                
                # second alias is a child of the first one
                self.assertTrue(self.dispatchTree.aliases[i] in self.dispatchTree.aliases[i-1].children,"second alias is a child of the first one")
            
            elif diff == 0:
                
                # have same parent
                self.assertEqual(self.dispatchTree.aliases[i].parent, self.dispatchTree.aliases[i-1].parent)
            
            elif diff < 0:
                
                # cannot be in the same generation
                p1 = self.dispatchTree.aliases[i]
                p2 = self.dispatchTree.aliases[i+1]
                while 1:
                    self.assertTrue(not p1.parent==p2.parent)
                    p1 = p1.parent
                    p2 = p2.parent
                    if not (p1 and p2): break
                
                # commun ancester can be precisly defined
                
                communAncester = self.dispatchTree.aliases[i-1].parent

                while diff:
                    communAncester = communAncester.parent
                    diff += 1
                
                self.assertEqual(communAncester,self.dispatchTree.aliases[i].parent)
                
    def test_aliases_clusters_and_children_are_well_set(self):
        
        for alias in self.dispatchTree.aliases:
            
            children = alias.children            
            clusters = alias.clusters
            
            priority = set([child.priority for child in children])
            
            oldChildrenLen = len(children)
            
            children = [x for x in set(children)]
            
            # each child is unique
            self.assertEqual(oldChildrenLen,len(children),"each alias child should be unique in its parent .children attribute")
            
            # each priority is represented in the clusters
            clustersChildren = []
            
            for p in priority:
                self.assertTrue(p in [c.priority for c in alias.clusters],"each child priority must be represented by a cluster")
                clustersChildren += [c for c in alias.clusters if c.priority == p][0]
            
            self.assertEqual(oldChildrenLen,len(clustersChildren))
            
            for child in children:
                self.assertTrue(child in clustersChildren)
            
            
class TestDispatchtreeMapping(TestCase):            

    def setUp(self):
        self.dispatchTree = DispatchTree()
        self.dispatchTree.getDatas()
        
        
    def test_submission_of_a_new_alias(self):
        task = Task("submittedTask")
        
        for view in self.dispatchTree.views:
            view.mappingRules = lambda a: ([ [["a"],["b",3,4],["c",2,3,5],[task.name]],[["eee"]]])
        
        self.dispatchTree.createAliasesForNewTask(task)
        print self.dispatchTree
            
            
            
            
