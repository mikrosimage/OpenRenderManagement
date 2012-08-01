import unittest
from octopus.core.framework.webservice import Mapping, MappingSet

## A test case for the Mapping.match method
#
# This test consists of 4 parts.
#
# The first part checks that the mappings with no argument work.
# The second one checks that unnamed arguments mappings work.
# The third one checks that named arguments mappings work.
# The last one checks that a mix of named and unnamed arguments work.
#
class TestMapping(unittest.TestCase):
    
    def callback(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        return True
    
    def setUp(self):
        self.mapping_noargs = Mapping(r"^/home/$", self.callback)
        self.mapping_nonamedargs = Mapping(r"^/archive/by_date/(\d\d\d\d).(\d\d).(\d\d)/$", self.callback)
        self.mapping_namedargs = Mapping(r"^/views/by_name/(?P<name>\w+)/$", self.callback)
        self.mapping_everythingargs = Mapping(r"^/journals/(?P<username>\w+)/by_date/(\d\d\d\d).(\d\d).(\d\d)/$", self.callback)
    
    def tearDown(self):
        self.args = None
        self.kwargs = None
    
    def test_match_noargs(self):
        mapping = self.mapping_noargs

        # These should NOT work.
        self.assertFalse(mapping.match(None, "/home"))
        self.assertFalse(mapping.match(None, "/"))
        self.assertFalse(mapping.match(None, "/home/bob/"))
        
        # This one MUST work.
        self.assertTrue(mapping.match(None, "/home/"))
        self.assertEqual(self.args, (None, ))
        self.assertEqual(self.kwargs, dict())
    
    
    def test_match_nonamedargs(self):
        mapping = self.mapping_nonamedargs

        # These should NOT work.
        self.assertFalse(mapping.match(None, "/archive/by_date/20020817"))
        self.assertFalse(mapping.match(None, "/archive/by_date/2002.08.17"))
        self.assertFalse(mapping.match(None, "/"))
        self.assertFalse(mapping.match(None, "/home/bob/"))
        
        # This one MUST work.
        self.assertTrue(mapping.match(None, "/archive/by_date/2002.08.17/"))
        self.assertEqual(self.args, (None, "2002", "08", "17"))
        self.assertEqual(self.kwargs, dict())


    def test_match_namedargs(self):
        mapping = self.mapping_namedargs

        # These should NOT work.
        self.assertFalse(mapping.match(None, "/views/by_name//"))
        self.assertFalse(mapping.match(None, "/view/by_name/prod"))
        self.assertFalse(mapping.match(None, "/"))
        self.assertFalse(mapping.match(None, "/home/bob/"))
        
        # This one MUST work.
        self.assertTrue(mapping.match(None, "/views/by_name/prod/"))
        self.assertEqual(self.args, (None, ))
        self.assertEqual(self.kwargs, { "name": "prod" })
    


    def test_match_everythingargs(self):
        mapping = self.mapping_everythingargs

        # These should NOT work.
        self.assertFalse(mapping.match(None, "/journals/bob/by_date/2008.12.05"))
        self.assertFalse(mapping.match(None, "/journals/bob/by_date/20081205/"))
        self.assertFalse(mapping.match(None, "/home/bob/"))
        self.assertFalse(mapping.match(None, "/journals/by_date/2008.12.05/"))
        
        # This one MUST work.
        self.assertTrue(mapping.match(None, "/journals/bob/by_date/2008.12.05/"))
        self.assertEqual(self.args, (None, "2008", "12", "05"))
        self.assertEqual(self.kwargs, { "username": "bob" })

## A simple test case for the MappingSet.add method
#
# The test runs as follow:
# - create a new empty MappingSet
# - check that "/prods/" and /prods/prod1/ do not match the set
# - add a "/prods/" mapping
# - check that "/prods/" matches and "/prods/prod1/" still does not
# - add a "/prods/{prodname}/" mapping
# - check that the set now matches both "/prods/" and "/prods/prod1/"
#
class TestMappingSetAdd(unittest.TestCase):

    def callback(self, request, *args):
        pass

    def setUp(self):
        self.mappings = MappingSet()

    def test_add(self):
        self.assertFalse(self.mappings.match(None, "/prods/"))
        self.assertFalse(self.mappings.match(None, "/prods/prod1/"))
        
        # add the mapping to /prods/ and test!
        self.mappings.add(Mapping(r"^/prods/$", self.callback))
        self.assertTrue(self.mappings.match(None, "/prods/"))
        self.assertFalse(self.mappings.match(None, "/prods/prod1/"))
        
        # add the mapping to /prods/{viewname}/ and test!
        self.mappings.add(Mapping(r"^/prods/(\w+)/$", self.callback))
        self.assertTrue(self.mappings.match(None, "/prods/prod1/"))
        self.assertTrue(self.mappings.match(None, "/prods/prod1/"))
        
        self.mappings.add(("^/tmp/$", self.callback))
        self.assertTrue(self.mappings.match(None, "/tmp/"))


## A simple test case for the MappingSet.match method
#
# This test creates the following mapping set:
# - /views/  -->  list_views
# - /views/{viewname}/  -->  detail_view
# - /views/{viewname}/path/to/element  -->  detail_view_element
#
# It then checks that the set matches:
# - /views/ calls list_views with no argument
# - /views/prods/ calls detail_view with "prods" as viewname argument
# - /views/prods/prod1/plan2 calls detail_view_element with "prods" as viewname argument and "/prod1/plan2/" as named argument "path".
#
class TestMappingSetMatch(unittest.TestCase):

    def list_views(request, self):
        pass
    
    def detail_view(self, request, viewname):
        self.assertEqual(viewname, "prods")
    
    def detail_view_element(self, request, viewname, path):
        self.assertEqual(viewname, "prods")
        self.assertEqual(path, "/prod1/plan2/")

    def setUp(self):
        self.set = MappingSet(
            (r"^/views/$", self.list_views),
            (r"^/views/(\w+)/$", self.detail_view),
            (r"^/views/(\w+)(?P<path>/(?:\w+/)+)$", self.detail_view_element),
        )

    def test_match(self):
        self.assertTrue(self.set.match(None, "/views/"))
        self.assertTrue(self.set.match(None, "/views/prods/"))
        self.assertTrue(self.set.match(None, "/views/prods/prod1/plan2/"))
