from unittest import TestCase
from mock import Mock
from octopus.dispatcher.webservice.rendernodes import RenderNodeController

class RendernodeControllerTestCase(TestCase):
    
    def setUp(self):
        self.framework = Mock()
        self.controller = RenderNodeController(self.framework)
        