'''
Created on Apr 27, 2012

@author: acs
'''

from octopus.core.framework import ResourceNotFoundError, BaseResource, queue
from tornado.httpclient import HTTPError
    
    
class LicensesResource(BaseResource):
    @queue
    def get(self):
        self.writeCallback(repr(self.dispatcher.licenceManager))
        
class LicenseResource(BaseResource):
    @queue
    def get(self, licenseName):
        try:
            lic = self.dispatcher.licenceManager.licences[licenseName]
            licenseRepr = repr(lic) + " by {"
            for rn in lic.currentUsingRenderNodes:
                licenseRepr += rn.name + ","
            licenseRepr += "}"
            self.writeCallback(licenseRepr)
        except KeyError:
            raise ResourceNotFoundError
    
    @queue
    def put(self, licenseName):
        data = self.getBodyAsJSON()
        try:
            maxLic = data['maxlic']
        except KeyError:
            return HTTPError(404, "Missing entry : 'maxlic'")
        else:
            self.dispatcher.licenceManager.setMaxLicencesNumber(licenseName, maxLic)
            self.writeCallback("OK")
            
    @queue
    def delete(self, licenseName):
        data = self.getBodyAsJSON()
        try:
            rns = data['rns']
        except KeyError:
            return HTTPError(404, "Missing entry : 'rns'")
        else:
            rnsList = rns.split(",")
            for rn in rnsList:
                self.dispatcher.licenceManager.releaseLicenceForRenderNode(licenseName, rn)
            self.writeCallback("OK")
             
            
            