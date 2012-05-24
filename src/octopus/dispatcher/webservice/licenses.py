'''
Created on Apr 27, 2012

@author: acs
'''

from octopus.core.framework import ResourceNotFoundError, BaseResource, queue
from tornado.httpclient import HTTPError
    
    
class LicensesResource(BaseResource):
    @queue
    def get(self):
        self.writeCallback(repr(self.dispatcher.licenseManager))
        
class LicenseResource(BaseResource):
    @queue
    def get(self, licenseName):
        try:
            lic = self.dispatcher.licenseManager.licenses[licenseName]
            licenseRepr = "{'max':%s, 'used':%s, 'rns':[" % (str(lic.maximum), str(lic.used))
            for rn in lic.currentUsingRenderNodes:
                licenseRepr += "\"%s\"," % rn.name
            licenseRepr += "]}"
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
            self.dispatcher.licenseManager.setMaxLicensesNumber(licenseName, maxLic)
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
                self.dispatcher.licenseManager.releaseLicenseForRenderNode(licenseName, rn)
            self.writeCallback("OK")
             
            
            