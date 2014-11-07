'''
Created on Apr 27, 2012

@author: Arnaud Chassagne
'''

from octopus.core.framework import ResourceNotFoundError
from octopus.dispatcher.webservice import DispatcherBaseResource
from octopus.core.communication.http import Http404, Http500


class LicensesResource(DispatcherBaseResource):
    #@queue
    def get(self):
        self.writeCallback(repr(self.dispatcher.licenseManager))


class LicenseResource(DispatcherBaseResource):
    #@queue
    def get(self, licenseName):
        try:
            lic = self.dispatcher.licenseManager.licenses[licenseName]
            licenseRepr = "{'max':%s, 'used':%s, 'rns':[" % (str(lic.maximum), str(lic.used))
            for rn in sorted(lic.currentUsingRenderNodes):
                licenseRepr += "\"%s\"," % rn.name
            licenseRepr += "]}"
            self.writeCallback(licenseRepr)
        except KeyError:
            raise ResourceNotFoundError

    #@queue
    def put(self, licenseName):
        data = self.getBodyAsJSON()
        try:
            maxLic = data['maxlic']
        except KeyError:
            raise Http404("Missing entry : 'maxlic'")
        else:
            self.dispatcher.licenseManager.setMaxLicensesNumber(licenseName, maxLic)
            self.writeCallback("OK")

    #@queue
    def delete(self, licenseName):
        data = self.getBodyAsJSON()
        try:
            rns = data['rns']
        except KeyError:
            raise Http404("Missing entry : 'rns'")
        else:
            rnsList = rns.split(",")
            for rnName in rnsList:
                if rnName in self.dispatcher.dispatchTree.renderNodes:
                    rn = self.dispatcher.dispatchTree.renderNodes[rnName]
                else:
                    raise Http500("Internal Server Error: Render node %s is not registered." % (rnName))

                self.dispatcher.licenseManager.releaseLicenseForRenderNode(licenseName, rn)
            self.writeCallback("OK")
