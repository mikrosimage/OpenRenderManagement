'''

.. module:: puliclient
    :platform: Unix
    :synopsis: API to create and submit jobs on the renderfarm

.. moduleauthor:: Jean-Baptiste Spieser

Created on 25 nov. 2009

'''
import os

from octopus.dispatcher import settings


class LicenseManager:
    class License:
        def __init__(self, name, maximum):
            self.name = name
            self.maximum = int(maximum)
            self.used = 0
            self.currentUsingRenderNodes = []

        def __repr__(self):
            return "\"" + self.name + "\" : \"" + str(self.used) + " / " + str(self.maximum) + "\""

        def licenseInfo(self):
            return { 'name': self.name, "used":self.used, "total":self.maximum, "rns": [ rn.name for rn in self.currentUsingRenderNodes ] }

        def reserve(self):
            if self.used < self.maximum:
                self.used += 1
                return True
            return False

        def release(self):
            if self.used > 0:
                self.used -= 1

        def setMaxNumber(self, maxNumber):
            self.maximum = maxNumber

    def __init__(self):
        self.licenses = {}
        self.readLicensesData()

    def readLicensesData(self):
        if not os.path.exists(settings.FILE_BACKEND_LICENCES_PATH):
            raise Exception("Licenses file missing: %s" % settings.FILE_BACKEND_LICENCES_PATH)
        else:
            fileIn = open(settings.FILE_BACKEND_LICENCES_PATH, "r")
            lines = [line for line in fileIn.readlines() if not line.startswith("#")]
            fileIn.close()

            newLicense = None
            for line in lines:
                line = line.strip()
                if line:
                    newLicense = LicenseManager.License(*line.strip().split(" "))
                    self.licenses[newLicense.name] = newLicense

    def releaseLicenseForRenderNode(self, licenseName, renderNode):
        """
        :licenseName: 
        :renderNode: render node object expected
        """
        if "&" not in licenseName:
            licenseName += "&"
        for licName in licenseName.split("&"):
            if len(licName):
                try:
                    lic = self.licenses[licName]
                    try:
                        if renderNode in lic.currentUsingRenderNodes:
                            rnId = lic.currentUsingRenderNodes.index(renderNode)
                            del lic.currentUsingRenderNodes[rnId]

                            lic.release()
                    except IndexError:
                        print "Cannot release license %s for renderNode %s" % (licName, renderNode)
                except KeyError:
                    print "License %s not found" % licName

    def reserveLicenseForRenderNode(self, licenseName, renderNode):
        if "&" not in licenseName:
            licenseName += "&"
        globalsuccess = True
        liclist = []
        for licName in licenseName.split("&"):
            if len(licName):
                try:
                    lic = self.licenses[licName]
                    success = lic.reserve()
                    if success:
                        lic.currentUsingRenderNodes.append(renderNode)
                        liclist.append(lic)
                    else:
                        # if only one reservation fails, the whole reservation fails
                        globalsuccess = False
                except KeyError:
                    print "License %s not found" % licName
                    globalsuccess = False
        # in case of reservation failure, release the already reserved licenses, if any
        if not globalsuccess:
            for lic in liclist:
                rnId = lic.currentUsingRenderNodes.index(renderNode)
                del lic.currentUsingRenderNodes[rnId]
                lic.release()
        return globalsuccess

    def showLicenses(self):
        for lic in self.licenses.values():
            print lic

    def stats(self):
        """
        Get useful information on licenses declared on the server.

        :return: a list of dict, each of them being a license description like { 'name': 'shave', 'total': 70, 'used': 0, 'rns': []  }
        """
        res=[]
        for lic in self.licenses.keys():
            res.append( self.licenses[lic].licenseInfo() )

        return res


    def __repr__(self):
        rep = "{"
        for lic in self.licenses.values():
            rep += repr(lic) + ","
        # get rid of the last coma
        rep = rep[:-1]
        rep += "}"
        return rep

    def setMaxLicensesNumber(self, licenseName, number):
        try:
            lic = self.licenses[licenseName]
            if lic.maximum != number:
                lic.setMaxNumber(number)
        except KeyError:
            print "License %s not found... Creating new entry" % licenseName
            self.licenses[licenseName] = LicenseManager.License(licenseName, number)
