'''
Created on 25 nov. 2009

@author: jean-baptiste.spiese
'''
import os

from octopus.dispatcher import settings


class LicenceManager:
    
    
    class Licence:
        def __init__(self, name, maximum, renderNodeMode=False):
            if isinstance(renderNodeMode, str) and renderNodeMode.lower() == "false":
                renderNodeMode = False
            self.name = name
            self.maximum = int(maximum)
            self.used = 0
            self.renderNodeMode = renderNodeMode
            self.currentUsingRenderNodes = []
            

        def __repr__(self):
            return self.name + " : " + str(self.used) + "/" + str(self.maximum) + " on use."

        def reserve(self):
            if self.used < self.maximum:                                   
                self.used += 1
                return True            
            return False   
    
        def release(self):
            self.used -= 1
            #self.used = max(self.used,0)
            
        def setRenderNodeMode(self,renderNodeMode):
            if renderNodeMode and self.renderNodeMode: return
            self.renderNodeMode = renderNodeMode
            if renderNodeMode:
                # let's free some licence
                diff = len(self.currentUsingRenderNodes) - len(set(self.currentUsingRenderNodes))
                self.used -= diff
            else:
                # let's use a licence by command
                self.used = len(self.currentUsingRenderNodes)
        
        def setMaxNumber(self,maxNumber):
            self.maximum = maxNumber
    
    def __init__(self):
        self.licences = {}
        self.readLicencesData()
        
        
    def readLicencesData(self):
        if not os.path.exists(settings.FILE_BACKEND_LICENCES_PATH):
            raise Exception("Licence file missing: %s" % settings.FILE_BACKEND_LICENCES_PATH)
        else:
            fileIn = open(settings.FILE_BACKEND_LICENCES_PATH, "r")
            lines = [line for line in fileIn.readlines() if not line.startswith("#")]
            fileIn.close()
            
            newLicence = None
            for line in lines:
                line = line.strip()
                if line: 
                    newLicence = LicenceManager.Licence(*line.strip().split(" "))
                    self.licences[newLicence.name] = newLicence


    def releaseLicenceForRenderNode(self, licenceName,renderNode):        
        try:
            lic = self.licences[licenceName]
            try:
                rnId = lic.currentUsingRenderNodes.index(renderNode)
                del lic.currentUsingRenderNodes[rnId]
                if (not lic.renderNodeMode) or (not renderNode in lic.currentUsingRenderNodes):
                    # last instance of rendernode for that licence
                    lic.release()
            except IndexError:
                print "cannot release licence",licenceName,"for renderNode",renderNode               
        except KeyError:
            print "Licence " + licenceName + " not found"
            return False       
             
    def reserveLicenceForRenderNode(self, licenceName,renderNode):      
       
        try:
            lic = self.licences[licenceName]
            if lic.renderNodeMode and (renderNode in lic.currentUsingRenderNodes):
                success = True
            else:
                success = lic.reserve()
            if success:
                lic.currentUsingRenderNodes.append(renderNode)
            return success           
            
        except KeyError, e:
            print "Licence " + licenceName + " not found"
            return False

    def showLicences(self):
        for licence in self.licences.values():
            print licence
    

    def setMaxLicencesNumber(self,licenceName,number):
        try:
            lic = self.licences[licenceName]
            if lic.maximum !=  number:
                lic.setMaxNumber(number)            
        except KeyError, e:
            print "Licence " + licenceName + " not found"
            return False
    
    def setRenderNodeMode(self,licenceName,renderNodeMode):
        try:
            lic = self.licences[licenceName]
            if lic.renderNodeMode !=  renderNodeMode:
                lic.setRenderNodeMode(renderNodeMode)            
        except KeyError, e:
            print "Licence " + licenceName + " not found"
            return False

