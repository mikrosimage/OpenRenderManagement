#!/usr/bin/env python
####################################################################################################
# @file journal.py
# @package 
# @author acs, jbs, bud
# @date 2008/12/01
# @version 0.1
#
# @mainpage
# 
####################################################################################################
import pickle

## This class represents a List that is persisted in a file.
#
# @todo override all the remaining methods of List
#
class Journal(list):
    
    ## Creates a new Journal binded to the provided filename.
    # If the file already contains data, it means we are recovering from an unexpected termination.
    #
    # @param filename the path of the provided file
    #
    def __init__(self, framework,filename):
        super(Journal, self).__init__()
        self.framework = framework
        self.filename = filename
        self[:] = []
        """
        # should work once the worker is able to retrieve its commandwatchers
        if os.path.isfile(self.filename):
            readData = pickle.load(open(self.filename, 'rb'))
            for data in readData:
                data[0]= getattr(self.framework.application,data[0])
                self.append(data)
            print self
        """
    ##
    #
    def append(self, element):
        super(Journal, self).append(element)
        self.__writeToFile()

    ##
    #    
    def pop3(self, index = -1):
        a = super(Journal, self).pop(index)
        self.__writeToFile()
        return a
    
    
    ## Writes in the binded file all the content of the journal.
    #
    def __writeToFile(self):
        
        f = open(self.filename,'wb')
        toWrite = [[order[0].__name__]+order[1:] for order in self]
        p = pickle.Pickler(f)
        p.dump(toWrite)
        f.close()
