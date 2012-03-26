import unittest

import dispatcher
import core

def suite():
    return unittest.TestSuite((dispatcher.suite(), core.suite()))

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())

