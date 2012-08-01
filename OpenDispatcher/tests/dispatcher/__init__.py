import unittest

import webservices, model

def suite():
    return unittest.TestSuite([webservices.suite(), model.suite()])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
