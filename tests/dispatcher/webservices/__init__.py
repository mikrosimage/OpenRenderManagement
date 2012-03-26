import unittest

import controllers

def suite():
    return controllers.suite()

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
