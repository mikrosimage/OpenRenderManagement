import unittest

import test_dispatchtree

def suite():
    return test_dispatchtree.suite()

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
