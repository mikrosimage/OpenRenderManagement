import unittest

def suite():
    return unittest.TestSuite([])

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
