#!/usr/bin/python
# coding: utf-8

import time


class MyClass:
    def myMethod(self, param1, param2, wait):

        print("Method param received:")
        print("  - param1: %r" % param1)
        print("  - param2: %r" % param2)
        print("  - wait: %r" % wait)

        print("About to wait for %d seconds..." % int(wait))
        time.sleep(int(wait))
        print("Done.")


def myFunction(param1, param2, wait):
    print("Function param received:")
    print("  - param1: %r" % param1)
    print("  - param2: %r" % param2)
    print("  - wait: %r" % wait)

    print("About to wait for %d seconds..." % int(wait))
    time.sleep(int(wait))
    print("Done.")
