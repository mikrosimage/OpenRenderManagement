import time
import collections

PROFILE = False

if PROFILE:
    records = collections.defaultdict(lambda: collections.defaultdict(int))
    
    def profile(name):
        def decorator(func):
            def instrumented_func(*args, **kwargs):
                t = time.time()
                result = func(*args, **kwargs)
                t = time.time() - t
                records[name]["time"] += t
                records[name]["calls"] += 1
                return result
            return instrumented_func
        return decorator
    
    def reset():
        records.clear()
    
    def printRecords():
        for record in records:
            r = records[record]
            t = r["time"]
            c = r["calls"]
            a = t / c
            print "%30s %8.2fs %6r %.2f" % (record, t, c, a)
else:
    def profile(name):
        return lambda func: func
    
    def reset():
        pass
    
    def printRecords():
        pass


if __name__ == '__main__':

    @profile("f")
    def f():
        for i in xrange(100):
            g()

    @profile("g")
    def g():
        pass

    f()
    print records

