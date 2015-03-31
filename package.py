name = 'puli'
version = 'eval'

# If your tool depends on some other package(s)
requires = [
    'rez-current',
    'python-2.7',
    'tornado-2.2.1',
    'requests',
    'psutil'
]


# If you need to define some environment variables
def commands():

    # Need to prepend the package root to make package version accessible (and not overidden by other packages like tornado)
    env.PYTHONPATH.prepend('{root}')
    env.PYTHONPATH.append('{root}/src')

    env.PATH.append('{root}')
    env.PATH.append('{root}/src/pulitools/puliquery')
    env.PATH.append('{root}/src/pulitools/puliexec')

    # Create some aliases
    # These aliases will be directly available in the shell.
    # alias('myTool', '{root}/bin/myTool "$@"')
    alias('puliexec', 'python {root}/src/pulitools/puliexec/puliexec.py')
    alias('pulrn', 'python {root}/src/pulitools/puliquery/pul_rn')
    alias('pul_rn', 'python {root}/src/pulitools/puliquery/pul_rn')
    alias('pulquery', 'python {root}/src/pulitools/puliquery/pul_query')
    alias('pul_query', 'python {root}/src/pulitools/puliquery/pul_query')

    alias('workerd', 'python {root}/src/octopus/workerd.py')
#    alias('dispatcherd', 'python {root}/src/octopus/dispatcherd.py')
