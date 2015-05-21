name = 'puli'
version = '1.7.14'

# If your tool depends on some other package(s)
requires = [
    'rez-current',
    'python-2.7',
    'tornado-2.2.1',
    'requests',
    'psutil',
    'sqlobject',
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
    alias('puliexec', 'python {root}/src/pulitools/puliexec/puliexec.py')
    alias('pul_rn', 'python {root}/src/pulitools/puliquery/pul_rn')
    alias('pul_query', 'python {root}/src/pulitools/puliquery/pul_query')

    # Dev tools
    alias('puli_workerd', 'python {root}/src/octopus/workerd.py')
    alias('puli_workerd_dev', 'python {root}/src/octopus/workerd.py -s localhost --debug --console -p 9000 -K /tmp/render/kill9000 -P /tmp/worker9000.pid')

    alias('puli_dispatcherd', 'python {root}/src/octopus/dispatcherd.py')
    alias('puli_dispatcherd_dev', 'python {root}/src/octopus/dispatcherd.py --debug --console -p 8004')