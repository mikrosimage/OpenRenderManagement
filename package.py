name = 'puli'
version = '1.7.5'

# If your tool depends on some other package(s)
requires = ['pythonABI-2', 'tornado-2.2.1', 'requests-2.4.3', 'psutil-2.1.3']


# If you need to define some environment variables
def commands():
    # You can append directly without the need to check if the variable exist or not, ...
    env.PYTHONPATH.append('{root}/src')

    env.PATH.append('{root}')
    env.PATH.append('{root}/pulitools/puliquery')
    env.PATH.append('{root}/pulitools/puliexec')

    # Create some aliases
    # These aliases will be directly available in the shell.
    # alias('myTool', '{root}/bin/myTool "$@"')
    # VERY DIRTY, to test with python 2.7
    alias('puliexec',   'python {root}/pulitools/puliexec/puliexec.py "$@"')
    alias('pulrn',      'python {root}/pulitools/puliquery/pul_rn "$@"')
    alias('pul_rn',      'python {root}/pulitools/puliquery/pul_rn "$@"')
    alias('pulquery',   'python {root}/pulitools/puliquery/pul_query "$@"')
    alias('pul_query',   'python {root}/pulitools/puliquery/pul_query "$@"')

    alias('workerd',   'python {root}/octopus/workerd.py "$@"')
    alias('dispatcherd',   'python {root}/octopus/dispatcherd.py "$@"')
