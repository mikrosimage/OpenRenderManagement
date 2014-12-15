#!/usr/bin/env python
# coding: utf-8

import optparse  # no argparse on python 2.6

import sys
import os

# HACK: hard coded path
sys.path.append("/s/apps/lin/puli")
import puliclient


def get_user_name():
    import getpass
    return getpass.getuser()


def split_first_key(values, key):
    try:
        index = values.index(key)
        return (values[:index], values[index+1:])
    except ValueError:
        return (values, None)


def main():

    ## Handle input arguments ##
    input_args = sys.argv[1:]
    input_a, input_b = split_first_key(input_args, '--')

    # print "input A:", input_a
    # print "input B:", input_b

    cmd_args = []
    puliexec_args = []

    if not input_b:
        for arg in input_a:
            if not cmd_args and arg.startswith("-"):
                puliexec_args.append(arg)
            else:
                cmd_args.append(arg)
        # cmd_args = input_args_blocks[0]
    else:
        puliexec_args = input_a
        cmd_args = input_b

    ## Handle puli arguments ##
    usage = """
usage: %prog [options] -- yourcommand [cmd_options]

Used to start any shell command either locally or on the render farm.
Don't forget to specify a pool name otherwise your job will be submitted but will never start.
    """
    parser = optparse.OptionParser(usage=usage)

    parser.add_option(
        "-n", "--name",
        dest="name",
        default="Command Line: '%s'" % cmd_args[0] if cmd_args else "",
        help="Job name. [default: '%default']")

    parser.add_option(
        "-u", "--user",
        dest="user",
        default=get_user_name(),
        help="User name. [default: '%default']")

    parser.add_option(
        "", "--pool",
        dest="pool",
        default="default",
        help="Pool name. [default: '%default']")

    parser.add_option(
        "", "--outFolder",
        dest="outFolder",
        default=os.getcwd(),
        help="Output directory. [default: '%default']")

    parser.add_option(
        "", "--nbFrames",
        dest="nbFrames",
        default='1',
        help="Job number of frames. [default: '%default']")

    parser.add_option(
        "", "--prio",
        dest="priority",
        type=int,
        default=0,
        help="Job priority [default: '%default']")

    parser.add_option(
        "", "--jobType",
        dest="jobType",
        default="commandline",
        help="Job type. [default: '%default']")

    parser.add_option(
        "-P", "--prod",
        dest="prod",
        default=os.environ.get("PROD", ""),
        help="Production name. [default: '%default']")

    parser.add_option(
        "", "--maxRN",
        dest="maxRN",
        type=int,
        default=-1,
        help="Puli max render node for this job. [default: '%default']")

    parser.add_option(
        "", "--time-delay",
        dest="time_delay",
        default=None,
        help="Time delay to run the Job (like '3h30m', '100m').")

    parser.add_option(
        "", "--licenses",
        dest="licenses",
        default="",
        help="Licenses used by the Job (like 'nuke,mtoa').")

    parser.add_option(
        "", "--minRam",
        dest="minRam",
        type=int,
        default=0,
        help="Minimum RAM required for the Job. [default: '%default']")

    parser.add_option(
        "", "--host",
        dest="host",
        default=os.getenv('PULIHOST', 'puliserver'),
        help="Puli Host (like 'puliserver', 'pulitest' or 'localhost'). [default: '%default']")

    parser.add_option(
        "", "--port",
        dest="port",
        type=int,
        default=int(os.getenv('PULIPORT', 8004)),
        help="Puli Server Port. [default: '%default']")

    parser.add_option(
        "", "--local",
        dest="local",
        default=False,
        action='store_true',
        help="Execute puli job locally instead of using the renderfarm (for test purposes).")

    parser.add_option(
        "-v",
        dest="verbose",
        default=False,
        action='store_true',
        help="Richer output")


    (options, args) = parser.parse_args(puliexec_args)

    # print( "options:", str(options) )
    # print( "args:", str(args) )

    if not cmd_args:
        print "No command to run."
        exit(-1)

    ## Submit Graph ##
    import subprocess
    commandline = subprocess.list2cmdline(cmd_args)

    puliTask_args = {'args': commandline}
    runner = "puliclient.contrib.commandlinerunner.CommandLineRunner"

    tags = {
        "type": options.jobType,
        "prod": options.prod,
        'paused': 'false',
        'nbFrames': options.nbFrames,
        'imageFolder': options.outFolder,
    }

    time_delay = None
    if options.time_delay:
        import re
        import datetime

        hours_minutes = re.match("^(?:(\d*)h)?(?:(\d*)m?)?$", options.time_delay).groups()
        if hours_minutes is None:
            print "Invalid delay value."
            exit(-1)
        # convert str to int and None to 0
        hours_minutes = [int(i) if i is not None else 0 for i in hours_minutes]
        future = datetime.datetime.utcnow() + datetime.timedelta(hours=hours_minutes[0], minutes=hours_minutes[1])
        # convert back to timestamp value
        import calendar
        time_delay = calendar.timegm(future.utctimetuple())

    licenses = options.licenses.replace(",", "&")

    task = puliclient.Task(
        options.name, puliTask_args, runner,
        tags=tags, lic=licenses, ramUse=options.minRam,
        timer=time_delay, dispatchKey=options.priority)

    graph = puliclient.Graph(
        options.name, task,
        user=options.user, poolName=options.pool, maxRN=options.maxRN)

    if options.verbose:
        print graph

    if not options.local:
        if options.pool is "":
            print "\nWARNING: no pool defined for this job, it won't start until you set a proper pool value via pulback.\n"
        return graph.submit(options.host, options.port)
    else:
        return graph.execute()


if __name__ == '__main__':
    main()
