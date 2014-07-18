README for Puli
===============


DESCRIPTION
-----------

Puli consists of the core of the Puli Project.
It contains both the dispatcher and the worker code.


SYSTEM PREREQUISITES
--------------------

Currently, Puli is only supported on Linux (32 or 64 bits).

#### Central manager

The manager is the main server component, it holds the following subsystems:
 - a central db
 - a dispatcher daemon

##### Linux packages :

   * mysql-devel
   * python-devel (for your version of python)

##### Python 2.6+ with following modules :

   * python-tornado
   * MySQL-python
   * python-sqlobject
   * requests

##### MySQL server 5.0+


#### Render node

A render node is installed on each machine that can holds a rendering process.
It holds the following subsystems:
   * a worker daemon
   * an optionnal commandwatcher process when a process is currently rendering

##### Python 2.6+ with following modules :

   * python-tornado
   * requests
   * psutil


#### Additionnal tools

Several tools can be installed on a separate computer to operate the system:
   * puliquery: command line request tools
   * puliexec: simplify submission of scripts and command line
   * pulistats: set of script to trace graphs based on server statistics
   * pulistatsviewer: GUI program to trace and display graphs

##### Python 2.6+ with following modules :

   * python-tornado
   * numpy
   * pygal
   * pyqt4




INSTALLATION
------------

#### Creation of the database

Execute the following mysql commands to create the database and the dedicated user:

```sql
create database pulidb character set utf8;
create user puliuser identified by 'yourpasswd';
grant all privileges on pulidb.* 
      to 'puliuser'@'localhost' 
      identified by 'yourpasswd' with grant option;
```
#### Installation and configuration

You can copy the `octopus` folder containing all the code to wherever you like (for example `/opt/puli`) just make sure this dir is in your PYTHONPATH.

Create the folders `conf` and `logs` in the install dir.

The settings are located in the file `/opt/puli/octopus/dispatcher/settings.py`.

Please check the following settings:
```ini
CONFDIR = /opt/puli/conf
LOGDIR = /opt/puli/logs
POOLS_BACKEND_TYPE = "file"
DB_ENABLE = True
DB_CLEAN_DATA = True
```
#### First launch

To launch the dispatcher, execute the following command in a shell:

    python /opt/puli/scripts/dispatcherd.py -D -C

Upon the first execution, Puli will create the appropriate tables in the database.

For the following executions, you may set these parameters in the settings file:

```ini
POOLS_BACKEND_TYPE = "db"
DB_ENABLE = True
DB_CLEAN_DATA = False
```
These will tell Puli to init itself with the database that was previously created, and it will enable the persistence of the jobs.

If you want to erase the database in order to have a clean start, reset these parameters to the values in the installation section.


RELEASE NOTES
-------------


KNOWN ISSUES
------------

The model needs a bit of a refactoring. It has been designed several years ago to address some use-cases that are no longer relevant.
It can be more simple and straightforward than it is now.

Several aspects of the worker can be optimized (like the handling of status and completion update, and the interaction between the cmdwatcher and the process)


LICENSING
---------

Puli is distributed using the modified BSD license. Please read the "LICENSE" file for the legal wording.

Long story short, Puli is free, as well as freely modifiable and redistributable.

You may use part or all of it in your own applications, whether proprietary or open, free or commercial or not.
