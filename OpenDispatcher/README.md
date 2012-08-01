README for Puli 
===============


DESCRIPTION
-----------

Puli is an open-source dispatcher entirely written in python.


SYSTEM PREREQUISITES
--------------------

Currently, Puli is only supported on Linux (32 or 64 bits).

##### Linux packages :

   * mysql-devel
   * python-devel (for your version of python)

##### Python 2.6+ with following modules :

   * MySQL-python
   * python-sqlobject
   * python-tornado

##### MySQL server 5.0+


INSTALLATION
------------

#### Creation of the database

Execute the following mysql commands to create the database and the dedicated user:

    create database pulidb character set utf8;
    create user puliuser identified by 'yourpasswd';
    grant all privileges on pulidb.* to 'puliuser'@'localhost' identified by 'yourpasswd' with grant option;

#### Installation and configuration

You can copy the `octopus` folder containing all the code to wherever you like (for example `/opt/puli`) just make sure this dir is in your PYTHONPATH.

Create the folders `conf` and `logs` in the install dir.

The settings are located in the file `/opt/puli/octopus/dispatcher/settings.py`.

Please check the following settings:

* CONFDIR = /opt/puli/conf
* LOGDIR = /opt/puli/logs
* POOLS_BACKEND_TYPE = "file"
* DB_ENABLE = True
* DB_CLEAN_DATA = True

#### First launch

To launch the dispatcher, execute the following command in a shell:

    python /opt/puli/scripts/dispatcherd.py -D -C

Upon the first execution, Puli will create the appropriate tables in the database.

For the following executions, you may set these parameters in the settings file:

* POOLS_BACKEND_TYPE = "db"
* DB_ENABLE = True
* DB_CLEAN_DATA = False

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

To put it in a nutshell, Puli is free, as well as freely modifiable and redistributable.

You may use part or all of it in your own applications, whether proprietary or open, free or commercial or not.
