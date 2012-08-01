#!/bin/bash

USAGE="
${0} [ -b ] 
     [-d -w workspace] 
     -u username 
     -s servername

-b option to backup distant worker before new deploy

-d run deploy process
"

PULISERVER_PATH=/s/apps/lin/puli
OCTOPUSSERVER_PATH=${PULISERVER_PATH}/octopus

OPTION_B=0
OPTION_D=0
OPTION_U=0
OPTION_S=0
OPTION_W=0

while getopts ":bdu:s:w:" OPTION
do
  case "${OPTION}" in
  	"b")
  		OPTION_B=1
  		;;
  	"d")
  		OPTION_D=1
  		;;
  	"u")
  		OPTION_U=1
  		USERNAME="${OPTARG}"
  		;;
  	"s")
  		OPTION_S=1
  		SERVERNAME="${OPTARG}"
  		;;
  	"w")
  		OPTION_W=1
  		WORKSPACE="${OPTARG}"
  		;;
  	*)
  		echo ${USAGE}
  esac
done
  
if [ ${OPTION_B} -eq 0 -a ${OPTION_D} -eq 0 -a ${OPTION_U} -eq 0 -a ${OPTION_S} -eq 0 -a ${OPTION_W} -eq 0 ]
then 
   echo ${USAGE}
   exit 1
fi

if [ ${OPTION_B} -eq 1 ]
then
   if [ ${OPTION_U} -eq 0 -o ${OPTION_S} -eq 0 ]
   then
   	echo ${USAGE}
   	exit 1
   fi
fi

if [ ${OPTION_D} -eq 1 ]
then
   if [ ${OPTION_U} -eq 0 -o ${OPTION_S} -eq 0 -o ${OPTION_W} -eq 0 ]
   then
   	echo ${USAGE}
   	exit 1
   fi
fi

if [ ${OPTION_D} -eq 1 -a ${OPTION_S} -eq 1 ]
then
	PULISERVER=${USERNAME}@${SERVERNAME}
fi



if [ ${OPTION_B} -eq 1 ]
then
   echo "Start backup puli"
   
   if [ ! -d "pulibackup" ]
   then
   	echo "Create pulibackup directory"
   	mkdir pulibackup
   fi
   if [ ! -d "tmp" ]
   then
   	echo "Create tmp directory"
   	mkdir tmp
   fi
   
   rm -rf pulibackup/worker
   mkdir -p pulibackup/worker
   
   scp -r ${PULISERVER}:${OCTOPUSSERVER_PATH} pulibackup/worker
   
   scp ${PULISERVER}:${OCTOPUSSERVER_PATH}/worker/settings.py tmp/settings.py
   
fi

if [ ${OPTION_D} -eq 1 ]
then
   echo "Start deploing puli"
   
   ssh ${PULISERVER} rm -rf ${OCTOPUSSERVER_PATH}/*

   scp -r ${WORKSPACE} ${PULISERVER}:${PULISERVER_PATH}

   scp tmp/settings.py ${PULISERVER}:${OCTOPUSSERVER_PATH}/worker/settings.py
fi