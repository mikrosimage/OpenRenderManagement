#!/bin/bash
#
# Deploy a puli server.
#


usage()
{
cat << EOF
usage: $0 -s [origin folder] -d [installation folder] [options]

This script install puli server execution files into the desired folder.

MANDATORY
   -s      origin folder
           i.e. the root folder containing puli source code and puli scripts,
           for instance: /tmp/OpenRenderManagement/Puli

   -d      destination folder
           i.e. the place you want the program to be installed, typically: /opt/puli

OPTIONS:
   -h      Show this message
   -q      Quiet install, no user prompt

EOF
}

SOURCE=
DESTINATION=
QUIET=
BACKUP_FOLDER=

# Assign process vars with parameters
while getopts “h:s:d:q” OPTION
do
     case $OPTION in
         h)
             usage
             exit 1
             ;;
         s)
             SOURCE=$OPTARG
             ;;
         d)
             DESTINATION=$OPTARG
             ;;
         q)
             QUIET=0
             ;;
         ?)
             usage
             exit
             ;;
     esac
done



# Check if at least source and destination are defined
if [[ -z $SOURCE ]] || [[ -z $DESTINATION ]]
then
    echo "Error: You need to specify the origin and destination folder."
    usage
    exit 1
fi

# Check folders exist
if [[ -d "${SOURCE}" && ! -L "${SOURCE}" ]] ; then

  if [[ ! -d "${SOURCE}/src/octopus" ]] ; then
    echo "Error: origin folder does not contains the puliserver core 'src/octopus' subfolder."
    exit 1
  fi

  if [[ ! -d "${SOURCE}/scripts" ]] ; then
    echo "Error: origin folder does not contains the '${SOURCE}/scripts' subfolder."
    exit 1
  fi

  if [[ ! -f "${SOURCE}/tools" ]] ; then
    echo "Error: origin folder does not contains the '${SOURCE}/tools' subfolder."
    exit 1
  fi

  if [[ ! -f "${SOURCE}/scripts/startup/puliserver" ]] ; then
    echo "Error: origin folder does not contains the startup script: '${SOURCE}/scripts/startup/puliserver'."
    exit 1
  fi

else
    echo "Error: origin folder does not exist or is a symlink."
    exit 1
fi

if [[ ! -d "${DESTINATION}" ]]
then
    echo "Error: destination folder does not exist."
    exit 1
fi

if [[ "$(ls -A ${DESTINATION})" ]]
then
  BACKUP_FOLDER=${DESTINATION}_bkp_`date +'%Y-%m-%d_%H%M%S'`

  if [[ -z $QUIET ]]
  then
    echo "Warning: Destination is not empty."
    read -p "A backup named: ${BACKUP_FOLDER} will be created, Continue (y/n) ? "
    if [[ "$REPLY" != "y" ]]
    then
      echo "Installation interrupted by user." 
      exit 1
    fi
  fi

  echo ""
  echo "Previous install detected, creating backup:"
  echo "  - previous install: "${DESTINATION}
  echo "  - backup folder: "${BACKUP_FOLDER}
  mv ${DESTINATION} ${BACKUP_FOLDER}
fi

# Summary and user prompt
echo ""
echo "Installing puliserver:"
echo "  - origin folder: "$SOURCE
echo "  - destination folder: "$DESTINATION


if [[ -z $QUIET ]]
then
  read -p "Continue (y/n) ? "
  if [[ "$REPLY" != "y" ]]
  then
    echo "Installation interrupted by user." 
    exit 1
  fi
fi

# Process installation
echo ""
echo "Copying octopus source files..."
rsync -rL --exclude "*.pyc" ${SOURCE}/src/octopus ${DESTINATION}/

echo "Copying scripts files..."
mkdir -p ${DESTINATION}/scripts
rsync -rL --exclude "*.pyc" ${SOURCE}/scripts/dispatcherd.py ${DESTINATION}/scripts
rsync -rL --exclude "*.pyc" ${SOURCE}/scripts/pulicleaner ${DESTINATION}/scripts
rsync -rL --exclude "*.pyc" ${SOURCE}/scripts/startup/puliserver /etc/init.d/

echo "Creating config dir..."
mkdir -p ${DESTINATION}/conf

echo "Copying conf file..."
rsync -rL --exclude "*.pyc" ${SOURCE}/etc/puli/licences.lst ${DESTINATION}/conf
rsync -rL --exclude "*.pyc" ${SOURCE}/etc/puli/workers.lst ${DESTINATION}/conf
rsync -rL --exclude "*.pyc" ${SOURCE}/etc/puli/pools ${DESTINATION}/conf

if [[ -f "${BACKUP_FOLDER}/octopus/dispatcher/settings.py" ]] ; then
  echo "Restoring setting from backup: "${BACKUP_FOLDER}
  rsync -rL --exclude "*.pyc" ${BACKUP_FOLDER}/octopus/dispatcher/settings.py ${DESTINATION}/octopus/dispatcher
fi

echo "Creating log dir..."
mkdir  ${DESTINATION}/logs

echo "Installation done."
echo ""
