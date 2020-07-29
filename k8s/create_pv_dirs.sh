#!/bin/bash

declare -a RELEASES=("feature" 
"development"
"staging"
"release")

declare -a SERVICES=(".archive"
"conf"
"logs"
)



DIR="$1"

if [ $# -ne 1 ]
then
	/bin/echo "Usage: "
    /bin/echo '   source create_pv_dirs.sh "<dir-name>"; where <dir-name> is the path of the desired parent directory e.g. "/data/tradebot". This directory must exist for the script to complete successfully.'
else
    if [ -d "$DIR" ]
    then
        /bin/echo "$DIR directory  exists!"
        /bin/echo "Creating Directories..."
        for RELEASE in "${RELEASES[@]}"
            do
            for SERVICE in "${SERVICES[@]}"
                do
                /bin/echo "Creating ${DIR}/${RELEASE}/${SERVICE}"
                mkdir -p "${DIR}/${RELEASE}/${SERVICE}"
                done
        done
        /bin/echo "Directories successfully created"
    else
        echo "$DIR directory not found!"
    fi
fi
