#!/bin/bash
#
# Generate SQLAlchemy models from DBML database documentation file. 
# Uses DBML CLI to translate DBML -> SQL, then O!MyModels to 
# translate SQL -> models.py

set -eu

DBML_FILE=../va_courts.dbml
MODELS_LOCATION=../ # SQLAlchemy models file directory
TEMP_FILE=/tmp/models.sql # Location for writing intermediate .sql file

# Check for script requirements
if ! command -v dbml2sql &> /dev/null; then
    echo "This script requires DBML CLI!"
    echo "See here to install: https://www.dbml.org/cli/#installation"
    exit
fi

if ! command -v omm &> /dev/null; then
    echo "This script requires the O!MyModels CLI"
    echo "See here to install: https://github.com/xnuinside/omymodels"
    exit
fi

CWD=$PWD
SCRIPT_PATH=${0%/*}
if [ "$0" != "$SCRIPT_PATH" ] && [ "$SCRIPT_PATH" != "" ]; then 
    cd "$SCRIPT_PATH" || exit
fi

DBML_FILENAME=$(basename $DBML_FILE)
echo "Converting ${DBML_FILENAME} to SQL"
echo "Writing to $TEMP_FILE"

dbml2sql $DBML_FILE > $TEMP_FILE
cd $MODELS_LOCATION || exit
omm -m sqlalchemy $TEMP_FILE
cd "$CWD" || exit
