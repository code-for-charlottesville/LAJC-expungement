#!/bin/bash
#
# Generate SQLAlchemy models from DBML database documentation file. 
# Uses DBML CLI to translate DBML -> SQL, then O!MyModels to 
# translate SQL -> models.py

DBML_FILE=../va_courts.dbml
MODELS_LOCATION=../pipeline # SQLAlchemy models file directory
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


echo "Converting $DBML_FILE to SQL"
echo "Writing to $TEMP_FILE"

dbml2sql $DBML_FILE > $TEMP_FILE
cd $MODELS_LOCATION || exit
omm -m sqlalchemy $TEMP_FILE
cd - &> /dev/null || exit
