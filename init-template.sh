#!/bin/sh

PROJECT_NAME_DASH=$(echo "${1//_/-}")
PROJECT_NAME_UNDERSCORE=$(echo "${1//-/_}")

sed -i.bak "s/template/$PROJECT_NAME_DASH/g" pyproject.toml
sed -i.bak "s/template/$PROJECT_NAME_UNDERSCORE/g" main.py
sed -i.bak "s/template/$PROJECT_NAME_UNDERSCORE/g" template/common.py
rm -f *.bak
rm -f template/*.bak

mkdir -p $PROJECT_NAME_UNDERSCORE
mv template/* ./$PROJECT_NAME_UNDERSCORE
rm -r template

rm init-template.sh
