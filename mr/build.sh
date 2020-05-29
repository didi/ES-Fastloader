#!/bin/sh

cd src/main/resources 

if [ ! -f "elasticsearch-6.6.1.zip" ];then
zip -r elasticsearch-6.6.1.zip elasticsearch-6.6.1
fi

pwd

cd -

