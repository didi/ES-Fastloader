#!/bin/bash

mvn clean package -Dmaven.test.skip=true -Ppro
ret=$?
if [[ ${ret} -ne 0 ]]
then
    echo "=== maven build faild ==="
    exit ${ret}
else
    echo "=== maven build success! ==="
fi
rm -rf output
mkdir output
cp control.sh output/
mv target/mr2es-1.0.0-SNAPSHOT-jar-with-dependencies.jar output/
cp src/main/resources/pro/script/* output/
cp src/main/resources/pro/client.properties output/
cp src/main/resources/pro/schema/* output/
