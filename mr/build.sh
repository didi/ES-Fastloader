#!/bin/sh


if [ ! -f "src/main/resources/elasticsearch-6.6.1.zip" ];then
zip -r src/main/resources/elasticsearch-6.6.1.zip elasticsearch-6.6.1
fi

mvn clean package -Dmaven.test.skip


mv target/mr-1.0.0-SNAPSHOT-with-dep.jar ./mr.jar
