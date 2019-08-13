#!/usr/bin/env bash

date=${@: -1}
date_5=`date -d "$date -5 day" +%Y-%m-%d`
dt=(${date_5//-/ })

for (( n=1; n<$#; n++ ))
do
    path=${!n}
    if hadoop fs -test -d ${path}/${dt[0]}/${dt[1]}/${dt[2]}; then
        echo "${path}/${dt[0]}/${dt[1]}/${dt[2]} exists"
        hadoop fs -rm -r ${path}/${dt[0]}/${dt[1]}/${dt[2]}
    else
        echo "${path}/${dt[0]}/${dt[1]}/${dt[2]} does NOT exist."
    fi
done




