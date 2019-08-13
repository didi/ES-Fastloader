#!/usr/bin/env bash

workflow_name=$1

##日期
if [[ "x"$2 = "x" ]]
then
    dt=`date +"%Y%m%d" -d "-1 days"`
    dtPath=`date +"%Y/%m/%d" -d "-1 days"`
    date=`date +"%Y-%m-%d" -d "-1 days"`
else
    dt=`date -d $2 +%Y%m%d`
    dtPath=`date -d $2 +%Y/%m/%d`
    date=`date -d $2 +%Y-%m-%d`
fi
echo "dt is: " ${dt}
echo "dtPath is: " ${dtPath}
echo "date is:" ${date}

year=${date:0:4}
month=${date:5:2}
day=${date:8:2}

properties_path=client.properties
work_dir=$(grep "${workflow_name}.work.dir=" "${properties_path}" | awk -F= '{print $2}')
output_mr=${work_dir}${dtPath}"/mr"
output_es=${work_dir}${dtPath}"/es"
reduce_num=$(grep "${workflow_name}.reduce.num=" "${properties_path}" | awk -F= '{print $2}')
db=$(grep "${workflow_name}.db=" "${properties_path}" | awk -F= '{print $2}')
table=$(grep "${workflow_name}.table=" "${properties_path}" | awk -F= '{print $2}')
index=$(grep "${workflow_name}.index=" "${properties_path}" | awk -F= '{print $2}')
type=$(grep "${workflow_name}.type=" "${properties_path}" | awk -F= '{print $2}')
id=$(grep "${workflow_name}.id=" "${properties_path}" | awk -F= '{print $2}')
es_work_dir=$(grep "${workflow_name}.es.work.dir=" "${properties_path}" | awk -F= '{print $2}')
es_node_name=$(grep "${workflow_name}.es.node.name=" "${properties_path}" | awk -F= '{print $2}')
replicas_shards_number=$(grep "${workflow_name}.replicas.shards.number=" "${properties_path}" | awk -F= '{print $2}')
queue_name=$(grep "${workflow_name}.queue.name=" "${properties_path}" | awk -F= '{print $2}')
user_type=$(grep "${workflow_name}.user.type=" "${properties_path}" | awk -F= '{print $2}')

hadoop fs -rm -r ${output_mr}
hadoop fs -rm -r ${output_es}
hadoop jar mr2es-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.didi.bigdata.mr2es.Hive2ES -Dmapred.job.queue.name=${queue_name} --output_mr=${output_mr} --output_es=${output_es} --reduce_num=${reduce_num} --db=${db} --table=${table} --index=${index} --type=${type} --id=${id} --es_work_dir=${es_work_dir} --es_node_name=${es_node_name} --replicas_shards_number=${replicas_shards_number} --dt=${dt} --workflow_name=${workflow_name} --user_type=${user_type}
if [[ $? -ne 0 ]]
then
    exit 255
fi

echo "start delete before es file..."
sh clean_up.sh ${work_dir} ${dt}
echo "delete before es file finish!"