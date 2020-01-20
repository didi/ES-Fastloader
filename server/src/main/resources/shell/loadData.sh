#!/bin/sh

set -x

#lucene存放的hdfs路径
hdfsDir=$1
#索引名
indexName=$2
#索引UUID
uuid=$3
#索引shard
indexShard=$4
#脚本的工作目录
workDir=$5
#对应的hdfs reduce编号
reduceNums=$6
#主键名
primeKey=$7


#设置参数
export JAVA_HOME="/usr/local/jdk1.8.0_77"
export HADOOP_HOME="/usr/local/hadoop-current/"
export TERM=xterm
export PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:$PATH

export HADOOP_USER_NAME=$8
export HADOOP_USER_PASSWORD=$9

port=${10}

startTime=$(date +%s)

#查看是否有老任务在执行
ps aux | grep -v $$  | grep "$workDir" | awk '{ print $2 }' | xargs kill -9 2>/dev/null


#本地信息复位
rm -rf $workDir
mkdir -p $workDir
cd $workDir

index=0

IFS=,
for reduceNum in $reduceNums
do

shardWorkDir=$workDir/shard$index
rm -rf $shardWorkDir
mkdir $shardWorkDir

shardTarFile=$shardWorkDir"/data.tar"

#下载压缩包
status="fail"
hadoop fs -get $hdfsDir/$reduceNum/*.tar $shardTarFile && status="ok"
if [ "$status" == "ok" ];then
        echo "loadhdfsOK$index"
else
        echo "loadhdfs fail, hdfsShard:$reduceNum"
        rm -rf $workDir
        exit 1
fi


cd $shardWorkDir

#解压缩文件
status="fail"
tar xvf $shardTarFile 1>&2 && status="ok"
if [ "$status" == "ok" ];then
        echo "tarOK$index"
else
        echo "tar fail, hdfsShard:$reduceNum"
        rm -rf $workDir
        exit 1
fi


#删除压缩文件
rm -rf $shardTarFile

#搬迁index
status="fail"
mv  $shardWorkDir/*/*/nodes/0/indices/*/0/index $shardWorkDir/index && status="ok"
if [ "$status" == "ok" ];then
        echo "mvindexOK$index"
else
        echo "mv index fail, hdfsShard:$reduceNum"
        rm -rf $workDir
        exit 1
fi


index=$(($index+1))
done


#判断是否超时 15分钟
endTime=$(date +%s)
cost=$(($endTime-$startTime))
echo "cost:"$cost
if [ "$cost" -gt "36000" ];then
        echo "load data cost to mush, cost:"$cost"s"
        rm -rf $workDir
        exit 1
fi


dir=""
for ((i=0; i<$index; i ++))
do

if [ $i -ne 0 ];then
        dir=$dir","$workDir/shard$i/index
else
        dir=$workDir/shard$i/index
fi

done
echo "dir:"$dir


#append lucene
startTime=$(date +%s)

echo "append lucene"
status=$(curl "127.0.0.1:$port/lucene/append?indexName=$indexName&uuid=$uuid&shardId=$indexShard&append=$dir&primeKey=$primeKey")
echo $status
if [[ $status == *APPEND_LUCENE_OK* ]]
then
        echo "appendluceneOK"
else
        echo "append fail"
        rm -rf $workDir
        exit 1
fi

endTime=$(date +%s)
cost=$(($endTime-$startTime))
echo "append lucene total cost:"$cost

rm -rf $workDir
echo "allOK"