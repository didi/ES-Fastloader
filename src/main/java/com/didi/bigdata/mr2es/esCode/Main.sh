#!/bin/sh

set -x

DIR="/data0/elasticsearch/lib"

hdfsDir=$1
esDir=$2
workDir=$3
shards=$4

outputFile=${workDir}"/output"

#设置hadoop
export HADOOP_HOME="/usr/local/hadoop-current/"
export TERM=xterm
export PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:$PATH
export HADOOP_USER_PASSWORD=123456

#设置jvm
export JAVA_HOME="/usr/local/jdk1.8.0_77"

#查看是否有老任务在执行
ps aux | grep -v $$  | grep "$esDir" | awk '{ print $2 }' | xargs kill -9 2>/dev/null

rm -rf ${workDir}

#本地信息复位
mkdir -p ${workDir}
cd ${workDir}
echo "nothing" > $outputFile


#下载index合并文件
binDir="/data1/fastIndex/bin"
mkdir -p ${binDir}
luceneCoreFile=${binDir}/lucene-core-5.5.0.jar
luceneMiseFile=${binDir}/lucene-misc-5.5.0.jar
mergeIndexFile=${binDir}/FastIndex-MergeIndex.jar
if [[ ! -f ${mergeIndexFile} ]];then

hadoop fs -get /user/arius/fastindex/lucene-core-5.5.0.jar ${luceneCoreFile} && echo "loadluceneCoreFileOK"
hadoop fs -get /user/arius/fastindex/lucene-misc-5.5.0.jar ${luceneMiseFile} && echo "loadluceneMiseFileOK"
hadoop fs -get /user/arius/fastindex/FastIndex-MergeIndex.jar ${mergeIndexFile} && echo "loadMergeFileOK"

else
echo "loadMergeFileOK"
fi


index=0
shardParam=""

IFS=,
for shard in $shards
do

shardWorkDir=${workDir}/shard$index
rm -rf ${shardWorkDir}
mkdir ${shardWorkDir}

shardTarFile=${shardWorkDir}"/data.tar"

shardHdfsDir=${hdfsDir}/${shard}/*.tar



#下载压缩包
hadoop fs -get ${shardHdfsDir} ${shardTarFile} 2>&1 > $outputFile && echo "loadhdfsOK$index"

#解压缩文件
cd ${shardWorkDir}
tar xvf ${shardTarFile} >> $outputFile && echo "tarOK$index"

#删除压缩文件
rm -rf ${shardTarFile}


#搬迁index
mv  ${shardWorkDir}/*/*/nodes/0/indices/index/0/index ${shardWorkDir}/index && echo "mvindexOK$index"

#搬迁index
mv  ${shardWorkDir}/*/*/nodes/0/indices/index/0/translog ${shardWorkDir}/translog && echo "mvtranslogOK$index"


if [[ ${index} -ne 0 ]]
then  shardParam="$shardParam $shardWorkDir/index"
fi

index=$(($index+1))
done


shardDir=${workDir}/shard0
#将所有shard到shard0中
echo ${shardDir}/index ${shardParam} | xargs java -cp ${mergeIndexFile}:${luceneCoreFile}:${luceneMiseFile} com.didichuxing.datachannel.arius.hdfs.Main && echo "mergeindexOk"

#搬迁index
rm -rf ${esDir}/index
mv  ${shardDir}/index ${esDir}/index && echo "loadindexOK;"

#搬迁index
rm -rf ${esDir}/translog
mv  ${shardDir}/translog ${esDir}/translog && echo "loadtranslogOK;"


rm -rf ${workDir}/shard*

rm -rf ${workDir}
echo "allOK"