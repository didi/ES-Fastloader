# Introduction
# <img src="imgs/logo.png" width="250px" align="center" alt="ES-Fastloader"/>
The ES-Fastloader uses the fault tolerance and parallelism of Hadoop and builds individual ElasticSearch shards in multiple reducer nodes, then transfers shards to ElasticSearch cluster for serving. The loader will create a Hadoop job to read data from data files in HDFS, repartitions it on a per-node basis, and finally writes the generated indices to ES shards. In DiDi we have been using ES-Fastloader to create large-scale ElasticSearch indices from TB/PB level sequence files in Hive. 

# Features
* Supports batch construction of ES indexes, which can quickly process dozens of terabytes of data in 1-2 hours, and solve the low-efficiency problem when building massive ES index files.
* Support the horizontal expansion of computing power, and facilitate the expansion. By increasing the machine resources, you can further increase the index construction speed and the amount of data processed.

# Requirements
* JDK: 8 or greater
* ElasticSearch: 6.6.X or greater

# package command
cd mr
mvn clean package -Dmaven.test.skip

# Launch command
Launch --run in hadoop cluster
hadoop jar  mr-1.0.0-SNAPSHOT-with-dep.jar com.didichuxing.datachannel.arius.fastindex.FastIndex $PARAM

# Developer guide
* API document [wiki](https://github.com/didi/ES-Fastloader/wiki)
* Read [core library source code](https://github.com/didi/ES-Fastloader/tree/1.0.0)
* Read [main class](https://github.com/didi/ES-Fastloader/blob/1.0.0/mr/src/main/java/com/didichuxing/datachannel/arius/fastindex/FastIndex.java)
* Read [Release notes](RELEASE-NOTES.md)

# Contributing
Welcome to contribute by creating issues or sending pull requests. See [Contributing Guide](CONTRIBUTING.md) for guidelines.

# Who is using ES-Fastloader?
<img src="imgs/didi.png" width="78px" align="center" alt="滴滴出行"/>

# License
ES-Fastloader is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file.
