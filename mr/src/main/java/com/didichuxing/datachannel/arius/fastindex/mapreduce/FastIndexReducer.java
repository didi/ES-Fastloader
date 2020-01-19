package com.didichuxing.datachannel.arius.fastindex.mapreduce;

import com.alibaba.fastjson.JSONObject;
import com.didichuxing.datachannel.arius.fastindex.es.ESNode;
import com.didichuxing.datachannel.arius.fastindex.es.ESClient;
import com.didichuxing.datachannel.arius.fastindex.es.EsWriter;
import com.didichuxing.datachannel.arius.fastindex.es.config.IndexConfig;
import com.didichuxing.datachannel.arius.fastindex.remote.EnvEnum;
import com.didichuxing.datachannel.arius.fastindex.remote.RemoteService;
import com.didichuxing.datachannel.arius.fastindex.remote.config.TaskConfig;
import com.didichuxing.datachannel.arius.fastindex.remote.config.IndexInfo;
import com.didichuxing.datachannel.arius.fastindex.transform.Transformer;
import com.didichuxing.datachannel.arius.fastindex.transform.TransformerFactory;
import com.didichuxing.datachannel.arius.fastindex.utils.CommonUtils;
import com.didichuxing.datachannel.arius.fastindex.utils.HdfsUtil;
import com.didichuxing.datachannel.arius.fastindex.utils.LogUtils;
import com.didichuxing.datachannel.arius.fastindex.utils.MappingUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;


/**
 * Created by WangZhuang on 2018/12/5
 */
@Slf4j
public class FastIndexReducer extends Reducer<IntWritable, DefaultHCatRecord, NullWritable, NullWritable> {
    private TaskConfig taskConfig;
    private IndexInfo indexInfo;

    private int shardNo = -1;

    private ESNode esNode;
    private ESClient esClient;
    private EsWriter esWriter;
    private Transformer transformer;

    @Override
    protected void setup(Context context) {
        LogUtils.info("reduce start setup");
        try {
            taskConfig = TaskConfig.getTaskConfig(context);
            indexInfo = IndexInfo.getIndexInfo(context);
            RemoteService.setHost(EnvEnum.valueFrom(taskConfig.getEnv()), taskConfig.getSrcTag());

            this.shardNo = context.getTaskAttemptID().getTaskID().getId();
            LogUtils.info("sharNo:" + shardNo);

            esNode = new ESNode(taskConfig, indexInfo);
            esNode.init(context);
            esClient = esNode.getEsClient();
            esWriter = new EsWriter(esClient, taskConfig.getBatchSize(), taskConfig.getThreadPoolSize());

            transformer = TransformerFactory.getTransoformer(context, taskConfig, indexInfo);

            LogUtils.info("reduce start finish!");
        } catch (Throwable t)  {
            LogUtils.info("init reduce error, msg:" + t.getMessage());
            System.exit(-1);
        }
    }

    @Override
    protected void reduce(IntWritable key, Iterable<DefaultHCatRecord> values, Context context) throws IOException, InterruptedException {
        this.shardNo = key.get();

        LogUtils.info("reduce start, es shardNo is:" + shardNo);
        Iterator<DefaultHCatRecord> records = values.iterator();


        while (records.hasNext()) {
            DefaultHCatRecord record = records.next();
            if (record != null) {
                JSONObject jsonObject = transformer.tranform(record.getAll());

                String Primekey;
                List<String> keyList = taskConfig.getKeyList();
                if(keyList==null || keyList.size()==0) {
                    Primekey = UUID.randomUUID().toString();
                } else {
                    Primekey = getKeyValue(keyList, jsonObject);
                }

                esWriter.bulk(Primekey, jsonObject);
            }
        }

        esWriter.finish();
        context.write(NullWritable.get(), NullWritable.get());
        log.info("reduce finish!");
    }

    @Override
    protected void cleanup(Context context) {
        LogUtils.info("cleanup start");

        try {
            // 降低translog的大小
            esClient.putSetting("index.translog.retention.size", "8mb");

            if(!esClient.refresh()) {
                LogUtils.error("refresh get fail");
            }

            // 2 flush
            if(!esClient.flush()) {
                LogUtils.error("flush get fail");
            }

            // 1 force merge
            esClient.forceMerge();

            // 3 提交mapping
            JSONObject mappingJson= MappingUtils.simple(esClient.getMapping());
            LogUtils.info("simpleMapping:" + mappingJson.toJSONString());
            mappingJson = MappingUtils.diffMapping(new IndexConfig(indexInfo.getSetting()), mappingJson, esClient.type);
            if (mappingJson != null) {
                LogUtils.info("diff mapping:" + mappingJson.toJSONString());
                RemoteService.submitMapping(taskConfig.getEsTemplate(), taskConfig.getTime(), shardNo, mappingJson);
            }

            esClient.getSetting();

            // 4 由于es内部可能存在merge操作,导致文件变动,所以先close掉node, 再去copy文件
            esNode.stop();

            // 5 压缩文件
            String tarFile =  esNode.getDataDir() + "/fastIndex.tar";
            CommonUtils.tarFile(ESNode.LOCAL_ES_WORK_PATH+"/data", tarFile);

            // 6 上传压缩文件到hdfs
            local2Hdfs(context, tarFile);

            RemoteService.submitMetric(taskConfig.getEsTemplate(), taskConfig.getTime(), shardNo, esClient.getTaskMetrics());
        } catch (Throwable t) {
            LogUtils.error("clean up error, msg:" + t.getMessage(), t);
            System.exit(-1);
        }

        LogUtils.info("clean up finish");
    }

    private void local2Hdfs(Context context, String tarFile) throws Exception {
        String hdfsDir = taskConfig.getHdfsESOutputPath() + "/" + shardNo;
        LogUtils.info("local2hdfs start, tarFile:" + tarFile + ", hdfsDir:" + hdfsDir);

        int tryCount = 5;
        for (int i = 0; i < tryCount; i++) {
            try {
                boolean res = HdfsUtil.uploadFile(tarFile, hdfsDir, context.getConfiguration());
                if (res) {
                    LogUtils.info("local2hdfs finish!");
                    return ;
                } else {
                    LogUtils.error("copy file error,try again:"  + i);
                    HdfsUtil.deleteDir(context.getConfiguration(), hdfsDir);
                }
            } catch (Throwable t) {
                LogUtils.error("copy file error,try again:" + i, t);
                HdfsUtil.deleteDir(context.getConfiguration(), hdfsDir);
            }
        }

        throw new Exception("local2Hdfs error, tarFile:" + tarFile);
    }

    // 构建主键
    private String getKeyValue(List<String> keys, JSONObject value) {
        StringBuilder sb = new StringBuilder();
        for (String key : keys) {
            String id = value.getString(key);
            if (id == null || StringUtils.isBlank(id)) {
                sb.append("");
            } else {
                sb.append(id);
            }
            sb.append("_");
        }

        if (sb.length() > 1) {
            return sb.substring(0, sb.length() - 1);
        } else {
            return sb.toString();
        }
    }
}
