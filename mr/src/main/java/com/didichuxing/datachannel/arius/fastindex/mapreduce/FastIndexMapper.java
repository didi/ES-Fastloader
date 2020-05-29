package com.didichuxing.datachannel.arius.fastindex.mapreduce;

import com.didichuxing.datachannel.arius.fastindex.remote.RemoteService;
import com.didichuxing.datachannel.arius.fastindex.remote.config.TaskConfig;
import com.didichuxing.datachannel.arius.fastindex.remote.config.IndexInfo;
import com.didichuxing.datachannel.arius.fastindex.utils.CommonUtils;
import com.didichuxing.datachannel.arius.fastindex.utils.LogUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.io.IOException;
import java.util.List;

@Slf4j
public class FastIndexMapper extends Mapper<Object, HCatRecord, IntWritable, DefaultHCatRecord> {

    private HCatSchema schema;
    private TaskConfig taskConfig;
    private IndexInfo templateConfig;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LogUtils.info("map setup start.....");
        schema = HCatInputFormat.getTableSchema(context.getConfiguration());
        if(schema==null) {
            LogUtils.error("schema is null");
        }

        try {
            taskConfig = TaskConfig.getTaskConfig(context);

            // 判断主键是否在Hive表中存在
            for(String key : taskConfig.getKeyList()) {
                if(this.schema.getPosition(key)==null) {
                    LogUtils.info("key not exist, key:" + key);
                }
            }

            templateConfig = IndexInfo.getIndexInfo(context);
            RemoteService.setHost(taskConfig.getServer());

            LogUtils.info("map setup finish...");
        } catch (Throwable t) {
            LogUtils.error("map setup error", t);
        }
    }

    @Override
    protected void map(Object key, HCatRecord value, Context context) throws IOException, InterruptedException {
        DefaultHCatRecord hCatRecord = (DefaultHCatRecord) value;
        int shardNo;

        List<String> keyList = taskConfig.getKeyList();
        if(keyList==null || keyList.size()==0) {
            shardNo = (int) (Math.random()*templateConfig.getReducerNum());
        } else {
            String keyStr = getKeyValue(keyList, hCatRecord);
            shardNo = CommonUtils.getShardId(keyStr, templateConfig.getReducerNum());
        }

        //shard分片个数与reduce个数一样
        context.write(new IntWritable(shardNo), hCatRecord);
    }

    // 构建主键
    private String getKeyValue(List<String> keys, DefaultHCatRecord hCatRecord) throws HCatException {
        StringBuilder sb = new StringBuilder();
        for (String key : keys) {
            Object id = hCatRecord.get(key, this.schema);
            if (id == null || StringUtils.isBlank(id.toString())) {
                sb.append("");
            } else {
                sb.append(id.toString());
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
