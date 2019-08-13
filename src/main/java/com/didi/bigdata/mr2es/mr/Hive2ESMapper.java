package com.didi.bigdata.mr2es.mr;

import com.didi.bigdata.mr2es.utils.Constants;
import com.didi.bigdata.mr2es.utils.Ids;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.io.IOException;

/**
 * Created by WangZhuang on 2018/12/5
 */
@Slf4j
public class Hive2ESMapper extends Mapper<Object,
        HCatRecord, IntWritable, DefaultHCatRecord> {

    private HCatSchema schema;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        log.info("map setup start...");
        schema = HCatInputFormat.getTableSchema(context
                .getConfiguration());
        log.info("map setup finish!");
    }

    @Override
    protected void map(Object key, HCatRecord value,
                       Context context) throws IOException,
            InterruptedException {
        DefaultHCatRecord hCatRecord = (DefaultHCatRecord) value;
        Object id = hCatRecord.get(context.getConfiguration()
                .get(Constants.KEY_ID), this.schema);
        if (id == null || StringUtils.isBlank(id.toString())) {
            return;
        }
        //shard分片个数与reduce个数一样
        int shardNo = Ids.getShardId(id.toString(),
                context.getNumReduceTasks());
        context.write(new IntWritable(shardNo), hCatRecord);
    }

}
