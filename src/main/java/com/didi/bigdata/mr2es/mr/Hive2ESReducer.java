package com.didi.bigdata.mr2es.mr;

import com.alibaba.fastjson.JSONObject;
import com.didi.bigdata.mr2es.es.ESContainer;
import com.didi.bigdata.mr2es.es.ESJson;
import com.didi.bigdata.mr2es.service.FastIndexService;
import com.didi.bigdata.mr2es.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by WangZhuang on 2018/12/5
 */
@Slf4j
public class Hive2ESReducer extends
        Reducer<IntWritable, DefaultHCatRecord,
                NullWritable, NullWritable> {

    private String indexDir; //es索引生成临时路径
    private HCatSchema schema; //hive域结构
    private List<String> fields;
    private ESContainer container;
    private String index;
    private String type;
    private BulkProcessor processor;
    private int shardNo = -1;

    private ESJson esJson;
    private Map<String, String[]> defaultValueMap;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        log.info("start setup");
        indexDir = context.getConfiguration()
                .get(Constants.KEY_ES_WORK_DIR);
        type = "indextype";
        HadoopUtil.deleteDir(context.getConfiguration(), indexDir); //清理可能存在的文件夹
        initHiveTableSchema(context);
        boolean res = initESContainer(context);
        if (!res) {
            log.error("init es container error!stop reduce");
            System.exit(-1);
        }
        initProcessor(container.getNode().client());
        buildEsJson(context);
        log.info("setup finish!");
    }

    private void buildEsJson(Context context) {
        Configuration conf = context.getConfiguration();
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd");
        DateTime date = formatter.parseDateTime(conf.get(Constants.KEY_DT));
        long timestamp = date.getMillis();
        defaultValueMap = TagDefaultValueTools.getTagDefaultValue(conf);
        if (MapUtils.isEmpty(defaultValueMap)) {
            log.error("get defaultValue error!");
            System.exit(-1);
        }
        DateTimeFormatter timeFormatter =
                DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter dateFormatter =
                DateTimeFormat.forPattern("yyyy-MM-dd");
        DateTimeFormatter statFormatter =
                DateTimeFormat.forPattern("yyyyMMdd");
        String timeRegex = "\\d{4}\\-\\d{2}\\-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}";
        String dateRegex = "\\d{4}\\-\\d{2}\\-\\d{2}";
        String statRegex = "\\d{8}";
        int timezoneOffset = conf.getInt(Constants.TIME_ZONE_OFFSET, 0);
        esJson = new ESJson(dateFormatter, dateRegex,
                statFormatter, statRegex,
                timeFormatter, timeRegex,
                timestamp, timezoneOffset);
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        log.info("start cleanup...");

        container.forceMerge(index);

        log.info("force merge done!");

        JSONObject mappingJson = container.getMapping(index, type);
        if (mappingJson != null) {
            JSONObject resMapping = new JSONObject();
            resMapping.put(type, mappingJson);
            submitMapping(context, resMapping);
        }

        //由于es内部可能存在merge操作,导致文件变动,所以先close掉node, 再去copy文件
        container.closeNode();
        log.info("es node close success!");

        tarIndexDir();

        try {
            local2Hdfs(context);
        } catch (Exception e) {
            log.error("upload to hdfs error!", e);
        }
        ExecutorUtils.stop();
        log.info("cleanup finish!");
    }

    /**
     * 将最终mapping发送给es
     *
     * @param context
     * @param jsonObject
     */
    private void submitMapping(Context context, JSONObject jsonObject) {
        Configuration configuration = context.getConfiguration();
        String template = configuration.get(Constants.KEY_WORKFLOW_NAME);
        long time = DateUtils.getTimestamp(configuration.get(Constants.KEY_DT));
        FastIndexService.getInstance()
                .submitMapping(template, time, shardNo, jsonObject.toJSONString());
    }

    /**
     * 重试3次
     *
     * @param context
     * @throws Exception
     */
    private void local2Hdfs(Context context) throws Exception {
        log.info("upload local start");
        String esDest = context.getConfiguration()
                .get(Constants.KEY_ES_OUTPUT_PATH)
                + "/" + shardNo; //hdfs上目标路径
        log.info("es dest is:{}", esDest);
        int tryCount = 3;
        for (int i = 0; i < tryCount; i++) {
            boolean res = HadoopUtil.copyFile(indexDir
                    + ".tar", esDest, context.getConfiguration());
            if (res) {
                break;
            } else {
                log.error("copy file error,try again:{}", i);
                HadoopUtil.deleteDir(context.getConfiguration(), esDest);
            }
        }
        log.info("upload local finish!");
    }

    /**
     * 压缩生成的临时文件
     *
     * @throws Exception
     */
    private void tarIndexDir() {
        String cmd1 = "du -sh " + indexDir;
        try {
            Cmd.execCmd(cmd1);
        } catch (Exception e) {
            log.error("cmd:{},execute error!", cmd1, e);
        }

        //对es数据tar打包,最多重试3次
        log.info("start tar indexDir:{}...", indexDir);
        String cmd2 = "tar -cvf " + indexDir + ".tar " + indexDir;
        int tryCount = 3;
        for (int i = 0; i < tryCount; i++) {
            try {
                Cmd.execCmd(cmd2);
                break;
            } catch (Exception e) {
                log.error("tar index error!try:{}", i, e);
                String delCmd = "rm -rf " + indexDir + ".tar ";
                try {
                    Cmd.execCmd(delCmd);
                } catch (Exception e1) {
                    log.error("cmd:{},execute error!", delCmd, e1);
                }
            }
        }
        log.info("tar indexDir:{} finished!", indexDir);

        String cmd3 = "du -sh " + indexDir + ".tar";
        try {
            Cmd.execCmd(cmd3);
        } catch (Exception e) {
            log.error("cmd:{},execute error!", cmd3, e);
        }
    }

    /**
     * 初始化es容器
     */
    private boolean initESContainer(Context context) {
        Configuration configuration = context.getConfiguration();
        try {
            this.container = new ESContainer();
            container.start(indexDir,
                    configuration.get(Constants.KEY_ES_NODE_NAME));
            index = configuration.get(Constants.KEY_INDEX);
            if (!container.indexIsExist(index)) {
                String source = context.getConfiguration()
                        .get(Constants.KEY_INDEX_CONFIG);
                log.info("mapper is:{}", source);
                container.createNewIndex(index, source);
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 初始化hive表的结构
     *
     * @param context
     */
    private void initHiveTableSchema(Context context) {
        try {
            this.schema = HCatInputFormat
                    .getTableSchema(context.getConfiguration());
            this.fields = schema.getFieldNames();
            log.info("fields size:{}, hive fields is:{}",
                    fields.size(), fields);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void reduce(IntWritable key,
                          Iterable<DefaultHCatRecord> values, Context context)
            throws IOException, InterruptedException {
        log.info("reduce start");
        this.shardNo = key.get();
        log.info("es shardNo is:{}", shardNo);
        Iterator<DefaultHCatRecord> hCatRecordIterator = values.iterator();
        insertData(hCatRecordIterator, context);
        context.write(NullWritable.get(), NullWritable.get());
        log.info("reduce finish!");
    }

    private void insertData(Iterator<DefaultHCatRecord> records, Context context) {
        while (records.hasNext()) {
            DefaultHCatRecord record = records.next();
            if (record != null) {
                JSONObject jsonObject = esJson.buildJson(record.getAll(),
                        fields, defaultValueMap);
                String id = jsonObject.getString(context.getConfiguration()
                        .get(Constants.KEY_ID));
                processor.add(new IndexRequest(index, type, id)
                        .source(jsonObject.toJSONString()));
            }
        }
        try {
            boolean res = processor.awaitClose(20, TimeUnit.MINUTES);
            log.info("processor:{} close res:{}", processor, res);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void initProcessor(Client client) {
        processor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest,
                                  BulkResponse bulkResponse) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest,
                                  Throwable throwable) {

            }
        }).setBulkActions(5000)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(8)
                .build();
    }

}
