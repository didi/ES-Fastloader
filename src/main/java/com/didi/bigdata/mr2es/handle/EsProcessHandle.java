package com.didi.bigdata.mr2es.handle;

import com.alibaba.fastjson.JSONObject;
import com.didi.bigdata.mr2es.utils.Constants;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by WangZhuang on 2019/3/29
 */
@Data
@NoArgsConstructor
public class EsProcessHandle {

    private String index;
    private String type;
    private Reducer.Context context;
    private BulkProcessor processor;

    private static final Queue<JSONObject> recordQueue = new LinkedBlockingQueue<>();

    public EsProcessHandle(String index, String type,
                           Reducer.Context context, BulkProcessor processor) {
        this.index = index;
        this.type = type;
        this.context = context;
        this.processor = processor;
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    JSONObject record = recordQueue.poll();
                    if (record != null) {
                        String id = record
                                .getString(context.getConfiguration().get(Constants.KEY_ID));
                        processor.add(new IndexRequest(index, type, id)
                                .source(record.toJSONString()));
                    }
                }
            }
        }).start();
    }

    public void addRecord(JSONObject record) {
        recordQueue.add(record);
    }
}
