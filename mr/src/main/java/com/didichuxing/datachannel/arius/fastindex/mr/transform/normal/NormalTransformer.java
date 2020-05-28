package com.didichuxing.datachannel.arius.fastindex.mr.transform.normal;

import com.alibaba.fastjson.JSONObject;
import com.didichuxing.datachannel.arius.fastindex.mr.remote.config.IndexInfo;
import com.didichuxing.datachannel.arius.fastindex.mr.transform.Transformer;
import com.didichuxing.datachannel.arius.fastindex.mr.transform.normal.type.Type;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// 1 es中存在这个字段，则按照es的类型
// 2 新字段按照hive来
public class NormalTransformer implements Transformer {

    private List<Type> typeList = new ArrayList<>();

    public NormalTransformer(Reducer.Context context, IndexInfo indexInfo) throws Exception {
        Map<String, String> esTypeMap = indexInfo.getTypeMap();

        HCatSchema hCatSchema = HCatInputFormat.getTableSchema(context.getConfiguration());
        List<HCatFieldSchema> hCatFieldSchemas = hCatSchema.getFields();
        for(HCatFieldSchema hCatFieldSchema : hCatFieldSchemas) {
            String fieldName = hCatFieldSchema.getName();
            String hiveType = hCatFieldSchema.getTypeString();

            if(esTypeMap.containsKey(fieldName)) {
                String esType = esTypeMap.get(fieldName);
                typeList.add(Type.matchESType(fieldName, esType, indexInfo));
            } else {
                typeList.add(Type.matchHiveType(fieldName, hiveType, indexInfo));
            }
        }
    }

    @Override
    public JSONObject tranform(List<Object> valueList) throws HCatException {
        JSONObject data = new JSONObject();

        for(int i=0; i<typeList.size(); i++) {
            typeList.get(i).addField(data, valueList.get(i));
        }

        return data;
    }
}
